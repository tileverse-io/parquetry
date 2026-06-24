/*
 * Copyright (c) 2026 Multivers.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.parquetry.arrow.cdi;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import io.tileverse.parquetry.arrow.ipc.ArrowField;
import io.tileverse.parquetry.arrow.ipc.LogicalColumns;
import io.tileverse.parquetry.arrow.ipc.LogicalColumns.LogicalColumn;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Populates an {@code ArrowArrayStream} C struct that pulls parquetry batches lazily over the C Data Interface. The
 * four callbacks ({@code get_schema}, {@code get_next}, {@code get_last_error}, {@code release}) are process-lifetime
 * upcall stubs shared across every stream; each finds its {@link StreamState} from the stream's {@code private_data}.
 * The stream schema and each batch are a struct of the projected columns, which lets a single struct array hold a whole
 * record batch.
 *
 * @see <a href="https://arrow.apache.org/docs/format/CStreamInterface.html">Arrow C stream interface</a>
 */
// The four callbacks are bound as FFM upcall stubs by name (java:S1144 cannot trace the reflective binding) and must
// catch Throwable because an exception escaping an upcall stub crashes the JVM (java:S1181).
@SuppressWarnings({"java:S1144", "java:S1181"})
final class ArrowArrayStreamExporter {

    private static final System.Logger LOGGER = System.getLogger(ArrowArrayStreamExporter.class.getName());

    /** A C error code returned when the stream cannot be found, which should not happen for a live stream. */
    private static final int ERRNO_INVALID = 22;

    /** A generic I/O error code returned when even recording the failure throws. */
    private static final int ERRNO_IO = 5;

    private static final CallbackRegistry<StreamState> STREAMS = new CallbackRegistry<>();

    private static final MemorySegment GET_SCHEMA = intStub("getSchema");
    private static final MemorySegment GET_NEXT = intStub("getNext");
    private static final MemorySegment GET_LAST_ERROR = lastErrorStub();
    private static final MemorySegment RELEASE = releaseStub();

    private ArrowArrayStreamExporter() {
        // utility
    }

    /** The number of exported streams the consumer has not yet released, for leak observability. */
    static int outstanding() {
        return STREAMS.outstanding();
    }

    /**
     * Populates {@code outStream} to pull {@code batches} lazily, exporting each as a struct array of the projected
     * columns and borrowing its buffers from {@code pool}. The stream's release closes {@code batches} (and its reader
     * and storage).
     */
    static void export(
            ParquetSchema schema,
            Optional<GeoParquetMetadata> geo,
            Stream<ParquetRecordBatch> batches,
            SegmentPool pool,
            MemorySegment outStream) {
        Arena streamArena = Arena.ofShared();
        StreamState state = new StreamState(batches, rootField(schema, geo), pool, streamArena);
        CDataLayouts.streamSetGetSchema(outStream, GET_SCHEMA);
        CDataLayouts.streamSetGetNext(outStream, GET_NEXT);
        CDataLayouts.streamSetGetLastError(outStream, GET_LAST_ERROR);
        CDataLayouts.streamSetRelease(outStream, RELEASE);
        CDataLayouts.streamSetPrivateData(outStream, STREAMS.register(state));
    }

    private static ArrowField rootField(ParquetSchema schema, Optional<GeoParquetMetadata> geo) {
        List<ArrowField> columns = LogicalColumns.of(schema, geo).stream()
                .map(LogicalColumn::field)
                .toList();
        return new ArrowField("", ArrowField.Kind.STRUCT, null, columns, Map.of(), false);
    }

    private static int getSchema(MemorySegment self, MemorySegment outSchema) {
        StreamState state = lookup(self);
        if (state == null) {
            return ERRNO_INVALID;
        }
        try {
            state.exportSchema(outSchema.reinterpret(CDataLayouts.ARROW_SCHEMA.byteSize()));
            return 0;
        } catch (Throwable t) {
            return fail(state, t);
        }
    }

    private static int getNext(MemorySegment self, MemorySegment outArray) {
        StreamState state = lookup(self);
        if (state == null) {
            return ERRNO_INVALID;
        }
        try {
            state.next(outArray.reinterpret(CDataLayouts.ARROW_ARRAY.byteSize()));
            return 0;
        } catch (Throwable t) {
            return fail(state, t);
        }
    }

    /**
     * Records {@code error} on {@code state} and returns the C error code, never letting a secondary failure (the error
     * channel itself throwing) escape the upcall stub and crash the JVM.
     */
    private static int fail(StreamState state, Throwable error) {
        try {
            return state.fail(error);
        } catch (Throwable t) {
            LOGGER.log(System.Logger.Level.WARNING, "Recording an ArrowArrayStream pull failure threw", t);
            return ERRNO_IO;
        }
    }

    private static MemorySegment getLastError(MemorySegment self) {
        try {
            StreamState state = lookup(self);
            return state == null ? MemorySegment.NULL : state.lastError();
        } catch (Throwable t) {
            LOGGER.log(System.Logger.Level.WARNING, "ArrowArrayStream get_last_error callback failed", t);
            return MemorySegment.NULL;
        }
    }

    /**
     * Closes the stream's state and nulls the release pointer. The pointer is nulled before the close runs because the
     * close can throw (closing a reader over object storage) and the whole body is wrapped: an exception escaping an
     * FFM upcall stub crashes the JVM.
     */
    private static void release(MemorySegment self) {
        try {
            MemorySegment stream = self.reinterpret(CDataLayouts.ARROW_ARRAY_STREAM.byteSize());
            CDataLayouts.streamSetRelease(stream, MemorySegment.NULL);
            StreamState state = STREAMS.remove(CDataLayouts.streamPrivateData(stream));
            if (state != null) {
                state.close();
            }
        } catch (Throwable t) {
            LOGGER.log(System.Logger.Level.WARNING, "ArrowArrayStream release callback failed", t);
        }
    }

    private static StreamState lookup(MemorySegment self) {
        MemorySegment stream = self.reinterpret(CDataLayouts.ARROW_ARRAY_STREAM.byteSize());
        return STREAMS.lookup(CDataLayouts.streamPrivateData(stream));
    }

    private static MemorySegment intStub(String methodName) {
        return CDataStubs.upcall(
                MethodHandles.lookup(),
                methodName,
                MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class),
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    }

    private static MemorySegment lastErrorStub() {
        return CDataStubs.upcall(
                MethodHandles.lookup(),
                "getLastError",
                MethodType.methodType(MemorySegment.class, MemorySegment.class),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    }

    private static MemorySegment releaseStub() {
        return CDataStubs.upcall(
                MethodHandles.lookup(),
                "release",
                MethodType.methodType(void.class, MemorySegment.class),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    }
}
