/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.lang.foreign.MemorySegment;
import java.util.Optional;
import java.util.stream.Stream;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Exports a sequence of {@link ParquetRecordBatch} as a zero-copy {@code ArrowArrayStream} over the Arrow C Data
 * Interface, the form a native Arrow consumer (DuckDB {@code registerArrowStream}, Polars, any C Data consumer) pulls
 * batches from. The caller provides an allocated {@code ArrowArrayStream} struct (for example arrow-java's
 * {@code ArrowArrayStream.allocateNew}); this fills its callbacks. The stream pulls each batch lazily and closes the
 * batch source (and with it the reader and storage) when the consumer releases the stream.
 *
 * <p>Each exported batch's buffers are copied once into segments borrowed from a {@link SegmentPool} and returned when
 * the consumer releases that batch's array. The copy is required because a C consumer needs a stable native address
 * while parquetry's heap-backed buffers are recycled when the batch closes.
 *
 * <p><strong>The consumer must release every exported stream and every batch array it pulls.</strong> The C Data
 * Interface puts the release obligation on the consumer, and this exporter keeps no Java handle the caller holds; a
 * consumer that imports an export and never releases it therefore leaks that export's pooled buffers and arena until
 * the process exits. {@link #outstandingExports()} reports the registered-but-not-released count for leak monitoring.
 *
 * <p>Running this requires native access to be enabled for the module (the {@code --enable-native-access} JVM flag);
 * the project's {@code .mvn/jvm.config} sets it for the build.
 */
public final class ArrowCDataExporter {

    private ArrowCDataExporter() {
        // utility
    }

    /**
     * The number of exported streams and batch arrays the consumer has not yet released. A value that grows without
     * settling back toward zero is the signal of a consumer that imports exports without releasing them, which leaks
     * native memory. Intended for monitoring, not control flow.
     */
    public static long outstandingExports() {
        return (long) ArrowArrayStreamExporter.outstanding() + ArrowArrayExporter.outstanding();
    }

    /**
     * Fills the {@code ArrowArrayStream} at {@code streamAddress} to export {@code batches} for
     * {@code projectedSchema}. The address is an allocated {@code ArrowArrayStream} struct, for example arrow-java's
     * {@code ArrowArrayStream.allocateNew(allocator).memoryAddress()}.
     */
    public static void export(
            ParquetSchema projectedSchema,
            Optional<GeoParquetMetadata> geo,
            Stream<ParquetRecordBatch> batches,
            long streamAddress) {
        MemorySegment outStream =
                MemorySegment.ofAddress(streamAddress).reinterpret(CDataLayouts.ARROW_ARRAY_STREAM.byteSize());
        export(projectedSchema, geo, batches, outStream);
    }

    /**
     * Fills {@code outStream} to export {@code batches} for {@code projectedSchema}, attaching GeoArrow extension
     * metadata from {@code geo} when present. Buffers are pooled from the shared default segment pool.
     */
    public static void export(
            ParquetSchema projectedSchema,
            Optional<GeoParquetMetadata> geo,
            Stream<ParquetRecordBatch> batches,
            MemorySegment outStream) {
        export(projectedSchema, geo, batches, SegmentPool.getDefault(), outStream);
    }

    /** Fills {@code outStream}, borrowing each batch's export buffers from {@code pool}. */
    static void export(
            ParquetSchema projectedSchema,
            Optional<GeoParquetMetadata> geo,
            Stream<ParquetRecordBatch> batches,
            SegmentPool pool,
            MemorySegment outStream) {
        ArrowArrayStreamExporter.export(projectedSchema, geo, batches, pool, outStream);
    }
}
