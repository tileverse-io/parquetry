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
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import io.tileverse.parquetry.arrow.columnar.ArrowBufferCodec;
import io.tileverse.parquetry.arrow.columnar.EncodedNode;
import io.tileverse.parquetry.arrow.ipc.ArrowExportPrep;
import io.tileverse.parquetry.arrow.ipc.ArrowField;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The per-stream production state behind an {@code ArrowArrayStream}: the live batch iterator, the struct-of-columns
 * Arrow field for the stream schema, the buffer pool, and a stream-lifetime arena. The single pull lock serializes
 * {@link #next}, since the C stream interface is not safe for concurrent {@code get_next}; {@link #close} closes the
 * underlying batch source (and with it the reader and storage) and frees the arena.
 */
final class StreamState {

    /** A generic I/O error code returned to the consumer when a pull fails. */
    private static final int ERRNO_IO = 5;

    private final Stream<ParquetRecordBatch> source;
    private final Iterator<ParquetRecordBatch> batches;
    private final ArrowField rootField;
    private final List<ColumnPath> columnPaths;
    private final SegmentPool pool;
    private final Arena streamArena;
    private final Object pullLock = new Object();
    private MemorySegment lastError = MemorySegment.NULL;
    private boolean closed = false;

    StreamState(Stream<ParquetRecordBatch> source, ArrowField rootField, SegmentPool pool, Arena streamArena) {
        this.source = source;
        this.batches = source.iterator();
        this.rootField = rootField;
        this.columnPaths = topLevelPaths(rootField);
        this.pool = pool;
        this.streamArena = streamArena;
    }

    private static List<ColumnPath> topLevelPaths(ArrowField rootField) {
        List<ColumnPath> paths = new ArrayList<>(rootField.children().size());
        for (ArrowField column : rootField.children()) {
            paths.add(ColumnPath.of(column.name()));
        }
        return List.copyOf(paths);
    }

    /** Fills {@code outSchema} with the struct-of-columns stream schema. */
    void exportSchema(MemorySegment outSchema) {
        synchronized (pullLock) {
            ArrowSchemaExporter.exportInto(outSchema, rootField, streamArena);
        }
    }

    /**
     * Pulls the next batch into {@code outArray}, or marks {@code outArray} released (the C end-of-stream signal) when
     * the source is drained. Serialized by the pull lock; the live batch is closed after its buffers are copied out.
     */
    void next(MemorySegment outArray) {
        synchronized (pullLock) {
            if (closed || !batches.hasNext()) {
                CDataLayouts.arraySetLength(outArray, 0L);
                CDataLayouts.arraySetRelease(outArray, MemorySegment.NULL);
                return;
            }
            try (ParquetRecordBatch batch = batches.next()) {
                ColumnVector prepared = ArrowExportPrep.prepareForExport(rootStruct(batch), rootField);
                EncodedNode node = ArrowBufferCodec.encode(prepared);
                ArrowArrayExporter.export(node, pool, outArray);
            }
        }
    }

    /** Records the error message for a failed pull and returns the error code the upcall reports to the consumer. */
    int fail(Throwable error) {
        String message = error.getMessage() != null ? error.getMessage() : error.toString();
        synchronized (pullLock) {
            if (!closed) {
                lastError = streamArena.allocateFrom(message);
            }
        }
        return ERRNO_IO;
    }

    MemorySegment lastError() {
        synchronized (pullLock) {
            return lastError;
        }
    }

    /** Closes the batch source (and its reader and storage) and frees the stream arena. Idempotent. */
    void close() {
        synchronized (pullLock) {
            if (closed) {
                return;
            }
            closed = true;
            // Drop the last-error pointer before freeing the arena that backs it, keeping a concurrent get_last_error
            // from handing the consumer a segment into freed memory.
            lastError = MemorySegment.NULL;
            source.close();
            streamArena.close();
        }
    }

    private StructVector rootStruct(ParquetRecordBatch batch) {
        Map<ColumnPath, ColumnVector> children = new LinkedHashMap<>();
        for (ColumnPath path : columnPaths) {
            children.put(path, batch.columns().get(path));
        }
        return new StructVector(children, Validity.allValid(batch.rowCount()), batch.rowCount());
    }
}
