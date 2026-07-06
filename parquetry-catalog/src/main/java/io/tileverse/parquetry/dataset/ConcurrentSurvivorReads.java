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
package io.tileverse.parquetry.dataset;

import java.util.function.IntFunction;
import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.columnar.BatchRows;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;

/**
 * The cross-file survivor fan-out the catalog datasets share. Each dataset prunes its files to the survivors of a
 * predicate and drains up to {@code maxConcurrentFiles} of them at once, merging their elements into one pull-based
 * stream. This is the read-shaped convenience over {@link ConcurrentFileMerge}: the datasets pass a per-file opener
 * that returns that file's columnar batches, and the fan-out either delivers those batches or flattens them to rows on
 * the consuming thread.
 *
 * <p>The transported element is always a whole {@link ParquetRecordBatch}, which owns its data across the
 * producer-to-consumer hand-off; the row views a batch holds are batch-lifetime flyweights and never cross a thread
 * boundary. The record overload therefore fans the batches out and flattens each to rows through
 * {@link BatchRows#rows}, which materializes row by row on the consuming thread and closes a batch only when the stream
 * advances past it. Emission is unordered, the maximum-overlap default: these datasets present no windowed
 * (offset/limit) read, and no caller depends on a file-ordered sequence. A read that must preserve a deterministic
 * visit order (a spatial-decimation probe) composes {@link #sequential} instead and does not reach the fan-out.
 *
 * <p>{@code openFile} receives a dense index in {@code [0, fileCount)}; a dataset maps it to the survivor it drains.
 * Returning {@link Stream#empty()} for a file the dataset skips after pruning is normal and merges as an empty file.
 *
 * <p>Public because the STAC and Iceberg datasets live in their own modules; it keeps {@link ConcurrentFileMerge}
 * package-private while giving those datasets the fan-out entry points they need.
 */
public final class ConcurrentSurvivorReads {

    private ConcurrentSurvivorReads() {}

    /**
     * Fans the survivor files out as batches and flattens each to a row of {@code T} on the consuming thread. The
     * per-file batches transport across the hand-off; {@code materializer} turns each into {@code T} only once the
     * consumer holds the batch, keeping every flyweight row view on one thread.
     */
    @MustBeClosed
    public static <T> Stream<T> records(
            int fileCount,
            IntFunction<Stream<ParquetRecordBatch>> openFile,
            Materializer<T> materializer,
            int maxConcurrentFiles) {
        Stream<ParquetRecordBatch> batches = batches(fileCount, openFile, maxConcurrentFiles);
        return BatchRows.rows(batches, materializer);
    }

    /**
     * Fans the survivor files out as columnar batches, merged in arrival order. Closing the returned stream cancels the
     * producers and closes every batch the consumer never received.
     */
    @MustBeClosed
    public static Stream<ParquetRecordBatch> batches(
            int fileCount, IntFunction<Stream<ParquetRecordBatch>> openFile, int maxConcurrentFiles) {
        return ConcurrentFileMerge.stream(
                fileCount,
                openFile,
                ParquetRecordBatch::close,
                maxConcurrentFiles,
                ConcurrentFileMerge.Emission.UNORDERED);
    }

    /** Convenience for the canonical {@link ParquetRecord} record fan-out. */
    @MustBeClosed
    public static Stream<ParquetRecord> records(
            int fileCount, IntFunction<Stream<ParquetRecordBatch>> openFile, int maxConcurrentFiles) {
        return records(fileCount, openFile, Materializer.defaultRecord(), maxConcurrentFiles);
    }

    /**
     * Concatenates the files' element streams one at a time in index order, opening a file's stream lazily and closing
     * it only when advancing to the next file (or when the returned stream closes). The sequential companion to the
     * fan-out for reads that must pin the per-file visit order onto one thread (a spatial-decimation probe).
     *
     * <p>{@code Stream.flatMap} is not a substitute: pulled through {@code Stream.iterator()} it drains a whole
     * per-file stream into a buffer and closes it - releasing the batches backing that file's flyweight rows - before
     * the buffered rows are read.
     */
    @MustBeClosed
    public static <T> Stream<T> sequential(int fileCount, IntFunction<Stream<T>> openFile) {
        return SequentialFileConcat.stream(fileCount, openFile);
    }
}
