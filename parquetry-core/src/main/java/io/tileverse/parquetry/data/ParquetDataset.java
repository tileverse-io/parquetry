/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.data;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.batch.BatchMaterializer;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.filter.ExplainPlan;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Public facade for reading one or many Parquet files that share the same schema.
 *
 * <p>A {@code ParquetDataset} is the entry point to the parquetry read pipeline. The {@link #open(RangeReader)} factory
 * opens a one-file dataset over the supplied {@link RangeReader}; a future release will add factories for partitioned
 * datasets where every file agrees on {@link ParquetSchema} by equality. Concurrent {@code read()} calls on a shared
 * instance are safe; each call constructs its own column readers, filter-pipeline state, and single-use
 * {@link io.tileverse.parquetry.data.read.BatchPipeline} stream.
 *
 * <p>The default {@link #read()} overload reads every record through the canonical {@link ParquetRecord} materializer
 * with no predicate or projection. The expressive overloads expose predicate push-down (via the 5-tier filter
 * pipeline), column projection, custom {@link Materializer materializers}, and the {@link ReadOptions} tunables.
 *
 * <h2>Streams are closeable</h2>
 *
 * <p>Every {@code read(...)} overload returns a {@link Stream} whose {@link Stream#close()} hook releases any in-flight
 * row-group resources (pooled column-chunk buffers, page Arenas held by the current row group's column readers).
 * Callers <em>must</em> use try-with-resources; leaking the stream leaks pooled buffers.
 *
 * <h2>RowGroup view</h2>
 *
 * <p>{@link #rowGroups()} exposes a minimal, public view of the on-disk row groups (index, row count, sizes). It is
 * deliberately not the raw thrift {@link io.tileverse.parquetry.format.RowGroup} - that type belongs to the format
 * module's internal API. Callers needing the unfiltered thrift footer can call
 * {@link io.tileverse.parquetry.format.ParquetFormat#readFooter ParquetFormat.readFooter} directly.
 */
public sealed interface ParquetDataset permits DefaultParquetDataset {

    /** Returns the file's schema as decoded at {@link #open(RangeReader) open} time. */
    ParquetSchema schema();

    /**
     * Returns the file-level key/value metadata, with duplicates collapsed (later keys win). Values absent in the
     * thrift footer are reported as the empty string, freeing callers from the {@code Optional} wrapping.
     */
    Map<String, String> keyValueMetadata();

    /** Returns a public view of the row groups, in file order. */
    List<RowGroup> rowGroups();

    /**
     * Reads every record into the canonical {@link ParquetRecord} view, applying no predicate or projection.
     *
     * <p>Equivalent to {@code read(Predicate.ALWAYS_TRUE, Projection.ALL, Materializer.defaultRecord(),
     * ReadOptions.DEFAULTS)}.
     */
    @MustBeClosed
    default Stream<ParquetRecord> read() {
        return read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS);
    }

    /**
     * Reads records matching {@code predicate}, projecting to {@code projection}, with caller-supplied {@code options}.
     * Each record is a {@link ParquetRecord} from the default materializer.
     *
     * <p>The stream contains only rows that satisfy {@code predicate}. After metadata pruning, each surviving row is
     * evaluated against the predicate (the record-level tier, controlled by {@link ReadOptions#useRecordLevelFilter()}
     * and on by default). Columns the predicate references are decoded for this test even when they fall outside
     * {@code projection}; they are not added to the projected output.
     */
    @MustBeClosed
    Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options);

    /**
     * Generic-typed overload: same as {@link #read(Predicate, Projection, ReadOptions)} but materializes each row to
     * {@code T} via the supplied {@link Materializer}.
     */
    @MustBeClosed
    <T> Stream<T> read(Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options);

    /**
     * Reads every row group as a stream of {@link ParquetRecordBatch}, applying no predicate or projection. Equivalent
     * to {@code readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)}.
     *
     * <p>Each emitted batch is bounded by the natural page row count (or by {@link ReadOptions#batchSize()} when set)
     * and owns its own {@link java.lang.foreign.Arena}. Callers must close every batch they consume; the recommended
     * pattern is to drain the stream inside a try-with-resources.
     */
    @MustBeClosed
    default Stream<ParquetRecordBatch> readBatches() {
        return readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS);
    }

    /**
     * Reads batches from the row groups and pages that survive metadata pruning for {@code predicate}, projecting to
     * {@code projection}, with caller-supplied {@code options}. Each batch is a raw {@link ParquetRecordBatch} (the
     * default identity materializer).
     *
     * <p>Unlike {@link #read read}, the batch path applies pruning only: an emitted batch may still contain rows that
     * do not satisfy {@code predicate}. Callers needing per-row filtering should evaluate it themselves or use
     * {@code read}.
     */
    @MustBeClosed
    Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options);

    /**
     * Generic-typed overload: same as {@link #readBatches(Predicate, Projection, ReadOptions)} but materializes each
     * batch to {@code T} via the supplied {@link BatchMaterializer}.
     */
    @MustBeClosed
    <T> Stream<T> readBatches(
            Predicate predicate, Projection projection, BatchMaterializer<T> materializer, ReadOptions options);

    /**
     * Runs the filter pipeline without reading any column data; returns the {@link ExplainPlan} describing per-tier
     * decisions for every row group. Use this to debug push-down or report plan diagnostics to operators.
     */
    ExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options);

    /**
     * Opens a {@code ParquetDataset} over the file backed by {@code reader}. Reads the footer (one round trip) and
     * decodes the schema; subsequent {@code read} calls reuse both.
     *
     * <p>The returned {@code ParquetDataset} does <em>not</em> own the {@code RangeReader} - the caller retains
     * responsibility for closing it after the last {@code read(...)} stream has been closed. This matches the contract
     * every other parquetry-core reader follows.
     *
     * @throws io.tileverse.parquetry.format.ParquetFormatException if the bytes at the file's footer don't conform to
     *     the Parquet / Thrift spec (bad magic, encrypted file, invalid footer length, malformed metadata, etc.)
     * @throws java.io.UncheckedIOException if the underlying {@link RangeReader} fails to deliver the bytes
     */
    static ParquetDataset open(RangeReader reader) {
        ParquetReader fileReader = ParquetReader.open(reader);
        return new DefaultParquetDataset(java.util.List.of(fileReader));
    }
}
