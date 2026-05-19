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
package io.tileverse.parquetry.dataset;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.filter.ExplainPlan;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.read.ReadOptions;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Public facade for reading a Parquet file (or, in a future release, a partitioned multi-file fileset).
 *
 * <p>A {@code ParquetDataset} instance is the entry point to the parquetry read pipeline. It caches the file's footer
 * and schema once at {@link #open(RangeReader) open} time; every {@link #read read} call constructs fresh column
 * readers, filter pipeline state, and a single-use {@link io.tileverse.parquetry.read.RowGroupPipeline}, so a single
 * {@code ParquetDataset} is safe for concurrent reads from multiple threads.
 *
 * <p>The default {@link #read()} overload reads every record through the canonical {@link ParquetRecord} materializer
 * with no predicate or projection. The expressive overloads expose predicate push-down (via the 5-tier filter
 * pipeline), column projection, custom {@link Materializer materializers}, and the {@link ReadOptions} tunables.
 *
 * <h2>Streams are closeable</h2>
 *
 * <p>Every {@code read(...)} variant returns a {@link Stream} whose {@link Stream#close()} hook cascades to the
 * underlying {@code RowGroupPipeline}, returning pooled buffers and joining the producer thread. Callers <em>must</em>
 * use try-with-resources; leaking the stream leaks pooled buffers and a virtual thread.
 *
 * <h2>RowGroup view</h2>
 *
 * <p>{@link #rowGroups()} exposes a minimal, public view of the on-disk row groups (index, row count, sizes). It is
 * deliberately not the raw thrift {@link io.tileverse.parquetry.format.RowGroup} - that type belongs to the format
 * module's internal API. Callers needing the unfiltered thrift footer can call
 * {@link io.tileverse.parquetry.format.ParquetFormat#readFooter ParquetFormat.readFooter} directly.
 */
public sealed interface ParquetDataset permits FileDataset, MultiFileDataset {

    /** Returns the file's schema as decoded at {@link #open(RangeReader) open} time. */
    ParquetSchema schema();

    /**
     * Returns the file-level key/value metadata, with duplicates collapsed (later keys win). Values absent in the
     * thrift footer surface as the empty string here so callers don't need to track the {@code Optional} wrapping.
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
    default Stream<ParquetRecord> read() {
        return read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS);
    }

    /**
     * Reads records matching {@code predicate}, projecting to {@code projection}, with caller-supplied {@code options}.
     * Records surface as {@link ParquetRecord} via the default materializer.
     */
    Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options);

    /**
     * Generic-typed overload: same as {@link #read(Predicate, Projection, ReadOptions)} but materializes each row to
     * {@code T} via the supplied {@link Materializer}.
     */
    <T> Stream<T> read(Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options);

    /**
     * Runs the filter pipeline without reading any column data; returns the {@link ExplainPlan} describing per-tier
     * decisions for every row group. Use this to debug push-down or surface plan diagnostics to operators.
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
        return FileDataset.open(reader);
    }
}
