/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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

import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.columnar.BatchMaterializer;
import io.tileverse.parquetry.columnar.BatchRows;
import io.tileverse.parquetry.columnar.OutputBatches;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.FooterMetadataCache;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.RowGroupSummary;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.PredicateColumns;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * The read facade over one Parquet file.
 *
 * <p>{@link #open(ByteRangeSource)} reads the footer once (or takes it from {@link FooterMetadataCache}) and every
 * {@code read}, {@code count}, {@code bounds} and {@code explain} call shares it. A dataset of many files is a
 * {@link ParquetDataset}, assembled by the catalogs in {@code io.tileverse.parquetry.catalog}. Concurrent
 * {@code read()} calls on a shared instance are safe; each call constructs its own column readers, filter-pipeline
 * state, and single-use {@link io.tileverse.parquetry.internal.read.BatchPipeline} stream.
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
public sealed interface ParquetSource extends io.tileverse.parquetry.dataset.ParquetReader
        permits DefaultParquetSource {

    /** Returns the file's schema, as decoded from its footer. */
    ParquetSchema schema();

    /**
     * Returns the file-level key/value metadata, with duplicates collapsed (later keys win). Values absent in the
     * thrift footer are reported as the empty string, freeing callers from the {@code Optional} wrapping.
     */
    Map<String, String> keyValueMetadata();

    /** Returns a public view of the row groups, in file order. */
    List<RowGroupSummary> rowGroups();

    /**
     * The footer-aggregated prunable statistics for this file: per-column min/max/null-count combined across the row
     * groups, the geometry bounding box per geometry column, and the record count.
     */
    FileStats fileStats();

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
     * Reads the rows matching {@code predicate}, projected to {@code projection}, as columnar batches, with
     * caller-supplied {@code options}. Each batch is a raw {@link ParquetRecordBatch} (the default identity
     * materializer).
     *
     * <p>When record-level filtering is enabled (the default), the predicate is applied exactly: each emitted batch
     * holds only the matching rows. With {@code ReadOptions.useRecordLevelFilter()} disabled, only metadata pruning
     * applies and a surviving batch may still hold rows that do not match.
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
     * Reads batches shaped by {@code query}, applying its ordered output shape when one is present.
     *
     * <p>When the output renames a column, {@code query.predicate()} references the presented (output) name while the
     * file-level pushdown needs the physical name. This method lowers the predicate from output names to physical names
     * via the output mapping before pushing it down, then shapes each surviving batch into the output order.
     *
     * <p>Only {@link Projection.Column.Physical} and {@link Projection.Column.Promoted} contribute to that mapping;
     * {@link Projection.Column.Constant} and {@link Projection.Column.Null} have no physical source. The contract is
     * therefore that predicate leaves over injected (constant or null) columns are already folded out by the caller
     * before the {@link Query} is built; a predicate leaf naming an injected column would lower to a physical name the
     * file does not have and the pushdown would reject it.
     */
    @MustBeClosed
    default Stream<ParquetRecordBatch> readBatches(Query query, ReadOptions options) {
        Predicate pushed = lowerToPhysicalColumns(query);
        Stream<ParquetRecordBatch> produced = readBatches(pushed, query.projection(), options);
        Stream<ParquetRecordBatch> windowed = applyWindow(produced, query.offset(), query.limit());
        if (query.outputColumns().isEmpty()) {
            return windowed;
        }
        return windowed.map(batch -> OutputBatches.select(batch, query.outputColumns()));
    }

    /**
     * Applies the offset/limit window to the produced batch stream before the output selection, leaving the identity
     * window (offset 0, no limit) untouched. Windowing before the selection keeps the slice over the produced filtered
     * batches, never re-selecting an already-selected batch's columns.
     */
    private static Stream<ParquetRecordBatch> applyWindow(
            Stream<ParquetRecordBatch> batches, long offset, OptionalLong limit) {
        if (offset == 0 && limit.isEmpty()) {
            return batches;
        }
        return batches.gather(BatchWindow.of(offset, limit));
    }

    /**
     * Lowers {@code query.predicate()} from presented column names to file-physical names using the produce set's
     * rename mapping. Each {@link Projection.Column.Physical} and {@link Projection.Column.Promoted} maps its presented
     * name to its physical source; injected columns contribute nothing. A non-produce-set projection ({@code All}) has
     * no renames, leaving the predicate unchanged.
     */
    private static Predicate lowerToPhysicalColumns(Query query) {
        Map<ColumnPath, ColumnPath> outputToPhysical = new HashMap<>();
        if (query.projection() instanceof Projection.Of of) {
            of.columns().forEach(column -> addPhysicalMapping(outputToPhysical, column));
        }
        return PredicateColumns.remap(query.predicate(), outputToPhysical);
    }

    private static void addPhysicalMapping(Map<ColumnPath, ColumnPath> mapping, Projection.Column column) {
        switch (column) {
            case Projection.Column.Physical(ColumnPath name, ColumnPath source) -> mapping.put(name, source);
            case Projection.Column.Promoted(ColumnPath name, ColumnPath source, PrimitiveKind _) ->
                mapping.put(name, source);
            case Projection.Column.Constant _,
                    Projection.Column.Null _,
                    Projection.Column.RowPosition _,
                    Projection.Column.Coalesce _ -> {
                /* injected or coalesced column presents no physical source to lower a predicate through */
            }
        }
    }

    /**
     * Reads records shaped by {@code query} through the canonical record materializer; see {@link #read(Query,
     * Materializer, ReadOptions)} for the identity and shaped cases and the exactness of the predicate.
     */
    @MustBeClosed
    default Stream<ParquetRecord> read(Query query, ReadOptions options) {
        return read(query, Materializer.defaultRecord(), options);
    }

    /**
     * Reads records shaped by {@code query}, materializing each through {@code materializer}. The identity case (no
     * output shape) reads through {@link #read(Predicate, Projection, Materializer, ReadOptions)} with the predicate
     * lowered to physical names, then applies the offset/limit window to the rows. The shaped case builds rows from
     * {@link #readBatches(Query, ReadOptions)}, whose batches already present the output shape and the window, and
     * hands each row to {@code materializer} with the batch's projected schema, which describes the columns held by the
     * row.
     *
     * <p>Both cases return exactly the rows that satisfy {@code query.predicate()}: the batches of the shaped case are
     * already filtered exactly, and output shaping only renames, reorders, or injects columns over those rows without
     * ever dropping one. That exactness holds when record-level filtering is enabled (the default). With
     * {@code ReadOptions.useRecordLevelFilter()} disabled the read is pushdown-only, consistent with
     * {@link #read(Predicate, Projection, Materializer, ReadOptions)}.
     */
    @MustBeClosed
    default <T> Stream<T> read(Query query, Materializer<T> materializer, ReadOptions options) {
        if (query.outputColumns().isEmpty()) {
            Stream<T> rows = read(lowerToPhysicalColumns(query), query.projection(), materializer, options);
            return applyRowWindow(rows, query.offset(), query.limit());
        }
        return BatchRows.rows(readBatches(query, options), materializer);
    }

    /**
     * Applies the offset/limit window to a row stream, leaving the identity window untouched. Both row overloads of
     * {@code read(Query, ...)} reach this through {@link #read(Query, Materializer, ReadOptions)}: their
     * identity-output case materializes rows directly rather than through {@link #readBatches(Query, ReadOptions)} and
     * takes its window here at the row level, while the shaped case inherits the batch-level window from
     * {@code readBatches}. {@link Stream#limit(long)} short-circuits the read lazily, leaving the pages past the limit
     * undecoded, which matches the early finish of the batch window.
     */
    private static <T> Stream<T> applyRowWindow(Stream<T> rows, long offset, OptionalLong limit) {
        if (offset == 0 && limit.isEmpty()) {
            return rows;
        }
        Stream<T> windowed = offset == 0 ? rows : rows.skip(offset);
        return limit.isPresent() ? windowed.limit(limit.getAsLong()) : windowed;
    }

    /**
     * Counts the rows matching {@code query}, lowering its predicate from output names to physical names the same way
     * {@link #readBatches(Query, ReadOptions)} does.
     *
     * <p>The output shape never changes a row count: it only renames, reorders, or injects columns over rows the
     * predicate already selects. Only the predicate namespace needs lowering, after which this delegates to
     * {@link #count(Predicate, ReadOptions)}. The {@code offset}/{@code limit} window is ignored: counting is
     * predicate-only, not read-shaping.
     */
    default long count(Query query, ReadOptions options) {
        return count(lowerToPhysicalColumns(query), options);
    }

    /**
     * The bounds of the rows matching {@code query}, lowering its predicate from output names to physical names the
     * same way {@link #readBatches(Query, ReadOptions)} does.
     *
     * <p>The output shape never changes which rows match: it only renames, reorders, or injects columns over the rows
     * the predicate already selects. Only the predicate namespace needs lowering, after which this delegates to
     * {@link #bounds(Predicate, ReadOptions)}. The {@code offset}/{@code limit} window is ignored: bounding is
     * predicate-only, not read-shaping.
     */
    default Optional<BoundingBox> bounds(Query query, ReadOptions options) {
        return bounds(lowerToPhysicalColumns(query), options);
    }

    /**
     * Explains {@code query}, lowering its predicate from output names to physical names the same way
     * {@link #readBatches(Query, ReadOptions)} does.
     *
     * <p>The pruning plan is computed over physical columns, which the output shape does not change. Only the predicate
     * namespace needs lowering, after which this delegates to {@link #explain(Predicate, Projection, ReadOptions)}.
     */
    default ExplainPlan explain(Query query, ReadOptions options) {
        return explain(lowerToPhysicalColumns(query), query.projection(), options);
    }

    /**
     * Explains and analyzes {@code query}, lowering its predicate from output names to physical names the same way
     * {@link #readBatches(Query, ReadOptions)} does.
     *
     * <p>Both the pruning plan and the measured drain run over physical columns, which the output shape does not
     * change. Only the predicate namespace needs lowering, after which this delegates to
     * {@link #explainAnalyze(Predicate, Projection, ReadOptions)}.
     */
    default ExplainPlan explainAnalyze(Query query, ReadOptions options) {
        return explainAnalyze(lowerToPhysicalColumns(query), query.projection(), options);
    }

    /**
     * Runs the filter pipeline without reading any column data; returns the {@link ExplainPlan} describing per-tier
     * decisions for every row group. Use this to debug push-down or report plan diagnostics to operators.
     */
    ExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options);

    /**
     * Runs the filter pipeline and then a count-style drain of {@code predicate}, returning the {@link ExplainPlan}
     * annotated with the execution stats that drain actually produced. Use this to compare planned pruning against
     * measured work.
     */
    ExplainPlan explainAnalyze(Predicate predicate, Projection projection, ReadOptions options);

    /**
     * Counts the rows matching {@code predicate} without assembling any record.
     *
     * <p>{@link Predicate.Always} (the no-predicate {@link #count()} and the always-false case) is answered from
     * row-group metadata alone, decoding no column. For every other predicate the reader runs the filter pipeline over
     * metadata: row groups and pages ruled out by statistics are pruned, a row group whose statistics prove every row
     * matches contributes its row count with no decode, and only the remaining row groups decode the predicate's
     * columns to count the matches columnar. Records are never assembled and the geometry column is never materialized.
     */
    long count(Predicate predicate, ReadOptions options);

    /** Counts matching rows with default {@link ReadOptions}. */
    default long count(Predicate predicate) {
        return count(predicate, ReadOptions.DEFAULTS);
    }

    /** Counts every row in the file. */
    default long count() {
        return count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
    }

    /**
     * Opens a {@code ParquetSource} over {@code source}, reading its footer in one positional read unless
     * {@link FooterMetadataCache} already holds that file's metadata. Either way the schema and the planning form of
     * the footer are in place when this returns, and every subsequent {@code read} call reuses them.
     *
     * <p>The returned {@code ParquetSource} does <em>not</em> own {@code source}; the caller closes it after the last
     * {@code read(...)} stream is closed.
     *
     * @throws io.tileverse.parquetry.format.ParquetFormatException if the footer bytes don't conform to the spec
     * @throws java.io.UncheckedIOException if {@code source} fails to deliver the bytes
     */
    static ParquetSource open(ByteRangeSource source) {
        return open(source, OpenOptions.DEFAULTS);
    }

    /**
     * Opens a {@code ParquetSource} over {@code source}, binding the {@link ParquetRuntime} and any
     * {@link io.tileverse.parquetry.internal.read.DecryptionKeyRetriever} from {@code options}.
     */
    static ParquetSource open(ByteRangeSource source, OpenOptions options) {
        ParquetFileReader fileReader =
                ParquetFileReader.open(source, options.runtime(), options.decryptionKeyRetriever());
        return new DefaultParquetSource(fileReader);
    }

    /**
     * Convenience over a borrowed {@link FileChannel}: equivalent to {@code open(ByteRangeSource.ofChannel(channel))}.
     * The caller retains and closes the channel.
     */
    static ParquetSource open(FileChannel channel) {
        return open(ByteRangeSource.ofChannel(channel));
    }

    /** Same as {@link #open(FileChannel)}, binding read resources from {@code options}. */
    static ParquetSource open(FileChannel channel, OpenOptions options) {
        return open(ByteRangeSource.ofChannel(channel), options);
    }
}
