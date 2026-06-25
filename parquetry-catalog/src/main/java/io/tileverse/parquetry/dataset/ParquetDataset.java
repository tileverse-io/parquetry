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

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.batch.BatchMaterializer;
import io.tileverse.parquetry.batch.OutputBatches;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ParquetRuntime;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.RowGroupSummary;
import io.tileverse.parquetry.filter.OutputColumn;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.internal.filter.PredicateColumns;
import io.tileverse.parquetry.internal.filter.PredicateNormalizer;
import io.tileverse.parquetry.internal.filter.RecordAccessors;
import io.tileverse.parquetry.internal.filter.RecordLevelEvaluator;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Public facade for reading one or many Parquet files that share the same schema.
 *
 * <p>A {@code ParquetDataset} is the entry point to the parquetry read pipeline. The {@link #open(ByteRangeSource)}
 * factory opens a one-file dataset over the supplied {@link ByteRangeSource}; a future release will add factories for
 * partitioned datasets where every file agrees on {@link ParquetSchema} by equality. Concurrent {@code read()} calls on
 * a shared instance are safe; each call constructs its own column readers, filter-pipeline state, and single-use
 * {@link io.tileverse.parquetry.internal.read.BatchPipeline} stream.
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
public sealed interface ParquetDataset extends io.tileverse.parquetry.dataset.ParquetReader
        permits DefaultParquetDataset {

    /** Returns the file's schema as decoded at {@link #open(ByteRangeSource) open} time. */
    ParquetSchema schema();

    /**
     * Returns the file-level key/value metadata, with duplicates collapsed (later keys win). Values absent in the
     * thrift footer are reported as the empty string, freeing callers from the {@code Optional} wrapping.
     */
    Map<String, String> keyValueMetadata();

    /** Returns a public view of the row groups, in file order. */
    List<RowGroupSummary> rowGroups();

    /**
     * The footer-aggregated prunable statistics for this dataset's single file. Defined only for a one-file dataset; a
     * multi-file dataset has no single file aggregate and throws {@link UnsupportedOperationException} (consistent with
     * {@code explain}).
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
     * Reads batches shaped by {@code query}, applying its ordered output shape when one is present.
     *
     * <p>When the output renames a column, {@code query.predicate()} references the presented (output) name while the
     * file-level pushdown needs the physical name. This method lowers the predicate from output names to physical names
     * via the output mapping before pushing it down, then shapes each surviving batch into the output order.
     *
     * <p>Only {@link OutputColumn.Physical} and {@link OutputColumn.Promoted} contribute to that mapping;
     * {@link OutputColumn.Constant} and {@link OutputColumn.Null} have no physical source. The contract is therefore
     * that predicate leaves over injected (constant or null) columns are already folded out by the caller before the
     * {@link Query} is built; a predicate leaf naming an injected column would lower to a physical name the file does
     * not have and the pushdown would reject it.
     */
    @MustBeClosed
    default Stream<ParquetRecordBatch> readBatches(Query query, ReadOptions options) {
        if (query.output().isEmpty()) {
            return readBatches(query.predicate(), query.projection(), options);
        }
        Predicate lowered = lowerToPhysicalColumns(query.predicate(), query.output());
        Stream<ParquetRecordBatch> physical = readBatches(lowered, query.projection(), options);
        return physical.map(batch -> OutputBatches.shape(batch, query.output()));
    }

    /**
     * Lowers {@code predicate} from output (presented) column names to file-physical names using the source mapping of
     * {@code output}. Each {@link OutputColumn.Physical} and {@link OutputColumn.Promoted} maps its presented name to
     * its physical source; injected columns contribute nothing.
     */
    private static Predicate lowerToPhysicalColumns(Predicate predicate, List<OutputColumn> output) {
        Map<ColumnPath, ColumnPath> outputToPhysical = new HashMap<>();
        for (OutputColumn column : output) {
            switch (column) {
                case OutputColumn.Physical(ColumnPath name, ColumnPath source) -> outputToPhysical.put(name, source);
                case OutputColumn.Promoted(ColumnPath name, ColumnPath source, PrimitiveKind _) ->
                    outputToPhysical.put(name, source);
                case OutputColumn.Constant _, OutputColumn.Null _ -> {
                    /* injected column has no physical source */
                }
            }
        }
        return PredicateColumns.remap(predicate, outputToPhysical);
    }

    /**
     * Reads records shaped by {@code query}, applying its ordered output shape when one is present.
     *
     * <p>The identity case (empty output) delegates to {@link #read(Predicate, Projection, ReadOptions)}, which applies
     * the record-level filter tier. The shaped case builds records from {@link #readBatches(Query, ReadOptions)}, which
     * only prunes; a surviving batch may still hold rows that fail the predicate. This method therefore re-applies the
     * predicate per record so both cases return exactly the rows that satisfy {@code query.predicate()}.
     */
    @MustBeClosed
    default Stream<ParquetRecord> read(Query query, ReadOptions options) {
        if (query.output().isEmpty()) {
            return read(query.predicate(), query.projection(), options);
        }
        Predicate residual = PredicateNormalizer.normalize(query.predicate());
        if (residual instanceof Predicate.Always(boolean value) && value) {
            return readBatches(query, options).flatMap(ConstantColumnBatches::rows);
        }
        return readBatches(query, options)
                .flatMap(ConstantColumnBatches::rows)
                .filter(record -> RecordLevelEvaluator.test(residual, RecordAccessors.of(record)));
    }

    /**
     * Counts the rows matching {@code query}, lowering its predicate from output names to physical names the same way
     * {@link #readBatches(Query, ReadOptions)} does.
     *
     * <p>The output shape never changes a row count: it only renames, reorders, or injects columns over rows the
     * predicate already selects. Only the predicate namespace needs lowering, after which this delegates to
     * {@link #count(Predicate, ReadOptions)}.
     */
    default long count(Query query, ReadOptions options) {
        Predicate lowered = lowerToPhysicalColumns(query.predicate(), query.output());
        return count(lowered, options);
    }

    /**
     * Explains {@code query}, lowering its predicate from output names to physical names the same way
     * {@link #readBatches(Query, ReadOptions)} does.
     *
     * <p>The pruning plan is computed over physical columns, which the output shape does not change. Only the predicate
     * namespace needs lowering, after which this delegates to {@link #explain(Predicate, Projection, ReadOptions)}.
     */
    default ExplainPlan explain(Query query, ReadOptions options) {
        Predicate lowered = lowerToPhysicalColumns(query.predicate(), query.output());
        return explain(lowered, query.projection(), options);
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
        Predicate lowered = lowerToPhysicalColumns(query.predicate(), query.output());
        return explainAnalyze(lowered, query.projection(), options);
    }

    /**
     * Runs the filter pipeline without reading any column data; returns the {@link ExplainPlan} describing per-tier
     * decisions for every row group. Use this to debug push-down or report plan diagnostics to operators.
     */
    ExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options);

    /**
     * Runs the filter pipeline and then a count-style drain of {@code predicate}, returning the {@link ExplainPlan}
     * annotated with the execution stats that drain actually produced. Use this to compare planned pruning against
     * measured work. Single-file only for now; throws {@link UnsupportedOperationException} on a multi-file dataset.
     */
    ExplainPlan explainAnalyze(Predicate predicate, Projection projection, ReadOptions options);

    /**
     * Counts the rows matching {@code predicate} without assembling any record.
     *
     * <p>{@link Predicate.Always} (the no-predicate {@link #count()} and the always-false case) is answered from
     * row-group metadata alone, decoding no column. For every other predicate the dataset delegates to each file's
     * reader, which runs the filter pipeline over metadata: row groups and pages ruled out by statistics are pruned, a
     * row group whose statistics prove every row matches contributes its row count with no decode, and only the
     * remaining row groups decode the predicate's columns to count the matches columnar. Records are never assembled
     * and the geometry column is never materialized.
     */
    long count(Predicate predicate, ReadOptions options);

    /** Counts matching rows with default {@link ReadOptions}. */
    default long count(Predicate predicate) {
        return count(predicate, ReadOptions.DEFAULTS);
    }

    /** Counts every row in the dataset. */
    default long count() {
        return count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
    }

    /**
     * Opens a {@code ParquetDataset} over {@code source}. Reads the footer (one positional read) and decodes the
     * schema; subsequent {@code read} calls reuse both.
     *
     * <p>The returned {@code ParquetDataset} does <em>not</em> own {@code source}; the caller closes it after the last
     * {@code read(...)} stream is closed.
     *
     * @throws io.tileverse.parquetry.format.ParquetFormatException if the footer bytes don't conform to the spec
     * @throws java.io.UncheckedIOException if {@code source} fails to deliver the bytes
     */
    static ParquetDataset open(ByteRangeSource source) {
        return open(source, OpenOptions.DEFAULTS);
    }

    /**
     * Opens a {@code ParquetDataset} over {@code source}, binding the {@link ParquetRuntime} and any
     * {@link io.tileverse.parquetry.internal.read.DecryptionKeyRetriever} from {@code options}.
     */
    static ParquetDataset open(ByteRangeSource source, OpenOptions options) {
        ParquetReader fileReader = ParquetReader.open(source, options.runtime(), options.decryptionKeyRetriever());
        return new DefaultParquetDataset(List.of(fileReader));
    }

    /**
     * Convenience over a borrowed {@link FileChannel}: equivalent to {@code open(ByteRangeSource.ofChannel(channel))}.
     * The caller retains and closes the channel.
     */
    static ParquetDataset open(FileChannel channel) {
        return open(ByteRangeSource.ofChannel(channel));
    }

    /** Same as {@link #open(FileChannel)}, binding read resources from {@code options}. */
    static ParquetDataset open(FileChannel channel, OpenOptions options) {
        return open(ByteRangeSource.ofChannel(channel), options);
    }

    /**
     * Opens a dataset over every file in {@code fileset}, in index order. Each file is opened immediately (its footer
     * is read), and all files must agree on {@link ParquetSchema} by equality. The byte sources returned by
     * {@link FilesetReader#openFile(int)} are borrowed; the caller owns and closes them.
     *
     * @throws IllegalArgumentException if {@code fileset} reports zero files
     * @throws io.tileverse.parquetry.format.ParquetFormatException if any footer fails to conform to the spec
     * @throws java.io.UncheckedIOException if any source fails to deliver bytes
     */
    static ParquetDataset open(FilesetReader fileset) {
        return open(fileset, OpenOptions.DEFAULTS);
    }

    /** Same as {@link #open(FilesetReader)}, binding read resources from {@code options}. */
    static ParquetDataset open(FilesetReader fileset, OpenOptions options) {
        int fileCount = fileset.fileCount();
        if (fileCount <= 0) {
            throw new IllegalArgumentException("fileset must contain at least one file");
        }
        List<ParquetReader> readers = new ArrayList<>(fileCount);
        for (int index = 0; index < fileCount; index++) {
            readers.add(
                    ParquetReader.open(fileset.openFile(index), options.runtime(), options.decryptionKeyRetriever()));
        }
        return new DefaultParquetDataset(readers);
    }
}
