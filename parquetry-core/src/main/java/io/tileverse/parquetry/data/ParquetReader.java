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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.batch.BatchMaterializer;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.read.BatchPipeline;
import io.tileverse.parquetry.data.read.ParallelDecodeCoordinator;
import io.tileverse.parquetry.data.read.RowGroupFetcher;
import io.tileverse.parquetry.data.read.RowGroupPrefetcher;
import io.tileverse.parquetry.data.read.RowGroupSurvivor;
import io.tileverse.parquetry.filter.ExplainPlan;
import io.tileverse.parquetry.filter.FilterPipeline;
import io.tileverse.parquetry.filter.FilterPipeline.BloomFilterLookup;
import io.tileverse.parquetry.filter.FilterPipeline.ColumnBloom;
import io.tileverse.parquetry.filter.FilterPipeline.ColumnPageStatsLookup;
import io.tileverse.parquetry.filter.FilterPipeline.ColumnStats;
import io.tileverse.parquetry.filter.FilterPipeline.ColumnStatsLookup;
import io.tileverse.parquetry.filter.FilterPipeline.DictionaryLookup;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowGroupPlan;
import io.tileverse.parquetry.filter.bloom.BloomFilterReader;
import io.tileverse.parquetry.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * Read entry point for a single Parquet file. Opens the footer once via {@link #open(RangeReader)}, caches the decoded
 * {@link FileMetaData}, the parquetry {@link ParquetSchema}, and the collapsed key/value metadata, and exposes
 * {@link #read read}, {@link #readBatches readBatches}, and {@link #explain explain} entry points that share that one
 * footer.
 *
 * <p>Instances are safe to share across threads. Cached footer, schema, and metadata are immutable; every
 * {@code read()} call allocates its own filter survivors, row-group prefetcher, and batch pipeline. The underlying
 * {@link RangeReader} must itself be thread-safe; the reader does not own it, and the caller closes it after the last
 * returned stream has been closed.
 *
 * <p>The class is non-final and the per-row-group filter helpers ({@link #runFilterPipeline runFilterPipeline},
 * {@link #filterInputsFor filterInputsFor}, {@link #bloomLookupFor bloomLookupFor}, {@link #statsLookup statsLookup},
 * {@link #survivorsFor survivorsFor}) are {@code protected}. A subclass can specialize how stats, bloom filters, or
 * column page indexes are sourced (for example, to plug in a cached or remote lookup) without re-implementing the rest
 * of the read pipeline.
 */
public class ParquetReader {

    private final RangeReader rangeReader;
    private final FileMetaData footer;
    private final ParquetSchema fileSchema;
    private final Map<String, String> keyValueMetadata;
    private final List<RowGroup> rowGroupView;

    protected ParquetReader(
            RangeReader rangeReader,
            FileMetaData footer,
            ParquetSchema fileSchema,
            Map<String, String> keyValueMetadata,
            List<RowGroup> rowGroupView) {
        this.rangeReader = rangeReader;
        this.footer = footer;
        this.fileSchema = fileSchema;
        this.keyValueMetadata = keyValueMetadata;
        this.rowGroupView = rowGroupView;
    }

    /** Opens a reader, performing exactly one footer read against {@code reader}. */
    public static ParquetReader open(@NonNull RangeReader reader) {
        FileMetaData footer = ParquetFormat.readFooter(reader);
        Map<String, String> kvMetadata = collapseKeyValueMetadata(footer.keyValueMetadata());
        ParquetSchema fileSchema = buildFileSchema(footer, kvMetadata);
        List<RowGroup> rgView = toRowGroupView(footer);
        return new ParquetReader(reader, footer, fileSchema, kvMetadata, rgView);
    }

    /** Returns the file schema, with GeoParquet 1.x logical-type annotations folded in. */
    public ParquetSchema schema() {
        return fileSchema;
    }

    /** Returns the file's key/value metadata, collapsed to the last value per key. */
    public Map<String, String> keyValueMetadata() {
        return keyValueMetadata;
    }

    /** Returns a public view of the row groups in file order. */
    public List<RowGroup> rowGroups() {
        return rowGroupView;
    }

    /** Streams rows matching {@code predicate} under {@code projection}, materialized via the default record shape. */
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        return read(predicate, projection, Materializer.defaultRecord(), options);
    }

    /** Streams rows matching {@code predicate} under {@code projection}, materialized via {@code materializer}. */
    public <T> Stream<T> read(
            @NonNull Predicate predicate,
            @NonNull Projection projection,
            @NonNull Materializer<T> materializer,
            @NonNull ReadOptions options) {

        boolean recordLevel = options.useRecordLevelFilter();
        Projection scanProjection = recordLevel ? scanProjectionFor(projection, predicate) : projection;
        ExplainPlan plan = runFilterPipeline(predicate, scanProjection, options);
        List<RowGroupSurvivor> survivors = survivorsFor(plan);
        ParquetSchema scanSchema = plan.projectedSchema();
        ParallelDecodeCoordinator coordinator = newDecodeCoordinator(survivors, scanSchema, options);
        ParquetSchema outputSchema = outputSchemaFor(projection);
        Predicate recordFilter = recordLevel ? recordFilterOf(plan.normalizedPredicate()) : null;
        return BatchPipeline.rows(coordinator, materializer, outputSchema, recordFilter);
    }

    /**
     * Expands {@code projection} to also include the predicate's columns, so record-level evaluation can read them even
     * when the caller did not project them. {@link Projection.All} already decodes every column.
     */
    private static Projection scanProjectionFor(Projection projection, Predicate predicate) {
        return switch (projection) {
            case Projection.All _ -> projection;
            case Projection.Columns(Set<ColumnPath> kept) -> {
                Set<ColumnPath> union = new LinkedHashSet<>(kept);
                union.addAll(Predicate.columns(predicate));
                yield Projection.of(union);
            }
        };
    }

    /** The schema rows are materialized through: exactly the caller's projection, not the expanded scan set. */
    private ParquetSchema outputSchemaFor(Projection projection) {
        return switch (projection) {
            case Projection.All _ -> fileSchema;
            case Projection.Columns(Set<ColumnPath> kept) -> fileSchema.project(kept);
        };
    }

    /** Returns the per-row filter, or {@code null} when the predicate is trivially true (nothing to evaluate). */
    private static Predicate recordFilterOf(Predicate normalized) {
        if (normalized instanceof Predicate.Always(boolean value) && value) {
            return null;
        }
        return normalized;
    }

    /** Streams record batches matching {@code predicate} under {@code projection} via the default batch shape. */
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        return readBatches(predicate, projection, BatchMaterializer.defaultBatch(), options);
    }

    /**
     * Streams record batches matching {@code predicate} under {@code projection}, materialized via
     * {@code materializer}.
     */
    public <T> Stream<T> readBatches(
            @NonNull Predicate predicate,
            @NonNull Projection projection,
            @NonNull BatchMaterializer<T> materializer,
            @NonNull ReadOptions options) {

        ExplainPlan plan = runFilterPipeline(predicate, projection, options);
        List<RowGroupSurvivor> survivors = survivorsFor(plan);
        ParquetSchema projectedSchema = plan.projectedSchema();
        ParallelDecodeCoordinator coordinator = newDecodeCoordinator(survivors, projectedSchema, options);
        Stream<ParquetRecordBatch> batches = BatchPipeline.batches(coordinator);
        return batches.map(batch -> materializer.materialize(projectedSchema, batch));
    }

    /**
     * Builds the coalescing fetcher and the prefetch pipeline for one read. The prefetcher owns a fresh per-read
     * virtual-thread executor; closing the returned stream cascades to {@link RowGroupPrefetcher#close()}, which shuts
     * the executor down, so no executor outlives the read.
     */
    private RowGroupPrefetcher newPrefetcher(
            List<RowGroupSurvivor> survivors, ParquetSchema projectedSchema, ReadOptions options) {
        RowGroupFetcher fetcher = new RowGroupFetcher(
                rangeReader,
                fileSchema,
                projectedSchema,
                options.byteBufferPool(),
                options.maxCoalesceGap(),
                options.maxCoalescedSpan());
        ExecutorService executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("parquetry-fetch-", 0).factory());
        try {
            return new RowGroupPrefetcher(
                    survivors,
                    fetcher,
                    options.fetchBudget(),
                    executor,
                    options.prefetchDepth(),
                    options.maxConcurrentFetchesPerRead());
        } catch (RuntimeException e) {
            executor.shutdownNow();
            throw e;
        }
    }

    /**
     * Wraps the fetch prefetcher in a {@link ParallelDecodeCoordinator} that decodes row groups in parallel on the
     * shared decode pool while preserving file order. Closing the returned coordinator drains in-flight decodes and
     * cascades to {@link RowGroupPrefetcher#close()}, so the per-read fetch executor still shuts down with the read.
     */
    private ParallelDecodeCoordinator newDecodeCoordinator(
            List<RowGroupSurvivor> survivors, ParquetSchema projectedSchema, ReadOptions options) {
        RowGroupPrefetcher prefetcher = newPrefetcher(survivors, projectedSchema, options);
        return new ParallelDecodeCoordinator(
                prefetcher,
                options.decodeExecutor(),
                options.maxDecodeAheadPerRead(),
                projectedSchema,
                fileSchema,
                options.batchSize());
    }

    /** Runs the filter pipeline without reading data and returns the explain plan. */
    public ExplainPlan explain(
            @NonNull Predicate predicate, @NonNull Projection projection, @NonNull ReadOptions options) {
        return runFilterPipeline(predicate, projection, options);
    }

    /**
     * Builds inputs for and runs the filter pipeline. Subclasses may override to inject extra inputs (e.g. an external
     * row-group catalog) before delegation.
     */
    protected ExplainPlan runFilterPipeline(Predicate predicate, Projection projection, ReadOptions options) {
        List<FilterPipeline.RowGroupInputs> inputs = filterInputsFor(footer, options);
        return FilterPipeline.evaluate(fileSchema, projection, predicate, inputs);
    }

    /**
     * Builds one {@link FilterPipeline.RowGroupInputs} per row group from the file footer. Subclasses may override to
     * supply richer dictionary or page-stats lookups than the in-footer defaults.
     */
    protected List<FilterPipeline.RowGroupInputs> filterInputsFor(FileMetaData fm, ReadOptions options) {
        List<FilterPipeline.RowGroupInputs> inputs =
                new ArrayList<>(fm.rowGroups().size());
        for (io.tileverse.parquetry.format.RowGroup rg : fm.rowGroups()) {
            inputs.add(new FilterPipeline.RowGroupInputs(
                    rg.numRows(),
                    statsLookup(rg),
                    noDictionaryLookup(),
                    noColumnPageStatsLookup(),
                    bloomLookupFor(rg, options)));
        }
        return inputs;
    }

    /**
     * Builds the per-row-group bloom-filter lookup. The lookup is memoizing and lazy: bytes are only fetched when the
     * evaluator asks about a specific column (and only Eq/In leaves trigger that), and each column is fetched at most
     * once per {@code read()} or {@code explain()} call.
     *
     * <p>Returns {@link FilterPipeline#emptyBloomLookup()} when {@code options.useBloomFilter()} is off or the row
     * group has no columns with bloom filters; the bloom tier degrades gracefully without forcing the evaluator to
     * handle nulls. Subclasses may override to plug a cached or remote bloom-filter source.
     */
    protected BloomFilterLookup bloomLookupFor(io.tileverse.parquetry.format.RowGroup rg, ReadOptions options) {
        if (!options.useBloomFilter()) {
            return FilterPipeline.emptyBloomLookup();
        }
        Map<ColumnPath, BloomChunkLocator> locators = bloomLocatorsFor(rg);
        if (locators.isEmpty()) {
            return FilterPipeline.emptyBloomLookup();
        }
        Map<ColumnPath, Optional<ColumnBloom>> cache = new LinkedHashMap<>();
        return path -> cache.computeIfAbsent(path, p -> loadBloom(locators.get(p)));
    }

    /**
     * Builds the inline stats lookup from the row group's column metadata. Returns empty for columns that have no
     * statistics or whose path does not map to a leaf in the file schema (the latter only happens with malformed
     * files). Subclasses may override to merge external stats sources (e.g. a sidecar index) with the in-footer
     * statistics.
     */
    protected ColumnStatsLookup statsLookup(io.tileverse.parquetry.format.RowGroup rg) {
        Map<ColumnPath, ColumnStats> byPath = new LinkedHashMap<>();
        for (ColumnChunk chunk : rg.columns()) {
            recordColumnStats(chunk, byPath);
        }
        return path -> Optional.ofNullable(byPath.get(path));
    }

    /**
     * Translates the explain plan's row-group outcomes into materialization-ready {@link RowGroupSurvivor survivors}.
     * Subclasses may override to drop or reorder survivors before they enter the batch pipeline.
     */
    protected List<RowGroupSurvivor> survivorsFor(ExplainPlan plan) {
        List<io.tileverse.parquetry.format.RowGroup> thriftRowGroups = footer.rowGroups();
        List<RowGroupSurvivor> survivors = new ArrayList<>(plan.rowGroups().size());
        for (RowGroupPlan rgPlan : plan.rowGroups()) {
            io.tileverse.parquetry.format.RowGroup rg = thriftRowGroups.get(rgPlan.index());
            switch (rgPlan.outcome()) {
                case ELIMINATED -> {
                    /* drop */
                }
                case PARTIAL -> survivors.add(new RowGroupSurvivor(rg, rgPlan.survivingRows()));
                case FULL -> survivors.add(RowGroupSurvivor.full(rg));
            }
        }
        return survivors;
    }

    /**
     * Scans the row group's column chunks once and indexes those that advertise a bloom-filter offset. The length is
     * captured when the writer provided it ({@code bloom_filter_length} was added in spec 2.10); older writers omit it
     * and the reader falls back to a two-step fetch.
     */
    protected Map<ColumnPath, BloomChunkLocator> bloomLocatorsFor(io.tileverse.parquetry.format.RowGroup rg) {
        Map<ColumnPath, BloomChunkLocator> locators = new LinkedHashMap<>();
        for (ColumnChunk chunk : rg.columns()) {
            locatorFor(chunk).ifPresent(entry -> locators.put(entry.path(), entry.locator()));
        }
        return locators;
    }

    /**
     * Resolves one locator into a loaded {@link ColumnBloom}. The single-I/O fast path applies when {@code length} is
     * set; otherwise the slow path of {@link BloomFilterReader#readWithoutLength} runs. Decode failures (truncated
     * bitset, unsupported algorithm) return empty - the evaluator degrades to NotApplied for that column instead of
     * aborting the read.
     */
    protected Optional<ColumnBloom> loadBloom(BloomChunkLocator locator) {
        if (locator == null) {
            return Optional.empty();
        }
        try {
            SplitBlockBloomFilter bloom = locator.length() > 0
                    ? BloomFilterReader.read(rangeReader, locator.offset(), locator.length())
                    : BloomFilterReader.readWithoutLength(rangeReader, locator.offset());
            return Optional.of(new ColumnBloom(locator.kind(), bloom));
        } catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    /**
     * Resolves one column chunk to its bloom-filter locator, or empty when the chunk has no bloom-filter offset, no
     * column metadata, or no matching primitive leaf in the file schema. Extracted from the row-group loop to keep the
     * loop a single pass without a chain of {@code continue} statements.
     */
    private Optional<PathedLocator> locatorFor(ColumnChunk chunk) {
        return chunk.metaData().flatMap(m -> {
            if (m.bloomFilterOffset().isEmpty()) {
                return Optional.empty();
            }
            ColumnPath path = new ColumnPath(m.pathInSchema());
            return primitiveKindAt(path).map(kind -> {
                int length = m.bloomFilterLength().isPresent()
                        ? Math.toIntExact(m.bloomFilterLength().getAsLong())
                        : -1;
                return new PathedLocator(
                        path, new BloomChunkLocator(m.bloomFilterOffset().getAsLong(), length, kind));
            });
        });
    }

    /** Records one column's inline stats keyed by path, when both the stats and the schema leaf are present. */
    protected void recordColumnStats(ColumnChunk chunk, Map<ColumnPath, ColumnStats> byPath) {
        Optional<ColumnMetaData> maybeMeta = chunk.metaData();
        if (maybeMeta.isEmpty() || maybeMeta.orElseThrow().statistics().isEmpty()) {
            return;
        }
        ColumnMetaData meta = maybeMeta.orElseThrow();
        ColumnPath path = new ColumnPath(meta.pathInSchema());
        primitiveKindAt(path)
                .ifPresent(kind ->
                        byPath.put(path, new ColumnStats(kind, meta.statistics().orElseThrow())));
    }

    /** Resolves a column path to its primitive kind, or empty for non-primitive or unknown paths. */
    protected Optional<PrimitiveKind> primitiveKindAt(ColumnPath path) {
        return fileSchema
                .find(path)
                .flatMap(field -> field instanceof SchemaNode.Primitive p ? Optional.of(p.kind()) : Optional.empty());
    }

    /** Carrier returned by {@link #locatorFor} - the calling loop puts the path-keyed entry in one step. */
    private record PathedLocator(ColumnPath path, BloomChunkLocator locator) {}

    /**
     * Pre-computed bloom-filter coordinates per column; cached to avoid re-walking the row group on every lookup.
     * {@code length} is the total chunk size ({@code bloom_filter_length} from the column metadata) or {@code -1} when
     * the writer omitted it; the loader picks the fast or slow read path accordingly.
     */
    protected record BloomChunkLocator(long offset, int length, PrimitiveKind kind) {}

    private static DictionaryLookup noDictionaryLookup() {
        return path -> Optional.empty();
    }

    private static ColumnPageStatsLookup noColumnPageStatsLookup() {
        return path -> Optional.empty();
    }

    /**
     * Builds the parquetry {@link ParquetSchema} from the footer, folding GeoParquet 1.x's {@code "geo"} key-value
     * metadata into native Geometry / Geography logical-type annotations on WKB columns that lack one. Downstream code
     * (e.g. the JtsMaterializer in parquetry-geo-jts) then sees one shape regardless of file version.
     */
    private static ParquetSchema buildFileSchema(FileMetaData footer, Map<String, String> kvMetadata) {
        return SchemaBuilder.build(footer.schema(), kvMetadata);
    }

    private static Map<String, String> collapseKeyValueMetadata(List<KeyValue> entries) {
        if (entries.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> collapsed = LinkedHashMap.newLinkedHashMap(entries.size());
        for (KeyValue entry : entries) {
            collapsed.put(entry.key(), entry.value().orElse(""));
        }
        return Collections.unmodifiableMap(collapsed);
    }

    private static List<RowGroup> toRowGroupView(FileMetaData footer) {
        List<io.tileverse.parquetry.format.RowGroup> rgs = footer.rowGroups();
        List<RowGroup> view = new ArrayList<>(rgs.size());
        for (int i = 0; i < rgs.size(); i++) {
            io.tileverse.parquetry.format.RowGroup rg = rgs.get(i);
            view.add(new RowGroup(
                    i,
                    rg.numRows(),
                    rg.totalByteSize(),
                    rg.totalCompressedSize().orElse(-1L)));
        }
        return Collections.unmodifiableList(view);
    }
}
