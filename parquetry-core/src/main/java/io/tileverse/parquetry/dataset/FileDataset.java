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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.batch.BatchMaterializer;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
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
import io.tileverse.parquetry.read.BatchPipeline;
import io.tileverse.parquetry.read.ColumnFetcher;
import io.tileverse.parquetry.read.ReadOptions;
import io.tileverse.parquetry.read.RowGroupSurvivor;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;

import lombok.NonNull;

/**
 * Single-file {@link ParquetDataset} implementation.
 *
 * <p>{@link #open(RangeReader)} performs one footer read and caches the decoded {@link FileMetaData}, the parquetry
 * {@link ParquetSchema}, the collapsed key/value metadata map, and a public {@link RowGroup} view list. Every
 * {@code read()} call constructs its own {@link FilterPipeline}-derived row-group survivor list, a fresh
 * {@link ColumnFetcher}, and a single-use {@link BatchPipeline} stream; the footer is never re-read.
 *
 * <p>Thread safety: the cached footer / schema / metadata are immutable; every {@code read()} call works only off its
 * own locally-allocated state, so concurrent {@code read()} calls on a shared {@code ParquetDataset} cannot collide.
 * The underlying {@link RangeReader} is required to be thread-safe (this is part of the {@code RangeReader} contract).
 */
final class FileDataset implements ParquetDataset {

    private final RangeReader rangeReader;
    private final FileMetaData footer;
    private final ParquetSchema fileSchema;
    private final Map<String, String> keyValueMetadata;
    private final List<RowGroup> rowGroupView;

    private FileDataset(
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

    static FileDataset open(@NonNull RangeReader reader) {
        FileMetaData footer = ParquetFormat.readFooter(reader);
        Map<String, String> kvMetadata = collapseKeyValueMetadata(footer.keyValueMetadata());
        // The two-arg SchemaBuilder.build folds GeoParquet 1.x's "geo" key-value metadata into the schema, synthesizing
        // a native Geometry / Geography logical-type annotation on WKB columns that lack one. Downstream code (e.g.
        // the JtsMaterializer in parquetry-geo-jts) sees a single shape regardless of file version.
        ParquetSchema fileSchema = SchemaBuilder.build(footer.schema(), kvMetadata);
        List<RowGroup> rgView = toRowGroupView(footer);
        return new FileDataset(reader, footer, fileSchema, kvMetadata, rgView);
    }

    @Override
    public ParquetSchema schema() {
        return fileSchema;
    }

    @Override
    public Map<String, String> keyValueMetadata() {
        return keyValueMetadata;
    }

    @Override
    public List<RowGroup> rowGroups() {
        return rowGroupView;
    }

    @Override
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        return read(predicate, projection, Materializer.defaultRecord(), options);
    }

    @Override
    public <T> Stream<T> read(
            @NonNull Predicate predicate,
            @NonNull Projection projection,
            @NonNull Materializer<T> materializer,
            @NonNull ReadOptions options) {

        ExplainPlan plan = runFilterPipeline(predicate, projection, options);
        List<RowGroupSurvivor> survivors = survivorsFor(plan);
        ParquetSchema projectedSchema = plan.projectedSchema();

        ColumnFetcher fetcher = ColumnFetcher.real(rangeReader, fileSchema, options.byteBufferPool());
        return BatchPipeline.rows(fileSchema, projectedSchema, survivors, fetcher, options.batchSize(), materializer);
    }

    @Override
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        return readBatches(predicate, projection, BatchMaterializer.defaultBatch(), options);
    }

    @Override
    public <T> Stream<T> readBatches(
            @NonNull Predicate predicate,
            @NonNull Projection projection,
            @NonNull BatchMaterializer<T> materializer,
            @NonNull ReadOptions options) {

        ExplainPlan plan = runFilterPipeline(predicate, projection, options);
        List<RowGroupSurvivor> survivors = survivorsFor(plan);
        ParquetSchema projectedSchema = plan.projectedSchema();

        ColumnFetcher fetcher = ColumnFetcher.real(rangeReader, fileSchema, options.byteBufferPool());
        Stream<ParquetRecordBatch> batches =
                BatchPipeline.batches(fileSchema, projectedSchema, survivors, fetcher, options.batchSize());
        return batches.map(batch -> materializer.materialize(projectedSchema, batch));
    }

    @Override
    public ExplainPlan explain(
            @NonNull Predicate predicate, @NonNull Projection projection, @NonNull ReadOptions options) {
        return runFilterPipeline(predicate, projection, options);
    }

    // --- filter / projection wiring ---

    private ExplainPlan runFilterPipeline(Predicate predicate, Projection projection, ReadOptions options) {
        List<FilterPipeline.RowGroupInputs> inputs = filterInputsFor(footer, options);
        return FilterPipeline.evaluate(fileSchema, projection, predicate, inputs);
    }

    private List<FilterPipeline.RowGroupInputs> filterInputsFor(FileMetaData fm, ReadOptions options) {
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
     * once per {@code read()} / {@code explain()} call.
     *
     * <p>Returns {@link FilterPipeline#emptyBloomLookup()} when {@code options.useBloomFilter()} is off or the row
     * group has no columns with bloom filters - so the bloom tier degrades gracefully without forcing the evaluator to
     * handle nulls.
     */
    private BloomFilterLookup bloomLookupFor(io.tileverse.parquetry.format.RowGroup rg, ReadOptions options) {
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
     * Scans the row group's column chunks once and indexes those that advertise a bloom-filter offset. We capture the
     * length when the writer provided it ({@code bloom_filter_length} was added in spec 2.10); older writers omit it
     * and the reader falls back to a two-step fetch.
     */
    private Map<ColumnPath, BloomChunkLocator> bloomLocatorsFor(io.tileverse.parquetry.format.RowGroup rg) {
        Map<ColumnPath, BloomChunkLocator> locators = new LinkedHashMap<>();
        for (ColumnChunk chunk : rg.columns()) {
            locatorFor(chunk).ifPresent(entry -> locators.put(entry.path(), entry.locator()));
        }
        return locators;
    }

    /**
     * Resolves one column chunk to its bloom-filter locator, or empty when the chunk has no bloom-filter offset, no
     * column metadata, or no matching primitive leaf in the file schema. Extracted from the row-group loop so the loop
     * stays a single pass without a chain of {@code continue} statements.
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

    /** Carrier returned by {@link #locatorFor} so the calling loop can put the path-keyed entry in one step. */
    private record PathedLocator(ColumnPath path, BloomChunkLocator locator) {}

    /**
     * Resolves one locator into a loaded {@link ColumnBloom}. Uses the single-I/O fast path when {@code length} is set;
     * falls back to {@link BloomFilterReader#readWithoutLength} when the writer omitted {@code bloom_filter_length}.
     * Decode failures (truncated bitset, unsupported algorithm) surface as empty so the evaluator degrades to
     * NotApplied for that column instead of aborting the read.
     */
    private Optional<ColumnBloom> loadBloom(BloomChunkLocator locator) {
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
     * Pre-computed bloom-filter coordinates per column; cached to avoid re-walking the row group on every lookup.
     * {@code length} is the total chunk size ({@code bloom_filter_length} from the column metadata) or {@code -1} when
     * the writer omitted it; the loader picks the fast or slow read path accordingly.
     */
    private record BloomChunkLocator(long offset, int length, io.tileverse.parquetry.schema.PrimitiveKind kind) {}

    /**
     * Builds the stats lookup from the row group's inline column metadata. Returns empty for columns that have no
     * statistics or whose path doesn't map to a leaf in the file schema (the latter only happens with malformed files).
     */
    private ColumnStatsLookup statsLookup(io.tileverse.parquetry.format.RowGroup rg) {
        Map<ColumnPath, ColumnStats> byPath = new LinkedHashMap<>();
        for (ColumnChunk chunk : rg.columns()) {
            recordColumnStats(chunk, byPath);
        }
        return path -> Optional.ofNullable(byPath.get(path));
    }

    private void recordColumnStats(ColumnChunk chunk, Map<ColumnPath, ColumnStats> byPath) {
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

    private Optional<io.tileverse.parquetry.schema.PrimitiveKind> primitiveKindAt(ColumnPath path) {
        return fileSchema
                .find(path)
                .flatMap(field -> field instanceof Field.Primitive p ? Optional.of(p.kind()) : Optional.empty());
    }

    private static DictionaryLookup noDictionaryLookup() {
        return path -> Optional.empty();
    }

    private static ColumnPageStatsLookup noColumnPageStatsLookup() {
        return path -> Optional.empty();
    }

    private List<RowGroupSurvivor> survivorsFor(ExplainPlan plan) {
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

    // --- one-time-at-open helpers ---

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
