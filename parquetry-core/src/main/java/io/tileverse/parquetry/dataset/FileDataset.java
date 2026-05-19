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
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.filter.ExplainPlan;
import io.tileverse.parquetry.filter.FilterPipeline;
import io.tileverse.parquetry.filter.FilterPipeline.ColumnPageStatsLookup;
import io.tileverse.parquetry.filter.FilterPipeline.ColumnStats;
import io.tileverse.parquetry.filter.FilterPipeline.ColumnStatsLookup;
import io.tileverse.parquetry.filter.FilterPipeline.DictionaryLookup;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowGroupPlan;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.read.ColumnFetcher;
import io.tileverse.parquetry.read.ConcurrencyMode;
import io.tileverse.parquetry.read.ReadOptions;
import io.tileverse.parquetry.read.RowGroupPipeline;
import io.tileverse.parquetry.read.RowGroupSurvivor;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;

/**
 * Single-file {@link ParquetDataset} implementation.
 *
 * <p>{@link #open(RangeReader)} performs one footer read and caches the decoded {@link FileMetaData}, the parquetry
 * {@link ParquetSchema}, the collapsed key/value metadata map, and a public {@link RowGroup} view list. Every
 * {@code read()} call constructs its own {@link FilterPipeline}-derived row-group survivor list, a fresh
 * {@link ColumnFetcher}, and a single-use {@link RowGroupPipeline}; the footer is never re-read.
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

    static FileDataset open(RangeReader reader) {
        Objects.requireNonNull(reader, "reader");
        FileMetaData footer = ParquetFormat.readFooter(reader);
        ParquetSchema rawSchema = SchemaBuilder.build(footer.schema());
        Map<String, String> kvMetadata = collapseKeyValueMetadata(footer.keyValueMetadata());
        // GeoParquet 1.x files carry no native logical type on geometry columns; the bridge synthesizes the
        // Geometry / Geography annotation from the file's "geo" key-value metadata so downstream code (e.g. the
        // JtsMaterializer in parquetry-geo-jts) only needs to look at the logical type.
        ParquetSchema fileSchema = GeoMetadataBridge.apply(rawSchema, kvMetadata);
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
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        Objects.requireNonNull(predicate, "predicate");
        Objects.requireNonNull(projection, "projection");
        Objects.requireNonNull(materializer, "materializer");
        Objects.requireNonNull(options, "options");

        ExplainPlan plan = runFilterPipeline(predicate, projection);
        List<RowGroupSurvivor> survivors = survivorsFor(plan);
        ParquetSchema projectedSchema = plan.projectedSchema();
        ReadOptions resolvedOptions = resolveConcurrencyMode(options);

        ColumnFetcher fetcher = ColumnFetcher.real(rangeReader, fileSchema, resolvedOptions.byteBufferPool());
        RowGroupPipeline<T> pipeline = new RowGroupPipeline<>(
                rangeReader, fileSchema, projectedSchema, survivors, resolvedOptions, fetcher, materializer);
        return pipeline.stream();
    }

    @Override
    public ExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options) {
        Objects.requireNonNull(predicate, "predicate");
        Objects.requireNonNull(projection, "projection");
        Objects.requireNonNull(options, "options");
        return runFilterPipeline(predicate, projection);
    }

    // --- filter / projection wiring ---

    private ExplainPlan runFilterPipeline(Predicate predicate, Projection projection) {
        List<FilterPipeline.RowGroupInputs> inputs = filterInputsFor(footer);
        return FilterPipeline.evaluate(fileSchema, projection, predicate, inputs);
    }

    private List<FilterPipeline.RowGroupInputs> filterInputsFor(FileMetaData fm) {
        List<FilterPipeline.RowGroupInputs> inputs =
                new ArrayList<>(fm.rowGroups().size());
        for (io.tileverse.parquetry.format.RowGroup rg : fm.rowGroups()) {
            inputs.add(new FilterPipeline.RowGroupInputs(
                    rg.numRows(), statsLookup(rg), noDictionaryLookup(), noColumnPageStatsLookup()));
        }
        return inputs;
    }

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

    // --- AUTO concurrency resolution ---

    /**
     * {@link ConcurrencyMode#AUTO} resolves at {@code ParquetDataset.read} time based on the {@code RangeReader}'s
     * locality hint (cloud readers favour {@code FULL}, local readers favour {@code SYNC}). The storage module hasn't
     * surfaced a locality hint yet, so it defaults to {@code FULL}: it is the safer choice for the cloud case (more
     * parallelism) and adds only a fixed virtual-thread overhead for local files - both of which are still bounded by
     * the row-group / column counts.
     *
     * <p>Pending(spec 9.4): consume {@code RangeReader.localityHint()} once it lands; map IN_MEMORY/LOCAL to
     * {@code SYNC} and CLOUD to {@code FULL}.
     */
    private static ReadOptions resolveConcurrencyMode(ReadOptions options) {
        if (options.concurrencyMode() != ConcurrencyMode.AUTO) {
            return options;
        }
        return new ReadOptions(
                options.useStatsFilter(),
                options.useDictionaryFilter(),
                options.useColumnIndexFilter(),
                options.useBloomFilter(),
                options.useRecordLevelFilter(),
                ConcurrencyMode.FULL,
                options.prefetchWindow(),
                options.maxConcurrency(),
                options.pruningDecisionListener(),
                options.decryptionKeyRetriever(),
                options.byteBufferPool());
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
