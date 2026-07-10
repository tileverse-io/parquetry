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
package io.tileverse.parquetry.stac;

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.StructuredTaskScope;
import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.CatalogSnapshot;
import io.tileverse.parquetry.dataset.ConcurrentSurvivorReads;
import io.tileverse.parquetry.dataset.DatasetCapabilities;
import io.tileverse.parquetry.dataset.DatasetCapabilities.FileSpatialBounds;
import io.tileverse.parquetry.dataset.DatasetCapabilities.FileStatsSource;
import io.tileverse.parquetry.dataset.GeoParquetDataset;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.dataset.explain.FileExplain;
import io.tileverse.parquetry.dataset.explain.Outcome;
import io.tileverse.parquetry.dataset.explain.Totals;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.prune.FilePruner;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.spatial.BoundsAccumulator;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

import io.tileverse.stac.StacFormatException;
import io.tileverse.stac.StacItem;

/**
 * A {@link GeoParquetDataset} over one STAC collection: each data part is a GeoParquet file named by a collection
 * item's data asset, with the item's bbox standing in for the file's spatial bounds. Before each query the dataset
 * prunes the parts whose item bbox cannot match the predicate and opens a {@link ParquetSource} over only the
 * survivors. Pruning is pure work-avoidance; the result is identical to scanning every part. Schema and geo metadata
 * are read once from a representative part.
 *
 * <p>No part is opened at construction: the dataset holds the item references and a shared {@link ContainerStorages},
 * and opens each part's byte reader on first access, memoizing one {@link ParquetSource} per part. The dataset owns the
 * readers it opens and releases them through {@link #closeResources()}, which the catalog calls before closing the
 * Storage registry.
 */
public final class StacDataset implements GeoParquetDataset {

    private final String name;
    private final List<StacItemRef> items;
    private final List<FileStats> fileStats;
    private final ContainerStorages storages;
    private final OpenOptions openOptions;
    private final ConcurrentHashMap<Integer, ParquetSource> perFileDatasets = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<RangeReader> openedReaders = new ConcurrentLinkedQueue<>();

    private volatile ParquetSchema schema;
    private volatile Optional<GeoParquetMetadata> geoMetadata;

    public StacDataset(
            String name,
            String geometryColumn,
            List<StacItemRef> items,
            List<double[]> itemBboxes,
            ContainerStorages storages,
            OpenOptions openOptions) {
        this.name = Objects.requireNonNull(name, "name");
        Objects.requireNonNull(geometryColumn, "geometryColumn");
        this.items = List.copyOf(items);
        this.storages = Objects.requireNonNull(storages, "storages");
        this.openOptions = Objects.requireNonNull(openOptions, "openOptions");
        this.fileStats = buildStats(this.items, itemBboxes, geometryColumn);
        if (this.items.isEmpty()) {
            throw new IllegalArgumentException("collection '" + name + "' has no GeoParquet data parts");
        }
    }

    private static List<FileStats> buildStats(List<StacItemRef> items, List<double[]> bboxes, String geometryColumn) {
        if (items.size() != bboxes.size()) {
            throw new IllegalArgumentException("item and bbox counts differ");
        }
        List<FileStats> stats = new ArrayList<>(items.size());
        for (int index = 0; index < items.size(); index++) {
            StacItem item =
                    new StacItem(items.get(index).itemId(), bboxes.get(index), Optional.empty(), List.of(), List.of());
            stats.add(StacFileStats.from(item, geometryColumn));
        }
        return stats;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ParquetSchema schema() {
        ParquetSchema resolved = schema;
        if (resolved == null) {
            resolved = perFile(0).schema();
            schema = resolved;
        }
        return resolved;
    }

    @Override
    public Optional<CatalogSnapshot> snapshot() {
        return Optional.empty();
    }

    // A null field marks the not-yet-resolved lazy state, distinct from a resolved Optional.empty() (no geo metadata).
    @SuppressWarnings("java:S2789")
    @Override
    public Optional<GeoParquetMetadata> geoMetadata() {
        Optional<GeoParquetMetadata> resolved = geoMetadata;
        if (resolved == null) {
            resolved = parseGeo(perFile(0));
            geoMetadata = resolved;
        }
        return resolved;
    }

    @Override
    public DatasetCapabilities capabilities() {
        return DatasetCapabilities.builder()
                .fileStats(FileStatsSource.STAC_ITEM)
                .fileSpatialBounds(FileSpatialBounds.NATIVE_GEO)
                .cheapCount(false)
                .cheapBounds(geoMetadata().flatMap(StacDataset::primaryBbox).isPresent())
                .build();
    }

    @Override
    public Optional<BoundingBox> bounds(Predicate predicate, ReadOptions options) {
        if (isUnfiltered(predicate)) {
            Optional<BoundingBox> metadataBox = geoMetadata().flatMap(StacDataset::primaryBbox);
            if (metadataBox.isPresent()) {
                return metadataBox;
            }
        }
        return boundsOfSurvivors(survivorsByDescendingItemArea(prune(predicate)), predicate, options);
    }

    /**
     * The exact bounds of the {@code predicate}-matching rows across the survivor parts, each part visited on its own
     * virtual thread and folded into one shared accumulator. Before a part opens, its cheap item box is tested against
     * the bounds accumulated so far, and a part those bounds already cover is skipped: a box already enclosed extends
     * the extent by nothing. A part with no item box always runs the engine. A failure in any part cancels the rest.
     */
    private Optional<BoundingBox> boundsOfSurvivors(List<Integer> survivors, Predicate predicate, ReadOptions options) {
        if (survivors.isEmpty()) {
            return Optional.empty();
        }
        ParquetSchema representative = schema();
        BoundsAccumulator accumulator = new BoundsAccumulator();
        try (StructuredTaskScope<Void, Void> scope = StructuredTaskScope.open()) {
            for (int index : survivors) {
                scope.fork(() -> foldOnePartBounds(index, predicate, options, accumulator, representative));
            }
            scope.join();
            return accumulator.snapshot();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted =
                    new InterruptedIOException("Interrupted while bounding collection rows");
            interrupted.initCause(e);
            throw new UncheckedIOException(interrupted);
        } catch (StructuredTaskScope.FailedException e) {
            throw asUnchecked(e.getCause());
        }
    }

    /**
     * Folds one survivor part's matching-row bounds into the shared accumulator. STAC folds no per-part predicate: the
     * original predicate visits each part unchanged. A part whose cheap pre-open item box the accumulated bounds
     * already cover is skipped before the part opens. Runs on a fan-out virtual thread.
     */
    private Void foldOnePartBounds(
            int index,
            Predicate predicate,
            ReadOptions options,
            BoundsAccumulator accumulator,
            ParquetSchema representative) {
        if (itemBoxCovered(index, accumulator)) {
            return null;
        }
        perFileChecked(index, representative).bounds(predicate, options).ifPresent(accumulator::union);
        return null;
    }

    /**
     * Whether the accumulated bounds already cover this part's item box. The item box is a conservative pre-open box
     * (the STAC item bbox, no narrower than the part's true extent). Covering that box implies covering the part. A
     * part with no item box is never reported as covered: an unknown extent might reach past the accumulated bounds.
     */
    private boolean itemBoxCovered(int index, BoundsAccumulator accumulator) {
        Optional<BoundingBox> itemBox = itemBox(index);
        return itemBox.isPresent() && accumulator.covers(itemBox.orElseThrow());
    }

    /**
     * Orders survivors by descending item-box area. Item boxes are free before any part opens. Visiting the widest
     * first seeds the shared accumulator fast, giving the containment skip the best chance to drop a narrower part a
     * wider one already encloses. A part with no item box sorts last and always visits.
     */
    private List<Integer> survivorsByDescendingItemArea(List<Integer> survivors) {
        List<Integer> ordered = new ArrayList<>(survivors);
        ordered.sort(Comparator.comparingDouble(this::itemBoxArea).reversed());
        return ordered;
    }

    private double itemBoxArea(int index) {
        Optional<BoundingBox> itemBox = itemBox(index);
        if (itemBox.isEmpty()) {
            return Double.NEGATIVE_INFINITY;
        }
        BoundingBox box = itemBox.orElseThrow();
        double width = box.xmax() - box.xmin();
        double height = box.ymax() - box.ymin();
        return width * height;
    }

    private Optional<BoundingBox> itemBox(int index) {
        return fileStats.get(index).geometryBounds().values().stream().findFirst();
    }

    /**
     * Rethrows a per-part bounds failure with the type a sequential visit would have thrown. The engine's per-part
     * bounds declares no checked exception: the cause is a {@link RuntimeException} or an {@link Error}, with anything
     * else falling back to {@link IllegalStateException}.
     */
    private static RuntimeException asUnchecked(Throwable cause) {
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("Bounding a collection part failed", cause);
    }

    @Override
    @MustBeClosed
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        return readSurvivors(predicate, projection, Materializer.defaultRecord(), options);
    }

    @Override
    @MustBeClosed
    public <T> Stream<T> read(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        return readSurvivors(predicate, projection, materializer, options);
    }

    /**
     * Reads the survivor parts' rows, draining them concurrently. A spatial-decimation probe pins the per-part visit
     * order onto a single thread, which the fan-out would break; a probe read therefore keeps the sequential per-part
     * composition. Otherwise each survivor's rows ride its columnar batches across the fan-out and flatten to rows on
     * the consuming thread.
     */
    @MustBeClosed
    private <T> Stream<T> readSurvivors(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        List<Integer> survivors = prune(predicate);
        if (survivors.isEmpty()) {
            return Stream.empty();
        }
        ParquetSchema representative = schema();
        if (options.spatialReadProbe().isPresent()) {
            return ConcurrentSurvivorReads.sequential(
                    survivors.size(),
                    dense -> perFileChecked(survivors.get(dense), representative)
                            .read(predicate, projection, materializer, options));
        }
        return ConcurrentSurvivorReads.records(
                survivors.size(),
                dense -> perFileChecked(survivors.get(dense), representative)
                        .readBatches(predicate, projection, options),
                materializer,
                maxConcurrentFiles());
    }

    @Override
    @MustBeClosed
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        List<Integer> survivors = prune(predicate);
        if (survivors.isEmpty()) {
            return Stream.empty();
        }
        ParquetSchema representative = schema();
        if (options.spatialReadProbe().isPresent()) {
            return survivors.stream()
                    .flatMap(
                            index -> perFileChecked(index, representative).readBatches(predicate, projection, options));
        }
        return ConcurrentSurvivorReads.batches(
                survivors.size(),
                dense -> perFileChecked(survivors.get(dense), representative)
                        .readBatches(predicate, projection, options),
                maxConcurrentFiles());
    }

    @Override
    public long count(Predicate predicate, ReadOptions options) {
        List<Integer> survivors = prune(predicate);
        if (survivors.isEmpty()) {
            return 0L;
        }
        ParquetSchema representative = schema();
        long total = 0L;
        for (int index : survivors) {
            total += perFileChecked(index, representative).count(predicate, options);
        }
        return total;
    }

    @Override
    public DatasetExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options) {
        return buildExplain(predicate, projection, options, false);
    }

    @Override
    public DatasetExplainPlan explainAnalyze(Predicate predicate, Projection projection, ReadOptions options) {
        return buildExplain(predicate, projection, options, true);
    }

    private DatasetExplainPlan buildExplain(
            Predicate predicate, Projection projection, ReadOptions options, boolean analyze) {
        List<FileExplain> files = new ArrayList<>(items.size());
        for (int index = 0; index < fileStats.size(); index++) {
            PruningDecision decision = FilePruner.evaluate(predicate, fileStats.get(index));
            files.add(fileExplain(index, decision, predicate, projection, options, analyze));
        }
        return new DatasetExplainPlan(predicate, files, Totals.from(files));
    }

    private FileExplain fileExplain(
            int index,
            PruningDecision decision,
            Predicate predicate,
            Projection projection,
            ReadOptions options,
            boolean analyze) {
        String location = items.get(index).href();
        OptionalLong unknownCount = OptionalLong.empty();
        if (decision instanceof PruningDecision.Eliminated ruledOut) {
            return new FileExplain(location, Outcome.SKIP, ruledOut.reason(), unknownCount, Optional.empty());
        }
        ParquetSource survivor = perFile(index);
        Query query = Query.of(predicate, projection);
        ExplainPlan plan = analyze ? survivor.explainAnalyze(query, options) : survivor.explain(query, options);
        return new FileExplain(location, Outcome.KEEP, "kept", unknownCount, Optional.of(plan));
    }

    private List<Integer> prune(Predicate predicate) {
        Objects.requireNonNull(predicate, "predicate");
        List<Integer> survivors = new ArrayList<>();
        for (int index = 0; index < fileStats.size(); index++) {
            PruningDecision decision = FilePruner.evaluate(predicate, fileStats.get(index));
            if (!(decision instanceof PruningDecision.Eliminated)) {
                survivors.add(index);
            }
        }
        return survivors;
    }

    /**
     * The {@link ParquetSource} for survivor {@code index}, its schema validated against the collection's
     * {@code representative} schema before the part is read. STAC reads each part as its own single-file source and
     * concatenates the rows, which bypasses the engine's multi-file schema check; a part whose columns differ from the
     * representative would otherwise yield wrong rows. The check runs as each part is opened for reading, not up front,
     * and a {@code limit(1)} read therefore does not open every survivor's footer. A mismatch throws
     * {@link StacFormatException} before the part's rows are emitted, naming the item.
     *
     * <p>Equality is exact: a part with extra or missing columns is rejected, not read as a superset. That is
     * deliberate - reading parts with drifting schemas as if homogeneous silently corrupts rows, and a loud failure is
     * safer than a silent wrong answer.
     */
    private ParquetSource perFileChecked(int index, ParquetSchema representative) {
        ParquetSource part = perFile(index);
        if (!representative.equals(part.schema())) {
            StacItemRef ref = items.get(index);
            throw new StacFormatException("collection '" + name + "' has parts with differing schemas; item '"
                    + ref.itemId() + "' (" + ref.href() + ") does not match the collection schema");
        }
        return part;
    }

    /**
     * The single-part {@link ParquetSource} over the part at {@code index}, parsed once and reused across queries and
     * threads. {@code computeIfAbsent} opens the part at most once per index even under concurrent reads: the first
     * caller opens its byte reader and parses the footer; later callers reuse the memoized source.
     */
    private ParquetSource perFile(int index) {
        return perFileDatasets.computeIfAbsent(index, this::openPerFile);
    }

    /**
     * Opens the part at {@code index}: resolves the asset href to its container Storage, opens a byte reader the
     * dataset owns, and parses the footer. The reader is tracked for {@link #closeResources()} only after the footer
     * parses; a parse failure closes the reader instead of tracking it, and a repeatedly failing part therefore never
     * accumulates open readers. The {@link ByteRangeSource} wrapping the reader borrows it and never closes it.
     *
     * <p>{@link StacItemRef#href()} must be an absolute URI. The in-tree catalog readers absolutize every asset href
     * before building the item references; a custom {@link io.tileverse.stac.StacCatalogReader} must do the same.
     */
    private ParquetSource openPerFile(int index) {
        URI assetUri = URI.create(items.get(index).href());
        Storage storage = storages.storageFor(assetUri.resolve("."));
        RangeReader reader = storage.openRangeReader(assetUri);
        try {
            ByteRangeSource source = ByteRangeSources.from(reader);
            ParquetSource opened = ParquetSource.open(source, openOptions);
            openedReaders.add(reader);
            return opened;
        } catch (RuntimeException openFailure) {
            closeOnFailure(reader, openFailure);
            throw openFailure;
        }
    }

    /**
     * Closes a reader whose part failed to open, folding any close error into the failure that triggered the cleanup.
     */
    private static void closeOnFailure(AutoCloseable closeable, RuntimeException primary) {
        try {
            closeable.close();
        } catch (Exception closeError) {
            primary.addSuppressed(closeError);
        }
    }

    /**
     * The number of part readers this dataset currently holds open. Package-private for tests that assert lazy opening
     * and the absence of a reader leak on a failing part.
     */
    int openReaderCount() {
        return openedReaders.size();
    }

    private int maxConcurrentFiles() {
        return openOptions.runtime().maxConcurrentFiles();
    }

    private Optional<GeoParquetMetadata> parseGeo(ParquetSource representative) {
        String geoJson = representative.keyValueMetadata().get("geo");
        if (geoJson == null || geoJson.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(GeoParquetMetadata.parse(geoJson));
        } catch (RuntimeException unparseable) {
            return Optional.empty();
        }
    }

    private static Optional<BoundingBox> primaryBbox(GeoParquetMetadata geo) {
        GeoColumn primary = geo.columns().get(geo.primaryColumn());
        return primary == null ? Optional.empty() : primary.bbox();
    }

    private static boolean isUnfiltered(Predicate predicate) {
        return predicate instanceof Predicate.Always(boolean value) && value;
    }

    /**
     * Closes the byte readers this dataset opened lazily. Borrowed by the catalog, which calls this before closing the
     * Storage registry. A failure closing one reader still closes the rest; the first is rethrown with the others
     * suppressed.
     */
    void closeResources() {
        RuntimeException failure = null;
        for (RangeReader reader : openedReaders) {
            failure = closeChaining(reader, failure);
        }
        openedReaders.clear();
        perFileDatasets.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException closeChaining(AutoCloseable closeable, RuntimeException accumulated) {
        try {
            closeable.close();
            return accumulated;
        } catch (RuntimeException alreadyUnchecked) {
            return accumulated == null ? alreadyUnchecked : addSuppressed(accumulated, alreadyUnchecked);
        } catch (Exception checked) {
            IllegalStateException wrapped = new IllegalStateException("closing " + closeable, checked);
            return accumulated == null ? wrapped : addSuppressed(accumulated, wrapped);
        }
    }

    private static RuntimeException addSuppressed(RuntimeException accumulated, RuntimeException next) {
        accumulated.addSuppressed(next);
        return accumulated;
    }
}
