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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
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
import io.tileverse.parquetry.dataset.SurvivorFanOut;
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
import io.tileverse.parquetry.schema.geo.projjson.Identifier;
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
 * and opens each part's byte reader on first access, memoizing one {@link ParquetSource} per part. The memo is bounded:
 * past {@link #DEFAULT_MAX_MEMOIZED_PARTS} parts, or after ten idle minutes, a part is dropped and its reader closed,
 * which a later query reopens transparently. Every reader opened this way belongs to the dataset: a part dropped by the
 * memo closes when the last read releases it. The dataset keeps every open part on its books, and
 * {@link #closeResources()} closes them all, memoized or not. The catalog calls that before closing the Storage
 * registry, and the dataset never outlives its catalog.
 */
public final class StacDataset implements GeoParquetDataset {

    /**
     * The largest number of parts kept open at once by one collection. A collection of several hundred parts would
     * otherwise keep the reader and the footer metadata of every visited part for the dataset's lifetime. Dropping a
     * part costs little: reopening reads its footer from the shared footer-metadata cache rather than from storage.
     */
    static final int DEFAULT_MAX_MEMOIZED_PARTS = 128;

    /** How long a memoized part outlives the last query to touch it. */
    private static final Duration MEMO_EXPIRY = Duration.ofMinutes(10);

    private final String name;
    private final String geometryColumn;
    private final Supplier<Parts> partsSupplier;
    private final Supplier<Optional<StacItemRef>> firstRefSupplier;
    private final Optional<BoundingBox> collectionBounds;
    private final ContainerStorages storages;
    private final OpenOptions openOptions;
    private final Cache<Integer, PartLease> memoizedParts;

    /** Every lease whose reader is open, whether the memo still holds it or only an unclosed read does. */
    private final Set<PartLease> openLeases = ConcurrentHashMap.newKeySet();

    // Each lazy field below is written once with an immutable snapshot; volatile is the correct publish for that.
    @SuppressWarnings("java:S3077")
    private volatile ParquetSchema schema;

    @SuppressWarnings("java:S3077")
    private volatile Optional<GeoParquetMetadata> geoMetadata;

    @SuppressWarnings("java:S3077")
    private volatile Materialized materialized;

    // A null field marks the not-yet-resolved lazy state, distinct from a resolved Optional.empty() (no first part).
    @SuppressWarnings({"java:S2789", "java:S3077"})
    private volatile Optional<StacItemRef> firstRef;

    /** The collection's resolved data parts: one GeoParquet part reference and its declared item box per item. */
    public record Parts(List<StacItemRef> refs, List<double[]> itemBboxes) {
        public Parts {
            refs = List.copyOf(refs);
            itemBboxes = List.copyOf(itemBboxes);
            if (refs.size() != itemBboxes.size()) {
                throw new IllegalArgumentException("item and bbox counts differ");
            }
        }
    }

    private record Materialized(List<StacItemRef> refs, List<FileStats> fileStats) {}

    public StacDataset(
            String name,
            String geometryColumn,
            List<StacItemRef> items,
            List<double[]> itemBboxes,
            ContainerStorages storages,
            OpenOptions openOptions) {
        this(name, geometryColumn, items, itemBboxes, Optional.empty(), storages, openOptions);
    }

    /**
     * A dataset over the parts already resolved by the caller. The parts materialize during construction, and a
     * collection with no data parts is rejected here.
     */
    public StacDataset(
            String name,
            String geometryColumn,
            List<StacItemRef> items,
            List<double[]> itemBboxes,
            Optional<BoundingBox> collectionBounds,
            ContainerStorages storages,
            OpenOptions openOptions) {
        if (items.isEmpty()) {
            throw new IllegalArgumentException("collection '" + name + "' has no GeoParquet data parts");
        }
        this(
                name,
                geometryColumn,
                eagerParts(items, itemBboxes),
                null,
                collectionBounds,
                storages,
                openOptions,
                DEFAULT_MAX_MEMOIZED_PARTS);
        materialize();
    }

    private static Supplier<Parts> eagerParts(List<StacItemRef> items, List<double[]> itemBboxes) {
        Parts parts = new Parts(items, itemBboxes);
        return () -> parts;
    }

    /**
     * A dataset whose parts resolve lazily: {@code partsSupplier} reads the collection's item documents on first
     * per-part need, and {@code firstRefSupplier} resolves the first part alone, letting {@link #schema()} and
     * {@link #geoMetadata()} pay one item document instead of the whole enumeration. The first supplier's ref must be
     * the first element of the materialized parts (both follow the collection's item link order); a first item with no
     * data part resolves empty and defers to the full materialization. A null {@code firstRefSupplier} always defers.
     */
    public StacDataset(
            String name,
            String geometryColumn,
            Supplier<Parts> partsSupplier,
            Supplier<Optional<StacItemRef>> firstRefSupplier,
            Optional<BoundingBox> collectionBounds,
            ContainerStorages storages,
            OpenOptions openOptions) {
        this(
                name,
                geometryColumn,
                partsSupplier,
                firstRefSupplier,
                collectionBounds,
                storages,
                openOptions,
                DEFAULT_MAX_MEMOIZED_PARTS);
    }

    /**
     * A lazy dataset whose part memo holds at most {@code maxMemoizedParts} parts at once. Every other constructor
     * reaches this one with {@link #DEFAULT_MAX_MEMOIZED_PARTS}. The supplier contract is the one stated on the public
     * lazy constructor.
     */
    // S107: the collection's resolved state and its memo bound arrive together from one place.
    @SuppressWarnings("java:S107")
    StacDataset(
            String name,
            String geometryColumn,
            Supplier<Parts> partsSupplier,
            Supplier<Optional<StacItemRef>> firstRefSupplier,
            Optional<BoundingBox> collectionBounds,
            ContainerStorages storages,
            OpenOptions openOptions,
            int maxMemoizedParts) {
        this.name = Objects.requireNonNull(name, "name");
        this.geometryColumn = Objects.requireNonNull(geometryColumn, "geometryColumn");
        this.partsSupplier = Objects.requireNonNull(partsSupplier, "partsSupplier");
        this.firstRefSupplier = firstRefSupplier;
        this.collectionBounds = Objects.requireNonNull(collectionBounds, "collectionBounds");
        this.storages = Objects.requireNonNull(storages, "storages");
        this.openOptions = Objects.requireNonNull(openOptions, "openOptions");
        this.memoizedParts = buildPartMemo(maxMemoizedParts);
    }

    /**
     * The bounded part memo. Eviction gives the memo's hold back, which closes the part's reader unless a read still
     * holds it. The scheduler matters for an idle collection: without it an expired part is dropped only when the next
     * query touches the memo, and a collection left unqueried never gives its readers back.
     *
     * <p>An eviction that does close a reader closes it inside Caffeine's own removal, under the entry's lock, on
     * whichever thread runs the maintenance - a pool or scheduler thread, or the query thread that tripped the bound.
     * Accepted deliberately: the work is one reader close, it happens only when no read holds the part, and only once
     * per part dropped.
     */
    private Cache<Integer, PartLease> buildPartMemo(int maxMemoizedParts) {
        return Caffeine.newBuilder()
                .maximumSize(maxMemoizedParts)
                .expireAfterAccess(MEMO_EXPIRY)
                .scheduler(Scheduler.systemScheduler())
                .<Integer, PartLease>evictionListener((Integer _, PartLease lease, RemovalCause _) -> lease.release())
                .build();
    }

    /** The materialized parts, resolved from the supplier once and reused across queries and threads. */
    private synchronized Materialized materialize() {
        Materialized resolved = materialized;
        if (resolved != null) {
            return resolved;
        }
        Parts parts = partsSupplier.get();
        resolved = new Materialized(parts.refs(), buildStats(parts.refs(), parts.itemBboxes(), geometryColumn));
        materialized = resolved;
        return resolved;
    }

    private List<StacItemRef> refs() {
        return materialize().refs();
    }

    private List<FileStats> fileStats() {
        return materialize().fileStats();
    }

    /**
     * The part reference at {@code index}. Index zero resolves through the first-ref fast path while the parts are
     * unmaterialized, letting a schema probe read one item document; any other index, or an empty fast path,
     * materializes the full list.
     */
    @SuppressWarnings("java:S2789")
    private StacItemRef ref(int index) {
        Materialized resolved = materialized;
        if (resolved != null) {
            return resolved.refs().get(index);
        }
        if (index == 0 && firstRefSupplier != null) {
            Optional<StacItemRef> first = firstRef;
            if (first == null) {
                first = firstRefSupplier.get();
                firstRef = first;
            }
            if (first.isPresent()) {
                return first.orElseThrow();
            }
        }
        return refs().get(index);
    }

    private static List<FileStats> buildStats(List<StacItemRef> items, List<double[]> bboxes, String geometryColumn) {
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
            try (PartLease lease = acquirePart(0)) {
                resolved = lease.source().schema();
            }
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
            try (PartLease lease = acquirePart(0)) {
                resolved = parseGeo(lease.source());
            }
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
                .cheapBounds(hasDeclaredDatasetBounds())
                .build();
    }

    /**
     * Whether a dataset-level declared box exists: a collection extent, or a single part whose own geo metadata box is
     * the whole dataset's. A multi-part collection's first-part box covers that part alone and never answers cheaply.
     */
    private boolean hasDeclaredDatasetBounds() {
        if (collectionBounds.isPresent()) {
            return true;
        }
        return singlePartMetadataBox().isPresent();
    }

    /**
     * A single part's own geo metadata box, which is the whole dataset's. Answered only once the parts are
     * materialized: counting parts requires them, and a capabilities probe on a still-lazy dataset must stay cheap.
     */
    private Optional<BoundingBox> singlePartMetadataBox() {
        Materialized resolved = materialized;
        if (resolved == null || resolved.refs().size() != 1) {
            return Optional.empty();
        }
        return geoMetadata().flatMap(StacDataset::primaryBbox);
    }

    @Override
    public Optional<BoundingBox> bounds(Predicate predicate, ReadOptions options) {
        if (isUnfiltered(predicate)) {
            Optional<BoundingBox> declared = declaredBounds();
            if (declared.isPresent()) {
                return declared;
            }
        }
        return boundsOfSurvivors(survivorsByDescendingItemArea(prune(predicate)), predicate, options);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Answered from the declared collection extent for the unfiltered query, else the union of the surviving parts'
     * item boxes. Item boxes come from the item documents alone; no part footer is read.
     */
    @Override
    public Optional<BoundingBox> estimatedBounds(Predicate predicate) {
        if (isUnfiltered(predicate)) {
            Optional<BoundingBox> declared = declaredBounds();
            if (declared.isPresent()) {
                return declared;
            }
        }
        BoundsAccumulator union = new BoundsAccumulator();
        for (int index : prune(predicate)) {
            Optional<BoundingBox> box = itemBox(index);
            if (box.isEmpty()) {
                return Optional.empty();
            }
            union.union(box.orElseThrow());
        }
        return union.snapshot();
    }

    /**
     * The dataset-level declared box answering an unfiltered bounds query without a scan: the STAC collection extent
     * when the data CRS is the GeoParquet WGS84 default (a STAC extent is WGS84 by spec, and a dataset in another CRS
     * cannot use it), else a single part's own geo metadata box.
     */
    private Optional<BoundingBox> declaredBounds() {
        if (collectionBounds.isPresent() && crsIsWgs84Default()) {
            return collectionBounds;
        }
        return singlePartMetadataBox();
    }

    /**
     * Whether the primary geometry column's CRS is the GeoParquet spec default (OGC:CRS84 - WGS 84). An absent geo
     * document and an absent {@code crs} field both mean the default applies; an explicit CRS matches through its
     * identifier, never structurally.
     */
    private boolean crsIsWgs84Default() {
        Optional<GeoParquetMetadata> geo = geoMetadata();
        if (geo.isEmpty()) {
            return true;
        }
        GeoColumn primary = geo.orElseThrow().columns().get(geo.orElseThrow().primaryColumn());
        if (primary == null || primary.crs().isEmpty()) {
            return true;
        }
        return primary.crs()
                .orElseThrow()
                .id()
                .map(StacDataset::isWgs84Identifier)
                .orElse(false);
    }

    /** OGC:CRS84 and EPSG:4326 both name WGS 84; axis order does not change a 2D extent box. */
    private static boolean isWgs84Identifier(Identifier id) {
        boolean ogcCrs84 = "OGC".equalsIgnoreCase(id.authority()) && "CRS84".equalsIgnoreCase(id.code());
        boolean epsg4326 = "EPSG".equalsIgnoreCase(id.authority()) && "4326".equals(id.code());
        return ogcCrs84 || epsg4326;
    }

    /**
     * The exact bounds of the {@code predicate}-matching rows across the survivor parts, folded into one shared
     * accumulator with at most {@code maxConcurrentFiles} parts bounded at once. Before a part opens, its cheap item
     * box is tested against the bounds accumulated so far, and a part those bounds already cover is skipped: a box
     * already enclosed extends the extent by nothing. A part with no item box always runs the engine. A failure in any
     * part cancels the rest.
     */
    private Optional<BoundingBox> boundsOfSurvivors(List<Integer> survivors, Predicate predicate, ReadOptions options) {
        if (survivors.isEmpty()) {
            return Optional.empty();
        }
        ParquetSchema representative = schema();
        BoundsAccumulator accumulator = new BoundsAccumulator();
        SurvivorFanOut.forEach(
                survivors.size(),
                dense -> foldOnePartBounds(survivors.get(dense), predicate, options, accumulator, representative),
                maxConcurrentFiles());
        return accumulator.snapshot();
    }

    /**
     * Folds one survivor part's matching-row bounds into the shared accumulator. STAC folds no per-part predicate: the
     * original predicate visits each part unchanged. A part whose cheap pre-open item box the accumulated bounds
     * already cover is skipped before the part opens. Runs on a fan-out virtual thread, or on the calling thread when
     * there is a single survivor.
     */
    private void foldOnePartBounds(
            int index,
            Predicate predicate,
            ReadOptions options,
            BoundsAccumulator accumulator,
            ParquetSchema representative) {
        if (itemBoxCovered(index, accumulator)) {
            return;
        }
        try (PartLease lease = acquirePart(index)) {
            checkedPart(lease, index, representative).bounds(predicate, options).ifPresent(accumulator::union);
        }
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
        return fileStats().get(index).geometryBounds().values().stream().findFirst();
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
                    dense -> readOnePart(
                            survivors.get(dense), representative, predicate, projection, materializer, options));
        }
        return ConcurrentSurvivorReads.records(
                survivors.size(),
                dense -> readOnePartBatches(survivors.get(dense), representative, predicate, projection, options),
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
            return ConcurrentSurvivorReads.sequential(
                    survivors.size(),
                    dense -> readOnePartBatches(survivors.get(dense), representative, predicate, projection, options));
        }
        return ConcurrentSurvivorReads.batches(
                survivors.size(),
                dense -> readOnePartBatches(survivors.get(dense), representative, predicate, projection, options),
                maxConcurrentFiles());
    }

    /** One survivor part's rows, with the part held open for as long as the returned stream is. */
    @MustBeClosed
    private <T> Stream<T> readOnePart(
            int index,
            ParquetSchema representative,
            Predicate predicate,
            Projection projection,
            Materializer<T> materializer,
            ReadOptions options) {
        return streamHoldingPart(
                index, representative, part -> part.read(predicate, projection, materializer, options));
    }

    /** One survivor part's columnar batches, held open exactly as {@link #readOnePart} holds its part. */
    @MustBeClosed
    private Stream<ParquetRecordBatch> readOnePartBatches(
            int index, ParquetSchema representative, Predicate predicate, Projection projection, ReadOptions options) {
        return streamHoldingPart(index, representative, part -> part.readBatches(predicate, projection, options));
    }

    /**
     * Opens one survivor part's stream through {@code open}, holding the part until that stream closes. The hold goes
     * back through the stream's close hook, which the cross-part merge fires as it finishes each part.
     *
     * <p>A failure before the stream exists gives the hold back instead. An {@link Error} counts as such a failure: the
     * fan-out above catches {@link Throwable} and the process keeps running, and a hold stranded here would keep the
     * part's reader open for the rest of the dataset's life.
     */
    @MustBeClosed
    // S1181: see above - an Error must give the hold back too, or the part's reader never closes.
    @SuppressWarnings("java:S1181")
    private <S> Stream<S> streamHoldingPart(
            int index, ParquetSchema representative, Function<ParquetSource, Stream<S>> open) {
        PartLease lease = acquirePart(index);
        try {
            return open.apply(checkedPart(lease, index, representative)).onClose(lease::release);
        } catch (Throwable failure) {
            releaseOnFailure(lease, failure);
            throw failure;
        }
    }

    /** Gives a hold back after a failed open, folding any close error into the failure that triggered the cleanup. */
    private static void releaseOnFailure(PartLease lease, Throwable primary) {
        try {
            lease.release();
        } catch (RuntimeException closeError) {
            primary.addSuppressed(closeError);
        }
    }

    @Override
    public long count(Predicate predicate, ReadOptions options) {
        List<Integer> survivors = prune(predicate);
        if (survivors.isEmpty()) {
            return 0L;
        }
        ParquetSchema representative = schema();
        return SurvivorFanOut.sum(
                survivors.size(),
                dense -> countOnePart(survivors.get(dense), representative, predicate, options),
                maxConcurrentFiles());
    }

    private long countOnePart(int index, ParquetSchema representative, Predicate predicate, ReadOptions options) {
        try (PartLease lease = acquirePart(index)) {
            return checkedPart(lease, index, representative).count(predicate, options);
        }
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
        List<FileStats> stats = fileStats();
        List<FileExplain> files = new ArrayList<>(stats.size());
        for (int index = 0; index < stats.size(); index++) {
            PruningDecision decision = FilePruner.evaluate(predicate, stats.get(index));
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
        String location = refs().get(index).href();
        OptionalLong unknownCount = OptionalLong.empty();
        if (decision instanceof PruningDecision.Eliminated ruledOut) {
            return new FileExplain(location, Outcome.SKIP, ruledOut.reason(), unknownCount, Optional.empty());
        }
        Query query = Query.of(predicate, projection);
        ExplainPlan plan = explainOnePart(index, query, options, analyze);
        return new FileExplain(location, Outcome.KEEP, "kept", unknownCount, Optional.of(plan));
    }

    private ExplainPlan explainOnePart(int index, Query query, ReadOptions options, boolean analyze) {
        try (PartLease lease = acquirePart(index)) {
            ParquetSource survivor = lease.source();
            return analyze ? survivor.explainAnalyze(query, options) : survivor.explain(query, options);
        }
    }

    private List<Integer> prune(Predicate predicate) {
        Objects.requireNonNull(predicate, "predicate");
        List<FileStats> stats = fileStats();
        List<Integer> survivors = new ArrayList<>();
        for (int index = 0; index < stats.size(); index++) {
            PruningDecision decision = FilePruner.evaluate(predicate, stats.get(index));
            if (!(decision instanceof PruningDecision.Eliminated)) {
                survivors.add(index);
            }
        }
        return survivors;
    }

    /**
     * The leased part's {@link ParquetSource}, its schema validated against the collection's {@code representative}
     * schema before the part is read. STAC reads each part as its own single-file source and concatenates the rows;
     * concatenating a part whose columns differ from the representative would yield wrong rows. The check runs as each
     * part is opened for reading, not up front, and a {@code limit(1)} read therefore does not open every survivor's
     * footer. A mismatch throws {@link StacFormatException} before the part's rows are emitted, naming the item.
     *
     * <p>Equality is exact: a part with extra or missing columns is rejected, not read as a superset. That is
     * deliberate - reading parts with drifting schemas as if homogeneous silently corrupts rows, and a loud failure is
     * safer than a silent wrong answer.
     */
    private ParquetSource checkedPart(PartLease lease, int index, ParquetSchema representative) {
        ParquetSource part = lease.source();
        if (!representative.equals(part.schema())) {
            StacItemRef ref = refs().get(index);
            throw new StacFormatException("collection '" + name + "' has parts with differing schemas; item '"
                    + ref.itemId() + "' (" + ref.href() + ") does not match the collection schema");
        }
        return part;
    }

    /**
     * Holds the part at {@code index} open for the caller, opening and memoizing it on first need. The caller releases
     * the hold once it is done with the part; until then eviction cannot close the part's reader.
     *
     * <p>The hold is taken inside the memo's own atomic update. That is what closes the window between finding a part
     * and holding it: an eviction landing in that window would otherwise close the reader that the caller is about to
     * read through. Opening inside the update also keeps a part to a single open per index under concurrent reads.
     */
    private PartLease acquirePart(int index) {
        return memoizedParts.asMap().compute(index, (Integer part, PartLease memoized) -> {
            PartLease lease = memoized == null ? openPart(part) : memoized;
            return lease.acquire();
        });
    }

    /**
     * Opens the part at {@code index}: resolves the asset href to its container Storage, opens a byte reader the
     * dataset owns, and parses the footer. The reader counts as open only after the footer parses; a parse failure
     * closes it instead, and a repeatedly failing part therefore never accumulates open readers. The
     * {@link ByteRangeSource} wrapping the reader borrows it and never closes it.
     *
     * <p>{@link StacItemRef#href()} must be an absolute URI. The in-tree catalog readers absolutize every asset href
     * before building the item references; a custom {@link io.tileverse.stac.StacCatalogReader} must do the same.
     */
    private PartLease openPart(int index) {
        URI assetUri = URI.create(ref(index).href());
        Storage storage = storages.storageFor(assetUri.resolve("."));
        RangeReader reader = storage.openRangeReader(assetUri);
        try {
            ByteRangeSource source = ByteRangeSources.from(reader);
            ParquetSource opened = ParquetSource.open(source, openOptions);
            PartLease lease = new PartLease(opened, reader);
            openLeases.add(lease);
            return lease;
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
     * The number of part readers currently held open by this dataset. Package-private for tests that assert lazy
     * opening, the absence of a reader leak on a failing part, and that an evicted part gives its reader back.
     */
    int openReaderCount() {
        return openLeases.size();
    }

    /** The number of parts currently held by the memo. Package-private for the tests over the memo's bound. */
    int memoizedPartCount() {
        return (int) memoizedParts.estimatedSize();
    }

    /**
     * Applies the evictions already decided by the memo's bound. Package-private for tests: Caffeine defers eviction to
     * a maintenance pass, and a test that has just crossed the bound must run that pass before reading the outcome.
     */
    void runPendingPartEvictions() {
        memoizedParts.cleanUp();
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
        } catch (RuntimeException _) {
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
     * Closes the byte readers opened lazily by this dataset. Every part still open closes here, exactly once and
     * regardless of the holds still out on it: the parts still in the memo, and equally the parts already dropped from
     * it while an unclosed read kept them alive. Borrowed by the catalog, which calls this before closing the Storage
     * registry; the dataset never outlives its catalog. A read still in flight fails on its next fetch from storage,
     * and giving its hold back afterwards closes nothing a second time. A failure closing one reader still closes the
     * rest; the first is rethrown with the others suppressed.
     */
    void closeResources() {
        RuntimeException failure = dropMemoizedParts(null);
        failure = closeLeasesLeftOpen(failure);
        if (failure != null) {
            throw failure;
        }
    }

    /** Empties the memo, closing the reader of every part still in it. */
    private RuntimeException dropMemoizedParts(RuntimeException accumulated) {
        ConcurrentMap<Integer, PartLease> memoized = memoizedParts.asMap();
        RuntimeException failure = accumulated;
        for (Integer index : List.copyOf(memoized.keySet())) {
            PartLease dropped = memoized.remove(index);
            if (dropped != null) {
                failure = closeChaining(dropped, failure);
            }
        }
        return failure;
    }

    /** Closes the reader of every part still open. */
    private RuntimeException closeLeasesLeftOpen(RuntimeException accumulated) {
        RuntimeException failure = accumulated;
        for (PartLease stranded : List.copyOf(openLeases)) {
            failure = closeChaining(stranded, failure);
        }
        return failure;
    }

    private static RuntimeException closeChaining(PartLease lease, RuntimeException accumulated) {
        try {
            lease.closeIgnoringHolds();
            return accumulated;
        } catch (RuntimeException closeFailure) {
            return accumulated == null ? closeFailure : addSuppressed(accumulated, closeFailure);
        }
    }

    private static RuntimeException addSuppressed(RuntimeException accumulated, RuntimeException next) {
        accumulated.addSuppressed(next);
        return accumulated;
    }

    /**
     * One memoized part: its {@link ParquetSource} and the {@link RangeReader} behind it, kept open for as long as
     * anything uses it. The count starts at one for the memo's own hold; a read takes one more and gives it back when
     * its stream closes. The reader closes when the last hold goes, which is the memo's own for an idle part and the
     * final read's for a part evicted mid-read. A lease counts as open from the moment when its reader opens until that
     * reader closes, and the dataset's drain reaches every open lease, memoized or not.
     *
     * <p>Three paths reach a lease from outside a read: the memo's eviction listener, the memo drain in
     * {@link StacDataset#closeResources()}, and that same close's sweep over the parts still open. The first two act on
     * an entry removed by their own atomic map operation, and whichever runs first is the one that sees the mapping;
     * the sweep needs no mapping at all and reaches a lease even while the memo still maps it. Eviction gives the
     * memo's hold back. The close marks the lease closed and shuts its reader, and every hold taken or given back after
     * that leaves the lease exactly as it is.
     */
    private final class PartLease implements AutoCloseable {

        /**
         * The hold count marking a lease already closed by the dataset's close. Taking and giving back holds never
         * reaches it.
         */
        private static final int CLOSED = Integer.MIN_VALUE;

        private final ParquetSource source;
        private final RangeReader reader;
        private final AtomicInteger holds = new AtomicInteger(1);

        private PartLease(ParquetSource source, RangeReader reader) {
            this.source = source;
            this.reader = reader;
        }

        private ParquetSource source() {
            return source;
        }

        /**
         * Takes one hold for a caller about to use the part. A lease already closed by the dataset's close takes no
         * hold and stays closed; the caller gets a part whose reader is gone, and its first fetch fails.
         */
        private PartLease acquire() {
            holds.updateAndGet(PartLease::oneHoldMore);
            return this;
        }

        private static int oneHoldMore(int held) {
            return held == CLOSED ? CLOSED : held + 1;
        }

        /**
         * Gives back one hold, closing the reader when it was the last one out. A hold given back on a lease already
         * closed by the dataset's close does nothing. A hold given back twice on a live lease is a programming error in
         * this class and fails loud rather than closing a reader still held by another caller.
         */
        private void release() {
            int remaining = holds.updateAndGet(PartLease::oneHoldFewer);
            if (remaining == CLOSED) {
                return;
            }
            if (remaining < 0) {
                throw new IllegalStateException(
                        "a part of collection '" + name + "' was released more often than it was held");
            }
            if (remaining == 0) {
                closeReader();
            }
        }

        private static int oneHoldFewer(int held) {
            return held == CLOSED ? CLOSED : held - 1;
        }

        @Override
        public void close() {
            release();
        }

        /**
         * Closes the part's reader regardless of the holds still out. The lease stays closed from here on, and giving
         * those holds back afterwards does nothing.
         */
        private void closeIgnoringHolds() {
            holds.set(CLOSED);
            closeReader();
        }

        /** Closes the reader once: the caller that takes the lease out of the open set is the one that closes it. */
        private void closeReader() {
            if (!openLeases.remove(this)) {
                return;
            }
            try {
                reader.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to close the reader of a part of collection '" + name + "'", e);
            }
        }
    }
}
