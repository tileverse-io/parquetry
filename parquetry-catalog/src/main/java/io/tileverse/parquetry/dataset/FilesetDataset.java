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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.SequencedSet;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.dataset.explain.FileExplain;
import io.tileverse.parquetry.dataset.explain.Outcome;
import io.tileverse.parquetry.dataset.explain.Totals;
import io.tileverse.parquetry.filter.ConstantColumn;
import io.tileverse.parquetry.filter.ConstantFolding;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.prune.FilePruner;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.spatial.BoundsAccumulator;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * A {@link GeoParquetDataset} composed from one or many same-schema Parquet files. Before each query it prunes files
 * whose Hive partition value cannot match the predicate (the same {@link FilePruner} path the Iceberg backend uses, fed
 * from path values as exact statistics) and reads only the survivors. The dataset keeps only compact per-file planning
 * state (schema, partition values, footer-derived {@link FileStats}): each query opens a surviving file's reader when
 * its read reaches that file, at most {@code maxConcurrentFiles} at a time, and lets the reader go when the file is
 * drained. A one-file dataset is the exception - it keeps the {@link ParquetSource} the catalog's gather pass already
 * parsed, since re-decoding a single large footer per query would cost more than it retains.
 */
public final class FilesetDataset implements GeoParquetDataset {

    private final String name;
    private final ParquetSchema fileSchema;
    private final ParquetSource singleFile;
    private final ParquetSchema augmentedSchema;
    private final HivePartitioning partitioning;
    private final List<Map<String, String>> perFilePartitions;
    private final List<ByteRangeSource> sources;
    private final List<String> locations;
    private final List<FileStats> partitionStats;
    private final DatasetCapabilities capabilities;
    private final Optional<GeoParquetMetadata> geoMetadata;
    private final Optional<BoundingBox> aggregatedBounds;
    private final OpenOptions openOptions;

    /**
     * Builds a dataset over the given files. The files behind {@code sources} must agree on {@code fileSchema} by
     * equality; the catalog verifies this when it gathers them, and a dataset built directly must be given files that
     * already agree. {@code singleFile} holds the source already parsed by the gather pass, and is present exactly for
     * a one-file dataset.
     */
    // The construction inputs are cohesive dataset state the catalog resolves in one place, not a long argument
    // list worth bundling into a parameter object.
    @SuppressWarnings("java:S107")
    public FilesetDataset(
            String name,
            ParquetSchema fileSchema,
            Optional<ParquetSource> singleFile,
            PartitionContext partitions,
            List<ByteRangeSource> sources,
            List<String> locations,
            DatasetCapabilities capabilities,
            Optional<GeoParquetMetadata> geoMetadata,
            OpenOptions openOptions) {
        this.name = Objects.requireNonNull(name, "name");
        this.fileSchema = Objects.requireNonNull(fileSchema, "fileSchema");
        Objects.requireNonNull(singleFile, "singleFile");
        Objects.requireNonNull(partitions, "partitions");
        if (singleFile.isPresent() != (sources.size() == 1)) {
            throw new IllegalArgumentException("a parsed source is kept exactly for a one-file dataset, got "
                    + sources.size() + " sources and singleFile " + (singleFile.isPresent() ? "present" : "absent"));
        }
        this.singleFile = singleFile.orElse(null);
        this.augmentedSchema = partitions.augmentedSchema();
        this.partitioning = partitions.partitioning();
        this.perFilePartitions = partitions.perFilePartitions();
        this.partitionStats = partitions.partitionStats();
        this.sources = List.copyOf(sources);
        this.locations = List.copyOf(locations);
        this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
        this.geoMetadata = Objects.requireNonNull(geoMetadata, "geoMetadata");
        this.aggregatedBounds = geoMetadata.flatMap(FilesetDataset::primaryBbox);
        this.openOptions = Objects.requireNonNull(openOptions, "openOptions");
    }

    /**
     * The partition and synthesis inputs the catalog assembles for one dataset: the schema with synthetic partition
     * leaves appended, the bound partitioning, each file's resolved partition values, and each file's partition-derived
     * {@link FileStats} used for pruning. Lists are defensively copied.
     */
    public record PartitionContext(
            ParquetSchema augmentedSchema,
            HivePartitioning partitioning,
            List<Map<String, String>> perFilePartitions,
            List<FileStats> partitionStats) {

        public PartitionContext {
            Objects.requireNonNull(augmentedSchema, "augmentedSchema");
            Objects.requireNonNull(partitioning, "partitioning");
            perFilePartitions = List.copyOf(perFilePartitions);
            partitionStats = List.copyOf(partitionStats);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ParquetSchema schema() {
        return augmentedSchema;
    }

    @Override
    public Optional<CatalogSnapshot> snapshot() {
        return Optional.empty();
    }

    @Override
    public DatasetCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public Optional<GeoParquetMetadata> geoMetadata() {
        return geoMetadata;
    }

    @Override
    public Optional<BoundingBox> bounds(Predicate predicate, ReadOptions options) {
        if (isUnfiltered(predicate) && aggregatedBounds.isPresent()) {
            return aggregatedBounds;
        }
        // No aggregated metadata box exists when the files' geo metadata declares no bbox (or there is no geo
        // metadata at all); the bounds then come from scanning, never from reporting empty over a non-empty dataset.
        return boundsOfSurvivors(pruneSurvivors(predicate), predicate, options);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Answered from the per-file footer statistics gathered at open: the aggregated metadata box for the unfiltered
     * query, else the union of the surviving files' geometry boxes. No file is re-read.
     */
    @Override
    public Optional<BoundingBox> estimatedBounds(Predicate predicate) {
        if (isUnfiltered(predicate) && aggregatedBounds.isPresent()) {
            return aggregatedBounds;
        }
        BoundsAccumulator union = new BoundsAccumulator();
        for (int index : pruneSurvivors(predicate)) {
            Optional<BoundingBox> fileBox = footerStatsBox(index);
            if (fileBox.isEmpty()) {
                return Optional.empty();
            }
            union.union(fileBox.orElseThrow());
        }
        return union.snapshot();
    }

    /**
     * The file's geometry box from the footer statistics gathered at open, resolved to the declared primary geometry
     * column when the {@code "geo"} metadata names one, else the first recorded geometry-bounds entry. The fallback
     * reads the first entry of one immutable map instance, matching the engine's own no-metadata fallback, whose
     * iteration order, though unspecified, is fixed for the run. No file is opened.
     */
    private Optional<BoundingBox> footerStatsBox(int index) {
        Map<ColumnPath, BoundingBox> footerBounds = partitionStats.get(index).geometryBounds();
        Optional<ColumnPath> primary = declaredPrimaryColumn();
        if (primary.isPresent()) {
            return Optional.ofNullable(footerBounds.get(primary.orElseThrow()));
        }
        return footerBounds.values().stream().findFirst();
    }

    /**
     * The exact bounds of the {@code predicate}-matching rows across the survivor files, at most
     * {@code maxConcurrentFiles} files bounded at once, each folded into one shared accumulator. Before a file runs the
     * engine, its footer geometry box is tested against the bounds accumulated so far, and a file those bounds already
     * cover is skipped: a box already enclosed extends the extent by nothing. A file whose footer records no geometry
     * box always runs the engine. A failure in any file cancels the rest.
     */
    private Optional<BoundingBox> boundsOfSurvivors(List<Integer> survivors, Predicate predicate, ReadOptions options) {
        if (survivors.isEmpty()) {
            return Optional.empty();
        }
        BoundsAccumulator accumulator = new BoundsAccumulator();
        SurvivorFanOut.forEach(
                survivors.size(),
                dense -> foldOneFileBounds(survivors.get(dense), predicate, options, accumulator),
                maxConcurrentFiles());
        return accumulator.snapshot();
    }

    /**
     * Folds one survivor file's matching-row bounds into the shared accumulator. The predicate folds against the file's
     * synthetic partition constants exactly as its reads fold it, through the same {@link #oneFileQuery} the reads use;
     * a file whose folded query is always false contributes nothing. A file whose footer geometry box the accumulated
     * bounds already cover is skipped before the engine runs. Runs on a fan-out virtual thread, or on the calling
     * thread when there is a single survivor.
     */
    private void foldOneFileBounds(int index, Predicate predicate, ReadOptions options, BoundsAccumulator accumulator) {
        Query query = oneFileQuery(index, predicate, Projection.ALL);
        if (query == null) {
            return;
        }
        if (accumulatedBoundsCoverFooter(index, accumulator)) {
            return;
        }
        openFile(index).bounds(query, options).ifPresent(accumulator::union);
    }

    /**
     * Whether the accumulated bounds already cover this file's footer geometry box, read from the same footer-derived
     * statistics the gather pass recorded per file - no file is opened. A file whose statistics record no box for the
     * bounded column is never reported as covered: an unknown extent might reach past the accumulated bounds, and
     * skipping it could lose rows.
     */
    private boolean accumulatedBoundsCoverFooter(int index, BoundsAccumulator accumulator) {
        Optional<BoundingBox> footerBox = footerStatsBox(index);
        return footerBox.isPresent() && accumulator.covers(footerBox.orElseThrow());
    }

    /**
     * The fileset's declared primary geometry column, as the engine resolves it, or empty when the metadata names none.
     */
    private Optional<ColumnPath> declaredPrimaryColumn() {
        return geoMetadata
                .map(GeoParquetMetadata::primaryColumn)
                .filter(primary -> !primary.isBlank())
                .map(primary -> ColumnPath.of(primary.split("\\.")));
    }

    @Override
    @MustBeClosed
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        return read(predicate, projection, Materializer.defaultRecord(), options);
    }

    /**
     * Reads the rows of the files surviving {@code predicate}, materialized through {@code materializer}, with any
     * projected synthetic column presented per file as a constant. Without a probe the survivors drain concurrently, up
     * to {@code maxConcurrentFiles} at once, each file opened by the producer that drains it (a single survivor is
     * opened and drained on the calling thread); a spatial-decimation probe pins the files to one thread, visited one
     * at a time in the order of their geometry boxes and each opened only when the previous one is drained. Either way
     * a file's reader lives as long as its own stream.
     */
    @Override
    @MustBeClosed
    public <T> Stream<T> read(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        List<Integer> survivors = pruneSurvivors(predicate);
        if (survivors.isEmpty()) {
            return Stream.empty();
        }
        Optional<SpatialReadProbe> probe = options.spatialReadProbe();
        if (probe.isPresent()) {
            return visitSequentially(
                    survivors,
                    probe.orElseThrow(),
                    index -> readOneFile(index, predicate, projection, materializer, options));
        }
        return ConcurrentSurvivorReads.records(
                survivors.size(),
                dense -> readOneFileBatches(survivors.get(dense), predicate, projection, options),
                materializer,
                maxConcurrentFiles());
    }

    /** The batch analogue of {@link #read(Predicate, Projection, Materializer, ReadOptions)}. */
    @Override
    @MustBeClosed
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        List<Integer> survivors = pruneSurvivors(predicate);
        if (survivors.isEmpty()) {
            return Stream.empty();
        }
        Optional<SpatialReadProbe> probe = options.spatialReadProbe();
        if (probe.isPresent()) {
            return visitSequentially(
                    survivors, probe.orElseThrow(), index -> readOneFileBatches(index, predicate, projection, options));
        }
        return ConcurrentSurvivorReads.batches(
                survivors.size(),
                dense -> readOneFileBatches(survivors.get(dense), predicate, projection, options),
                maxConcurrentFiles());
    }

    /**
     * Visits the survivors one at a time in the order of their footer geometry boxes, opening each file's stream only
     * when the previous one is drained and skipping a file whose whole box {@code probe} reports as already painted.
     * {@code readOneFile} yields one survivor's stream; the sequential concatenation calls it only after the previous
     * file ended, and the gate is consulted right before that call, by which point the probe has seen the previous
     * file's paint.
     */
    @MustBeClosed
    private <S> Stream<S> visitSequentially(
            List<Integer> survivors, SpatialReadProbe probe, IntFunction<Stream<S>> readOneFile) {
        SpatialFileVisit visit = SpatialFileVisit.plan(survivors, this::footerStatsBox, probe);
        List<Integer> order = visit.order();
        return ConcurrentSurvivorReads.sequential(order.size(), dense -> {
            int index = order.get(dense);
            if (visit.skips(index)) {
                return Stream.empty();
            }
            return readOneFile.apply(index);
        });
    }

    @MustBeClosed
    private <T> Stream<T> readOneFile(
            int index, Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        Query query = oneFileQuery(index, predicate, projection);
        if (query == null) {
            return Stream.empty();
        }
        return openFile(index).read(query, materializer, options);
    }

    @MustBeClosed
    private Stream<ParquetRecordBatch> readOneFileBatches(
            int index, Predicate predicate, Projection projection, ReadOptions options) {
        Query query = oneFileQuery(index, predicate, projection);
        if (query == null) {
            return Stream.empty();
        }
        return openFile(index).readBatches(query, options);
    }

    /**
     * {@inheritDoc}
     *
     * <p>A file whose folded predicate is always true contributes the record count the gather pass recorded for it - no
     * file is opened. The files with a real residual predicate are counted at most {@code maxConcurrentFiles} at a
     * time, each opened by its own task; a single file to count is opened on the calling thread.
     */
    @Override
    public long count(Predicate predicate, ReadOptions options) {
        long recorded = 0L;
        List<FileToCount> needRead = new ArrayList<>();
        for (int index : pruneSurvivors(predicate)) {
            Predicate residual = residualFor(index, predicate);
            if (residual.equals(Predicate.ALWAYS_TRUE)) {
                recorded += partitionStats.get(index).recordCount();
            } else if (!residual.equals(Predicate.ALWAYS_FALSE)) {
                needRead.add(new FileToCount(index, residual));
            }
        }
        if (needRead.isEmpty()) {
            return recorded;
        }
        long counted = SurvivorFanOut.sum(
                needRead.size(), dense -> countOneFile(needRead.get(dense), options), maxConcurrentFiles());
        return recorded + counted;
    }

    /** A survivor whose count needs a read, with the predicate left over after folding that file's constants. */
    private record FileToCount(int index, Predicate residual) {}

    private long countOneFile(FileToCount file, ReadOptions options) {
        return openFile(file.index()).count(file.residual(), options);
    }

    private int maxConcurrentFiles() {
        return openOptions.runtime().maxConcurrentFiles();
    }

    /**
     * The one-file {@link Query} for the file at {@code index}: the predicate folded against this file's synthetic
     * constants, the projection split to its physical columns, and the projected synthetic columns presented as the
     * output shape's constant columns. Returns null when the folded predicate is always false (the file is skipped).
     *
     * <p>With no projected synthetic columns this is the identity shape (the all-files fast path). Otherwise the output
     * is the file's physical columns in decode order followed by the projected constants, the same order the appended
     * physical-then-constant batch presented.
     */
    private Query oneFileQuery(int index, Predicate predicate, Projection projection) {
        Predicate residual = residualFor(index, predicate);
        if (residual.equals(Predicate.ALWAYS_FALSE)) {
            return null;
        }
        Map<String, String> filePartitions = perFilePartitions.get(index);
        List<ConstantColumn> constants = projectedConstants(projection, filePartitions);
        if (constants.isEmpty()) {
            return Query.of(residual, physicalProjection(projection));
        }
        return Query.of(residual, Projection.of(produceSet(projection, constants)));
    }

    /**
     * The produce set for a file with projected synthetic columns: a {@link Projection.Column.Physical} passthrough for
     * each physical column the read decodes, in the file's depth-first order, followed by a
     * {@link Projection.Column.Constant} for each projected partition column.
     */
    private SequencedSet<Projection.Column> produceSet(Projection projection, List<ConstantColumn> constants) {
        SequencedSet<Projection.Column> columns = new LinkedHashSet<>();
        for (ColumnPath leaf : presentedPhysicalColumns(projection)) {
            columns.add(new Projection.Column.Physical(leaf, leaf));
        }
        for (ConstantColumn constant : constants) {
            columns.add(new Projection.Column.Constant(constant.path(), constant.value()));
        }
        return columns;
    }

    /** The physical leaf columns the decoded batch presents for {@code projection}, in the file's depth-first order. */
    private List<ColumnPath> presentedPhysicalColumns(Projection projection) {
        List<ColumnPath> fileLeaves = fileSchema.leafColumns();
        Optional<Set<ColumnPath>> names = projectedNames(projection);
        if (names.isEmpty()) {
            return fileLeaves;
        }
        Set<ColumnPath> kept = names.get();
        List<ColumnPath> presented = new ArrayList<>();
        for (ColumnPath leaf : fileLeaves) {
            if (kept.contains(leaf)) {
                presented.add(leaf);
            }
        }
        return presented;
    }

    /** The projected column names, or empty for {@link Projection#ALL} (which presents every column). */
    private static Optional<Set<ColumnPath>> projectedNames(Projection projection) {
        return switch (projection) {
            case Projection.All _ -> Optional.empty();
            case Projection.Of(SequencedSet<Projection.Column> columns) -> {
                Set<ColumnPath> names = new LinkedHashSet<>();
                for (Projection.Column column : columns) {
                    names.add(column.name());
                }
                yield Optional.of(names);
            }
        };
    }

    private Predicate residualFor(int index, Predicate predicate) {
        Map<String, String> filePartitions = perFilePartitions.get(index);
        Map<ColumnPath, Value> constants = partitioning.constantMap(filePartitions);
        return ConstantFolding.fold(predicate, constants, partitioning.nullColumns(filePartitions));
    }

    /**
     * The projection of what a file physically holds: {@link Projection#ALL} unchanged, else the requested columns with
     * this dataset's synthetic (path-only) ones removed.
     */
    private Projection physicalProjection(Projection projection) {
        return switch (projection) {
            case Projection.All _ -> Projection.ALL;
            case Projection.Of(SequencedSet<Projection.Column> columns) -> withoutSyntheticColumns(projection, columns);
        };
    }

    /**
     * {@code projection} minus the columns naming a synthetic partition value, returned unchanged when it names none. A
     * projection of nothing but synthetic columns leaves an empty physical set; the batch reader derives the row count
     * from the decoded columns and would emit no rows. Project one cheap physical leaf instead, which enumerates every
     * file row for the synthetic constants to be appended to.
     */
    private Projection withoutSyntheticColumns(Projection projection, SequencedSet<Projection.Column> columns) {
        Set<ColumnPath> synthetic = partitioning.syntheticPaths();
        SequencedSet<Projection.Column> physical = new LinkedHashSet<>();
        for (Projection.Column column : columns) {
            if (!synthetic.contains(column.name())) {
                physical.add(column);
            }
        }
        if (physical.isEmpty()) {
            return rowEnumerationProjection();
        }
        if (physical.size() == columns.size()) {
            return projection;
        }
        return Projection.of(physical);
    }

    /** A projection of a single physical leaf, used to drive row enumeration when only synthetic columns are read. */
    private Projection rowEnumerationProjection() {
        return Projection.ofPhysical(List.of(fileSchema.leafColumns().get(0)));
    }

    /** The projected synthetic columns of this file as constant output columns. */
    private List<ConstantColumn> projectedConstants(Projection projection, Map<String, String> filePartitions) {
        List<ConstantColumn> all = partitioning.constantsFor(filePartitions);
        Optional<Set<ColumnPath>> names = projectedNames(projection);
        if (names.isEmpty()) {
            return all;
        }
        Set<ColumnPath> kept = names.get();
        List<ConstantColumn> result = new ArrayList<>();
        for (ConstantColumn constant : all) {
            if (kept.contains(constant.path())) {
                result.add(constant);
            }
        }
        return result;
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
        List<FileExplain> files = new ArrayList<>(partitionStats.size());
        for (int index = 0; index < partitionStats.size(); index++) {
            PruningDecision decision = FilePruner.evaluate(predicate, partitionStats.get(index));
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
        String location = locations.get(index);
        OptionalLong recordCount = OptionalLong.of(partitionStats.get(index).recordCount());
        if (decision instanceof PruningDecision.Eliminated ruledOut) {
            return new FileExplain(location, Outcome.SKIP, ruledOut.reason(), recordCount, Optional.empty());
        }
        Predicate residual = residualFor(index, predicate);
        if (residual.equals(Predicate.ALWAYS_FALSE)) {
            return new FileExplain(location, Outcome.SKIP, "partition value excluded", recordCount, Optional.empty());
        }
        Projection physical = physicalProjection(projection);
        ParquetSource survivor = openFile(index);
        ExplainPlan plan = analyze
                ? survivor.explainAnalyze(residual, physical, options)
                : survivor.explain(residual, physical, options);
        return new FileExplain(location, Outcome.KEEP, "kept", recordCount, Optional.of(plan));
    }

    /**
     * A {@link ParquetSource} over the one file at {@code index}: the retained parsed source for a one-file dataset,
     * else a fresh open over the catalog's shared byte source, whose footer comes from the shared footer-metadata cache
     * (a hit rebuilds nothing; a miss reads and parses the footer and the cache retains it under its own bound). The
     * source lives as long as the stream or task that it serves.
     */
    private ParquetSource openFile(int index) {
        if (singleFile != null) {
            return singleFile;
        }
        return ParquetSource.open(sources.get(index), openOptions);
    }

    private List<Integer> pruneSurvivors(Predicate predicate) {
        List<Integer> survivors = new ArrayList<>();
        for (int index = 0; index < partitionStats.size(); index++) {
            PruningDecision decision = FilePruner.evaluate(predicate, partitionStats.get(index));
            if (!(decision instanceof PruningDecision.Eliminated)) {
                survivors.add(index);
            }
        }
        return survivors;
    }

    private static boolean isUnfiltered(Predicate predicate) {
        return predicate instanceof Predicate.Always(boolean value) && value;
    }

    private static Optional<BoundingBox> primaryBbox(GeoParquetMetadata geo) {
        GeoColumn primary = geo.columns().get(geo.primaryColumn());
        return primary == null ? Optional.empty() : primary.bbox();
    }
}
