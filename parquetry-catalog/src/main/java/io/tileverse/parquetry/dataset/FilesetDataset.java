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

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.SequencedSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.StructuredTaskScope;
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
 * from path values as exact statistics) and reads only the survivors. When nothing prunes it reuses one pre-opened
 * {@link ParquetSource} over every file, which keeps the common unpartitioned case reading each footer once.
 */
public final class FilesetDataset implements GeoParquetDataset {

    private final String name;
    private final ParquetSource allFiles;
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
    private final Map<Integer, ParquetSource> perFileDatasets = new ConcurrentHashMap<>();

    // The eight construction inputs are cohesive dataset state the catalog resolves in one place, not a long argument
    // list worth bundling into a parameter object.
    @SuppressWarnings("java:S107")
    public FilesetDataset(
            String name,
            ParquetSource allFiles,
            PartitionContext partitions,
            List<ByteRangeSource> sources,
            List<String> locations,
            DatasetCapabilities capabilities,
            Optional<GeoParquetMetadata> geoMetadata,
            OpenOptions openOptions) {
        this.name = Objects.requireNonNull(name, "name");
        this.allFiles = Objects.requireNonNull(allFiles, "allFiles");
        Objects.requireNonNull(partitions, "partitions");
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
        if (isUnfiltered(predicate)) {
            return aggregatedBounds;
        }
        return boundsOfSurvivors(pruneSurvivors(predicate), predicate, options);
    }

    /**
     * The exact bounds of the {@code predicate}-matching rows across the survivor files, each file visited on its own
     * virtual thread and folded into one shared accumulator. Before a file runs the engine, its footer geometry box is
     * tested against the bounds accumulated so far, and a file those bounds already cover is skipped: a box already
     * enclosed extends the extent by nothing. A file whose footer records no geometry box always runs the engine. A
     * failure in any file cancels the rest.
     */
    private Optional<BoundingBox> boundsOfSurvivors(List<Integer> survivors, Predicate predicate, ReadOptions options) {
        if (survivors.isEmpty()) {
            return Optional.empty();
        }
        BoundsAccumulator accumulator = new BoundsAccumulator();
        try (StructuredTaskScope<Void, Void> scope = StructuredTaskScope.open()) {
            for (int index : survivors) {
                scope.fork(() -> foldOneFileBounds(index, predicate, options, accumulator));
            }
            scope.join();
            return accumulator.snapshot();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException("Interrupted while bounding dataset rows");
            interrupted.initCause(e);
            throw new UncheckedIOException(interrupted);
        } catch (StructuredTaskScope.FailedException e) {
            throw asUnchecked(e.getCause());
        }
    }

    /**
     * Folds one survivor file's matching-row bounds into the shared accumulator. The predicate folds against the file's
     * synthetic partition constants exactly as its reads fold it, through the same {@link #oneFileQuery} the reads use;
     * a file whose folded query is always false contributes nothing. A file whose footer geometry box the accumulated
     * bounds already cover is skipped before the engine runs. Runs on a fan-out virtual thread.
     */
    private Void foldOneFileBounds(int index, Predicate predicate, ReadOptions options, BoundsAccumulator accumulator) {
        Query query = oneFileQuery(index, predicate, Projection.ALL);
        if (query == null) {
            return null;
        }
        if (accumulatedBoundsCoverFooter(index, accumulator)) {
            return null;
        }
        perFile(index).bounds(query, options).ifPresent(accumulator::union);
        return null;
    }

    /**
     * Whether the accumulated bounds already cover this file's footer geometry box. Partition statistics record no
     * geometry box. The box is read from the opened file's footer, resolved to the same geometry column the engine
     * bounds. A file whose footer records no box for that column is never reported as covered: an unknown extent might
     * reach past the accumulated bounds, and skipping it could lose rows.
     */
    private boolean accumulatedBoundsCoverFooter(int index, BoundsAccumulator accumulator) {
        Optional<BoundingBox> footerBox = footerSkipBox(index);
        return footerBox.isPresent() && accumulator.covers(footerBox.orElseThrow());
    }

    /**
     * The footer geometry box the containment skip tests, resolved to the column the engine bounds: the declared
     * primary geometry column when the fileset's {@code "geo"} metadata names one, otherwise the first entry of the
     * per-file {@link FileStats#geometryBounds()}. A declared primary the file records no box for yields no box,
     * keeping the file in the scan. The fallback reads the first entry of the one map instance this method obtains;
     * that matches the engine's own no-metadata fallback, which reads the first key of an equivalent immutable
     * geometry-bounds map whose iteration order, though unspecified, is fixed for the run.
     */
    private Optional<BoundingBox> footerSkipBox(int index) {
        Map<ColumnPath, BoundingBox> footerBounds = perFile(index).fileStats().geometryBounds();
        Optional<ColumnPath> primary = declaredPrimaryColumn();
        if (primary.isPresent()) {
            return Optional.ofNullable(footerBounds.get(primary.orElseThrow()));
        }
        return footerBounds.values().stream().findFirst();
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

    /**
     * Rethrows a per-file bounds failure with the type a sequential visit would have thrown. The engine's per-file
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
        return new IllegalStateException("Bounding a dataset file failed", cause);
    }

    @Override
    @MustBeClosed
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        if (!referencesSynthetic(predicate, projection)) {
            return readAllFiles(predicate, projection, options);
        }
        return readSurvivors(predicate, projection, options);
    }

    /**
     * Reads batches shaped by {@code predicate} and {@code projection}, appending any projected synthetic columns as
     * constant columns per file. Mirrors {@link #read(Predicate, Projection, ReadOptions)} at the batch level for
     * batch-oriented consumers (such as the Arrow bridge).
     */
    @Override
    @MustBeClosed
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        if (!referencesSynthetic(predicate, projection)) {
            return readAllFileBatches(predicate, projection, options);
        }
        return readSurvivorBatches(predicate, projection, options);
    }

    /**
     * The materializer read path does not yet synthesize path-only partition columns; a query referencing a synthetic
     * column throws {@link UnsupportedOperationException}. Use {@link #read(Predicate, Projection, ReadOptions)} for
     * synthesized columns.
     */
    @Override
    @MustBeClosed
    public <T> Stream<T> read(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        if (referencesSynthetic(predicate, projection)) {
            throw new UnsupportedOperationException(
                    "the materializer read path does not yet support synthesized partition columns; use read(predicate, projection, options)");
        }
        ParquetSource query = surviving(predicate);
        if (query == null) {
            return Stream.empty();
        }
        return query.read(predicate, projection, materializer, options);
    }

    @Override
    public long count(Predicate predicate, ReadOptions options) {
        if (!referencesSynthetic(predicate, Projection.ALL)) {
            return countAllFiles(predicate, options);
        }
        long total = 0L;
        for (int index : pruneSurvivors(predicate)) {
            Predicate residual = residualFor(index, predicate);
            if (residual.equals(Predicate.ALWAYS_FALSE)) {
                continue;
            }
            total += perFile(index).count(residual, options);
        }
        return total;
    }

    @MustBeClosed
    private Stream<ParquetRecord> readAllFiles(Predicate predicate, Projection projection, ReadOptions options) {
        ParquetSource query = surviving(predicate);
        if (query == null) {
            return Stream.empty();
        }
        return query.read(predicate, projection, options);
    }

    @MustBeClosed
    private Stream<ParquetRecordBatch> readAllFileBatches(
            Predicate predicate, Projection projection, ReadOptions options) {
        ParquetSource query = surviving(predicate);
        if (query == null) {
            return Stream.empty();
        }
        return query.readBatches(Query.of(predicate, projection), options);
    }

    private long countAllFiles(Predicate predicate, ReadOptions options) {
        ParquetSource query = surviving(predicate);
        if (query == null) {
            return 0L;
        }
        return query.count(predicate, options);
    }

    /**
     * Reads the synthesized rows of the files surviving {@code predicate}, draining the survivors concurrently. A
     * spatial-decimation probe pins the per-file visit order onto a single thread, which the fan-out would break; a
     * probe read therefore keeps the sequential per-file composition. Otherwise each survivor's records ride its
     * columnar batches across the fan-out and flatten to rows on the consuming thread. Each one-file stream's resources
     * close with the composed stream.
     */
    @MustBeClosed
    private Stream<ParquetRecord> readSurvivors(Predicate predicate, Projection projection, ReadOptions options) {
        List<Integer> survivors = pruneSurvivors(predicate);
        if (survivors.isEmpty()) {
            return Stream.empty();
        }
        if (options.spatialReadProbe().isPresent()) {
            return ConcurrentSurvivorReads.sequential(
                    survivors.size(), dense -> readOneFile(survivors.get(dense), predicate, projection, options));
        }
        return ConcurrentSurvivorReads.records(
                survivors.size(),
                dense -> readOneFileBatches(survivors.get(dense), predicate, projection, options),
                maxConcurrentFiles());
    }

    /**
     * The batch analogue of {@link #readSurvivors}: the survivors' augmented batches, fanned out or kept sequential.
     */
    @MustBeClosed
    private Stream<ParquetRecordBatch> readSurvivorBatches(
            Predicate predicate, Projection projection, ReadOptions options) {
        List<Integer> survivors = pruneSurvivors(predicate);
        if (survivors.isEmpty()) {
            return Stream.empty();
        }
        if (options.spatialReadProbe().isPresent()) {
            return survivors.stream().flatMap(index -> readOneFileBatches(index, predicate, projection, options));
        }
        return ConcurrentSurvivorReads.batches(
                survivors.size(),
                dense -> readOneFileBatches(survivors.get(dense), predicate, projection, options),
                maxConcurrentFiles());
    }

    private int maxConcurrentFiles() {
        return openOptions.runtime().maxConcurrentFiles();
    }

    @MustBeClosed
    private Stream<ParquetRecord> readOneFile(
            int index, Predicate predicate, Projection projection, ReadOptions options) {
        Query query = oneFileQuery(index, predicate, projection);
        if (query == null) {
            return Stream.empty();
        }
        return perFile(index).read(query, options);
    }

    @MustBeClosed
    private Stream<ParquetRecordBatch> readOneFileBatches(
            int index, Predicate predicate, Projection projection, ReadOptions options) {
        Query query = oneFileQuery(index, predicate, projection);
        if (query == null) {
            return Stream.empty();
        }
        return perFile(index).readBatches(query, options);
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
        List<ColumnPath> fileLeaves = allFiles.schema().leafColumns();
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
     * The physical column projection: {@link Projection#ALL} unchanged, else the named columns minus synthetic ones.
     * When every requested column is synthetic the physical set is empty; the batch reader derives the row count from
     * decoded columns and would emit no rows. Project one cheap physical leaf instead so each file row is enumerated
     * and the synthetic constants are appended to it.
     */
    private Projection physicalProjection(Projection projection) {
        if (projectedNames(projection).isEmpty()) {
            return Projection.ALL;
        }
        List<ColumnPath> physical = presentedPhysicalColumns(projection);
        if (physical.isEmpty()) {
            return rowEnumerationProjection();
        }
        return Projection.ofPhysical(physical);
    }

    /** A projection of a single physical leaf, used to drive row enumeration when only synthetic columns are read. */
    private Projection rowEnumerationProjection() {
        return Projection.ofPhysical(List.of(allFiles.schema().leafColumns().get(0)));
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

    /**
     * Whether {@code predicate} or {@code projection} references a synthetic (path-only) column. Only then does a read
     * fold per file and append constants; otherwise the all-files fast path stays.
     */
    private boolean referencesSynthetic(Predicate predicate, Projection projection) {
        if (!partitioning.hasSynthetic()) {
            return false;
        }
        return projectionNamesSynthetic(projection) || predicateNamesSynthetic(predicate);
    }

    private boolean projectionNamesSynthetic(Projection projection) {
        Optional<Set<ColumnPath>> names = projectedNames(projection);
        return names.isEmpty() || intersectsSynthetic(names.get());
    }

    private boolean predicateNamesSynthetic(Predicate predicate) {
        return intersectsSynthetic(Predicate.columns(predicate));
    }

    private boolean intersectsSynthetic(Set<ColumnPath> columns) {
        for (ColumnPath synthetic : partitioning.syntheticPaths()) {
            if (columns.contains(synthetic)) {
                return true;
            }
        }
        return false;
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
        ParquetSource survivor = perFile(index);
        ExplainPlan plan = analyze
                ? survivor.explainAnalyze(residual, physical, options)
                : survivor.explain(residual, physical, options);
        return new FileExplain(location, Outcome.KEEP, "kept", recordCount, Optional.of(plan));
    }

    /**
     * The dataset over the files surviving {@code predicate}: {@code allFiles} when nothing prunes, null when all
     * prune.
     */
    private ParquetSource surviving(Predicate predicate) {
        List<Integer> survivors = pruneSurvivors(predicate);
        if (survivors.isEmpty()) {
            return null;
        }
        if (survivors.size() == sources.size()) {
            // Nothing pruned: pruneSurvivors emits [0..n) ascending, identical to allFiles.
            return allFiles;
        }
        return ParquetSource.open(new SurvivorFileset(sources, survivors), openOptions);
    }

    /**
     * The single-file {@link ParquetSource} over the source at {@code index}, parsed once and reused across queries and
     * threads. {@code computeIfAbsent} gives one footer parse per index even under concurrent reads; the dataset
     * borrows the catalog's shared source, which the catalog owns and closes.
     */
    private ParquetSource perFile(int index) {
        return perFileDatasets.computeIfAbsent(
                index, i -> ParquetSource.open(new SurvivorFileset(sources, List.of(i)), openOptions));
    }

    /** Test hook: the memoized single-file dataset for {@code index}, proving footer reuse across queries. */
    ParquetSource perFileDatasetForTest(int index) {
        return perFile(index);
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

    /** A {@link FilesetReader} over a dense slice of the catalog's pre-opened sources. The sources are borrowed. */
    private record SurvivorFileset(List<ByteRangeSource> sources, List<Integer> survivorIndices)
            implements FilesetReader {
        @Override
        public ByteRangeSource openFile(int index) {
            return sources.get(survivorIndices.get(index));
        }

        @Override
        public int fileCount() {
            return survivorIndices.size();
        }
    }
}
