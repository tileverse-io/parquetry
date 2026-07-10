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
package io.tileverse.parquetry.iceberg;

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import io.tileverse.parquetry.dataset.CatalogSnapshot;
import io.tileverse.parquetry.dataset.ConcurrentSurvivorReads;
import io.tileverse.parquetry.dataset.DatasetCapabilities;
import io.tileverse.parquetry.dataset.DatasetCapabilities.FileStatsSource;
import io.tileverse.parquetry.dataset.FilesetReader;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.dataset.explain.FileExplain;
import io.tileverse.parquetry.dataset.explain.Outcome;
import io.tileverse.parquetry.dataset.explain.Totals;
import io.tileverse.parquetry.filter.ConstantFolding;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.filter.RowPositionSet;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.prune.FilePruner;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.iceberg.IcebergReconciliation.Reconciliation;
import io.tileverse.parquetry.internal.filter.spatial.BoundsAccumulator;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * A {@link ParquetDataset} over one Iceberg table at a pinned snapshot. Before each query the dataset prunes the
 * snapshot's data files by their manifest bounds: a file whose statistics prove no row can match the predicate is
 * skipped, and the query opens a {@link ParquetSource} over only the survivors. Pruning is pure work-avoidance;
 * survivors are still filtered at row-group and record level during the read. The result is identical to scanning every
 * file.
 *
 * <p>The byte sources are pre-opened and owned by the {@link IcebergTableCatalog}; the per-query {@link ParquetSource}
 * borrows the survivor subset and never closes them. The dataset presents the table's current schema and reconciles
 * each data file's columns to it by Iceberg field id; a file whose schema already matches the table reads untouched.
 */
final class IcebergDataset implements ParquetDataset {

    /** The synthesized row-position column the reader produces; the delete predicate filters on it. */
    private static final ColumnPath ROW_POSITION = ColumnPath.of("_pos");

    /**
     * The Iceberg row-lineage columns a caller may project by name; both are read-only and absent from the schema. Row
     * lineage (and the reservation of these names) exists from format version 3; below that a column with one of these
     * names is ordinary user data and is never rewritten.
     */
    private static final ColumnPath ROW_ID = ColumnPath.of("_row_id");

    private static final ColumnPath LAST_UPDATED_SEQUENCE_NUMBER = ColumnPath.of("_last_updated_sequence_number");

    // The reserved metadata field ids Iceberg assigns the two lineage columns (org.apache.iceberg.MetadataColumns).
    private static final int ROW_ID_FIELD_ID = Integer.MAX_VALUE - 107;

    private static final int LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID = Integer.MAX_VALUE - 108;

    private static final int FIRST_FORMAT_VERSION_WITH_ROW_LINEAGE = 3;

    private final String name;
    private final CatalogSnapshot snapshot;
    private final IcebergSchema icebergSchema;
    private final IcebergPartitionSpec partitionSpec;
    private final List<IcebergManifests.DataFileRef> dataFiles;
    private final List<FileStats> fileStats;
    private final List<ByteRangeSource> sources;
    private final IcebergDeletePlan deletePlan;
    private final int formatVersion;
    private final IcebergNameMapping nameMapping;
    private final IcebergNestedSchema nestedSchema;
    private final OpenOptions openOptions;
    private final Map<Integer, ParquetSource> perFileDatasets = new ConcurrentHashMap<>();
    private final Set<Integer> nestedValidated = ConcurrentHashMap.newKeySet();

    // The catalog hands the dataset its fully resolved snapshot state in one place; these are cohesive fields, not a
    // long argument list worth bundling into a parameter object.
    @SuppressWarnings("java:S107")
    IcebergDataset(
            String name,
            CatalogSnapshot snapshot,
            IcebergSchema icebergSchema,
            IcebergPartitionSpec partitionSpec,
            List<IcebergManifests.DataFileRef> dataFiles,
            List<FileStats> fileStats,
            List<ByteRangeSource> sources,
            IcebergDeletePlan deletePlan,
            int formatVersion,
            IcebergNameMapping nameMapping,
            IcebergNestedSchema nestedSchema,
            OpenOptions openOptions) {
        this.name = Objects.requireNonNull(name, "name");
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        this.icebergSchema = Objects.requireNonNull(icebergSchema, "icebergSchema");
        this.partitionSpec = Objects.requireNonNull(partitionSpec, "partitionSpec");
        this.dataFiles = List.copyOf(dataFiles);
        this.fileStats = List.copyOf(fileStats);
        this.sources = List.copyOf(sources);
        this.deletePlan = Objects.requireNonNull(deletePlan, "deletePlan");
        this.formatVersion = formatVersion;
        this.nameMapping = Objects.requireNonNull(nameMapping, "nameMapping");
        this.nestedSchema = Objects.requireNonNull(nestedSchema, "nestedSchema");
        this.openOptions = Objects.requireNonNull(openOptions, "openOptions");
    }

    /** Whether this table's format version has row lineage; the reserved names have no meaning below it. */
    private boolean rowLineage() {
        return formatVersion >= FIRST_FORMAT_VERSION_WITH_ROW_LINEAGE;
    }

    /** A data file the dataset eliminated for a predicate, with the pruning decision that ruled it out. */
    public record EliminatedFile(String location, PruningDecision.Eliminated decision) {}

    /** The pruning outcome for a predicate: the data files kept versus the ones eliminated. */
    public record FilePlan(List<String> survivors, List<EliminatedFile> eliminated) {
        public FilePlan {
            survivors = List.copyOf(survivors);
            eliminated = List.copyOf(eliminated);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ParquetSchema schema() {
        return icebergSchema.parquetSchema();
    }

    @Override
    public Optional<CatalogSnapshot> snapshot() {
        return Optional.of(snapshot);
    }

    @Override
    public DatasetCapabilities capabilities() {
        return DatasetCapabilities.builder()
                .fileStats(FileStatsSource.MANIFEST)
                .cheapCount(false)
                .build();
    }

    /**
     * Evaluates the manifest bounds of every data file against {@code predicate} and reports which files survive and
     * which are eliminated. Survivors keep their file order. A file is a survivor unless the pruner proves no row in it
     * can match.
     */
    public FilePlan plan(Predicate predicate) {
        PruningResult pruning = prune(predicate);
        List<String> survivors = new ArrayList<>(pruning.survivorIndices().size());
        for (int index : pruning.survivorIndices()) {
            survivors.add(dataFiles.get(index).location());
        }
        return new FilePlan(survivors, pruning.eliminated());
    }

    @Override
    @MustBeClosed
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        return readSurvivors(predicate, projection, Materializer.defaultRecord(), options);
    }

    @Override
    @MustBeClosed
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        List<Integer> survivors = prune(predicate).survivorIndices();
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

    @MustBeClosed
    private Stream<ParquetRecordBatch> readOneFileBatches(
            int index, Predicate predicate, Projection projection, ReadOptions options) {
        Query query = oneFileQuery(index, predicate, projection);
        if (query == null) {
            return Stream.empty();
        }
        return perFile(index).readBatches(query, options);
    }

    @Override
    @MustBeClosed
    public <T> Stream<T> read(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        return readSurvivors(predicate, projection, materializer, options);
    }

    /**
     * Reads the survivor data files' reconciled rows, draining them concurrently. A spatial-decimation probe pins the
     * per-file visit order onto a single thread, which the fan-out would break; a probe read therefore keeps the
     * sequential per-file composition. Otherwise each survivor's rows ride its columnar batches across the fan-out and
     * flatten to rows on the consuming thread. Flattening reconciles a pass-through and an evolved file uniformly: the
     * materializer builds each row against that batch's own reconciled schema.
     */
    @MustBeClosed
    private <T> Stream<T> readSurvivors(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        List<Integer> survivors = prune(predicate).survivorIndices();
        if (survivors.isEmpty()) {
            return Stream.empty();
        }
        if (options.spatialReadProbe().isPresent()) {
            return ConcurrentSurvivorReads.sequential(
                    survivors.size(),
                    dense -> readOneFile(survivors.get(dense), predicate, projection, materializer, options));
        }
        return ConcurrentSurvivorReads.records(
                survivors.size(),
                dense -> readOneFileBatches(survivors.get(dense), predicate, projection, options),
                materializer,
                maxConcurrentFiles());
    }

    /**
     * The materializer read of one survivor. A pass-through file reads natively, applying the materializer in the
     * engine. A reconciled file reads reconciled (table-named) records through the {@link Query} path and applies the
     * materializer per record against that record's own reconciled schema, which describes the columns the record
     * holds.
     */
    @MustBeClosed
    private <T> Stream<T> readOneFile(
            int index, Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        Query query = oneFileQuery(index, predicate, projection);
        if (query == null) {
            return Stream.empty();
        }
        if (isReconciled(query)) {
            return perFile(index).read(query, options).map(row -> materializer.materialize(row.schema(), row));
        }
        return perFile(index).read(query.predicate(), query.projection(), materializer, options);
    }

    /** A reconciled read presents the table's produce set (renames, null-fills, constants); a pass-through does not. */
    private static boolean isReconciled(Query query) {
        return query.projection() instanceof Projection.Of of && of.needsShaping();
    }

    @Override
    public long count(Predicate predicate, ReadOptions options) {
        List<Integer> survivors = prune(predicate).survivorIndices();
        if (survivors.isEmpty()) {
            return 0L;
        }
        long total = 0L;
        for (int index : survivors) {
            Query query = oneFileQuery(index, predicate, Projection.ALL);
            if (query != null) {
                total += perFile(index).count(query, options);
            }
        }
        return total;
    }

    /**
     * The exact bounds of the {@code predicate}-matching rows over the primary geometry column. No cheap geometry
     * aggregate exists: the manifest records only conservative per-file boxes. Every call, unfiltered or not, visits
     * the survivor files. Each file is bounded on its own virtual thread and folded into one shared accumulator.
     */
    @Override
    public Optional<BoundingBox> bounds(Predicate predicate, ReadOptions options) {
        return boundsOfSurvivors(prune(predicate).survivorIndices(), predicate, options);
    }

    /**
     * Folds each survivor's exact per-file bounds into one shared accumulator across a fan-out. Before a file runs the
     * engine, its footer geometry box is tested against the bounds accumulated so far, and a file those bounds already
     * cover is skipped: a box already enclosed extends the extent by nothing. A file whose footer records no geometry
     * box always runs the engine. A failure in any file cancels the rest.
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
     * Folds one survivor data file's matching-row bounds into the shared accumulator. The predicate folds through the
     * same {@link #oneFileQuery} the reads use: field-id reconciliation, partition constants, and delete predicates
     * ANDed in. A merge-on-read deleted row therefore never contributes, and a file whose folded query is always false
     * contributes nothing. A file whose footer geometry box the accumulated bounds already cover is skipped before the
     * engine runs. Runs on a fan-out virtual thread.
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
     * Whether the accumulated bounds already cover this file's footer geometry box, read from the opened data file. The
     * skip fires only when {@link FileStats#geometryBounds()} records exactly one entry: with several geometry columns
     * the declared primary the engine bounds may differ from an arbitrary map entry, and a skip keyed to the wrong
     * column could under-report; with a single entry the engine's fallback resolution lands on that same entry whenever
     * the declared primary has statistics at all. A file with no footer geometry box is never reported as covered
     * either: an unknown extent might reach past the accumulated bounds.
     */
    private boolean accumulatedBoundsCoverFooter(int index, BoundsAccumulator accumulator) {
        Map<ColumnPath, BoundingBox> footerBounds = perFile(index).fileStats().geometryBounds();
        if (footerBounds.size() != 1) {
            return false;
        }
        BoundingBox footerBox = footerBounds.values().iterator().next();
        return accumulator.covers(footerBox);
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
    public DatasetExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options) {
        return buildExplain(predicate, projection, options, false);
    }

    @Override
    public DatasetExplainPlan explainAnalyze(Predicate predicate, Projection projection, ReadOptions options) {
        return buildExplain(predicate, projection, options, true);
    }

    private DatasetExplainPlan buildExplain(
            Predicate predicate, Projection projection, ReadOptions options, boolean analyze) {
        List<FileExplain> files = new ArrayList<>(dataFiles.size());
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
        IcebergManifests.DataFileRef ref = dataFiles.get(index);
        OptionalLong recordCount = OptionalLong.of(ref.recordCount());
        if (decision instanceof PruningDecision.Eliminated ruledOut) {
            return new FileExplain(ref.location(), Outcome.SKIP, ruledOut.reason(), recordCount, Optional.empty());
        }
        Query query = oneFileQuery(index, predicate, projection);
        if (query == null) {
            return new FileExplain(
                    ref.location(), Outcome.SKIP, "added-column null fold", recordCount, Optional.empty());
        }
        ParquetSource survivor = perFile(index);
        ExplainPlan plan = analyze ? survivor.explainAnalyze(query, options) : survivor.explain(query, options);
        return new FileExplain(ref.location(), Outcome.KEEP, "kept", recordCount, Optional.of(plan));
    }

    /** The survivor indices and eliminated files from a single pruning pass over every data file. */
    private record PruningResult(List<Integer> survivorIndices, List<EliminatedFile> eliminated) {}

    /** Runs {@link FilePruner#evaluate} once per file, partitioning the files into survivors and eliminated. */
    private PruningResult prune(Predicate predicate) {
        Objects.requireNonNull(predicate, "predicate");
        List<Integer> survivorIndices = new ArrayList<>();
        List<EliminatedFile> eliminated = new ArrayList<>();
        for (int index = 0; index < fileStats.size(); index++) {
            PruningDecision decision = FilePruner.evaluate(predicate, fileStats.get(index));
            if (decision instanceof PruningDecision.Eliminated ruledOut) {
                eliminated.add(new EliminatedFile(dataFiles.get(index).location(), ruledOut));
            } else {
                survivorIndices.add(index);
            }
        }
        return new PruningResult(survivorIndices, eliminated);
    }

    /**
     * The one-file {@link Query} for the data file at {@code index}, reconciled against the table's current schema by
     * field id.
     *
     * <p>A file that matches the table by name (including an id-less file whose mapped columns match) is read untouched
     * by name through the identity {@link Query#of}, which preserves nested and lineage columns the table schema does
     * not model and keeps the corpus fast path free of any output shaping.
     *
     * <p>An evolved file is read through a reconciled output shape. The predicate (already ANDed with any equality
     * anti-predicate) is folded against the columns the table adds (which are null in this file) and against the
     * identity-partition columns this file omits (each a constant taken from the file's manifest partition tuple): a
     * leaf over an added column collapses, a leaf over an omitted partition column evaluates against its constant, and
     * a predicate that folds to always-false skips the file (this method returns null). Folding the anti-predicate
     * before the read keeps an equality field that reconciles to a constant or a null from reaching the file, which
     * does not hold that column. The pushdown projection is the physical sources the output actually reads; when the
     * output is only injected nulls or constants, one cheap physical leaf still drives row enumeration. Evolved files
     * present every table field; restricting the presented set to the caller projection is a later optimization.
     *
     * <p>A pass-through file needs no widening for equality deletes: the core read decodes every column the predicate
     * references for the record filter and narrows the output back to the caller projection on its own, dropping the
     * equality columns the caller did not ask to see.
     */
    private Query oneFileQuery(int index, Predicate predicate, Projection projection) {
        IcebergManifests.DataFileRef ref = dataFiles.get(index);
        Optional<RowPositionSet> deleted = deletePlan.positionsFor(ref);
        Optional<Predicate> equalityDeletes = deletePlan.equalityDeletesFor(ref);
        Map<Integer, Value> partitionConstants =
                IcebergPartitionValues.constantsFor(partitionSpec, ref.partitionValues());
        ParquetSchema fileSchema = perFile(index).schema();
        validateNested(index, fileSchema);
        IcebergFileSchema file = IcebergFileSchema.of(fileSchema, nameMapping);
        Reconciliation recon = IcebergReconciliation.reconcile(icebergSchema, file, partitionConstants);
        Predicate withEquality = andEqualityDeletes(predicate, equalityDeletes);
        if (recon.passThrough()) {
            Projection produce = passThroughProjection(projection, fileSchema, file, ref);
            return Query.of(withDeletes(withEquality, deleted), produce);
        }
        Predicate folded = ConstantFolding.fold(withEquality, constantColumns(recon), addedNullColumns(recon));
        if (folded.equals(Predicate.ALWAYS_FALSE)) {
            return null;
        }
        Projection produce = reconciledProjection(recon, projection, file, ref);
        return Query.of(withDeletes(folded, deleted), produce);
    }

    /**
     * The produce set for a pass-through file. A caller projection that names a lineage column has it rewritten to the
     * synthesized column for this file; {@link Projection#ALL} is narrowed to exclude any physically materialized
     * reserved lineage leaf, keeping {@code _row_id} / {@code _last_updated_sequence_number} out of a default read. A
     * projection that names no lineage column and a file with no materialized lineage leaf pass through unchanged,
     * keeping a plain read byte-identical to a read of a table without lineage. Below format version 3 there is no row
     * lineage: every projection passes through untouched, and a column that merely shares a reserved name is read as
     * the user data it is.
     */
    private Projection passThroughProjection(
            Projection projection, ParquetSchema fileSchema, IcebergFileSchema file, IcebergManifests.DataFileRef ref) {
        if (!rowLineage()) {
            return projection;
        }
        if (!(projection instanceof Projection.Of(SequencedSet<Projection.Column> columns))) {
            return allExcludingReservedLineage(fileSchema, file);
        }
        if (!requestsLineage(columns)) {
            return projection;
        }
        SequencedSet<Projection.Column> rewritten = LinkedHashSet.newLinkedHashSet(columns.size());
        for (Projection.Column column : columns) {
            rewritten.add(isLineage(column.name()) ? lineageColumn(column.name(), file, ref) : column);
        }
        return Projection.of(rewritten);
    }

    /**
     * {@link Projection#ALL} narrowed to drop the physically materialized reserved lineage leaves a data file holds,
     * which keeps the lineage columns out of a default ({@code SELECT *}) read. The leaves are located exactly as
     * {@link #rowIdColumn} finds them ({@link #materializedLineageColumn}), whatever their physical names. A file that
     * holds no such leaf keeps {@link Projection#ALL}, preserving the plain-read fast path.
     */
    private static Projection allExcludingReservedLineage(ParquetSchema fileSchema, IcebergFileSchema file) {
        Set<ColumnPath> reserved = reservedLineagePaths(file);
        if (reserved.isEmpty()) {
            return Projection.ALL;
        }
        List<ColumnPath> leaves = fileSchema.leafColumns();
        List<ColumnPath> kept = new ArrayList<>(leaves.size());
        for (ColumnPath leaf : leaves) {
            if (!reserved.contains(leaf)) {
                kept.add(leaf);
            }
        }
        return Projection.ofPhysical(kept);
    }

    /** The physical paths of the reserved lineage leaves the file materializes. */
    private static Set<ColumnPath> reservedLineagePaths(IcebergFileSchema file) {
        Set<ColumnPath> reserved = new HashSet<>();
        materializedLineageColumn(file, ROW_ID_FIELD_ID, ROW_ID).ifPresent(column -> reserved.add(column.path()));
        materializedLineageColumn(file, LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID, LAST_UPDATED_SEQUENCE_NUMBER)
                .ifPresent(column -> reserved.add(column.path()));
        return reserved;
    }

    /**
     * The file's materialized reserved lineage column, located by reserved field id. Only a file where nothing resolves
     * by field id (no embedded ids and no mapping match) falls back to the reserved name, keeping the SELECT-*
     * exclusion and the projection rewrite consistent for such files. A file whose id index resolved anything never
     * falls back: a column merely named like a reserved one is user data there.
     */
    static Optional<IcebergFileSchema.FileColumn> materializedLineageColumn(
            IcebergFileSchema file, int reservedFieldId, ColumnPath reservedName) {
        Optional<IcebergFileSchema.FileColumn> byId = file.byFieldId(reservedFieldId);
        if (byId.isPresent() || file.hasFieldIds()) {
            return byId;
        }
        return file.byName(reservedName.name());
    }

    /**
     * The reconciled produce set with the caller's requested lineage columns appended. An evolved file already presents
     * every table field; a projected lineage column rides on top of that produce set rather than being dropped. Below
     * format version 3 nothing is appended: a reserved name in the caller projection is not a lineage request there.
     */
    private Projection reconciledProjection(
            Reconciliation recon, Projection projection, IcebergFileSchema file, IcebergManifests.DataFileRef ref) {
        SequencedSet<Projection.Column> columns = new LinkedHashSet<>(recon.columns());
        if (rowLineage()) {
            for (ColumnPath name : requestedLineageNames(projection)) {
                columns.add(lineageColumn(name, file, ref));
            }
        }
        return Projection.of(columns);
    }

    private static boolean requestsLineage(SequencedSet<Projection.Column> columns) {
        for (Projection.Column column : columns) {
            if (isLineage(column.name())) {
                return true;
            }
        }
        return false;
    }

    private static List<ColumnPath> requestedLineageNames(Projection projection) {
        if (!(projection instanceof Projection.Of(SequencedSet<Projection.Column> columns))) {
            return List.of();
        }
        List<ColumnPath> names = new ArrayList<>();
        for (Projection.Column column : columns) {
            if (isLineage(column.name())) {
                names.add(column.name());
            }
        }
        return names;
    }

    private static boolean isLineage(ColumnPath name) {
        return name.equals(ROW_ID) || name.equals(LAST_UPDATED_SEQUENCE_NUMBER);
    }

    /**
     * The synthesized Iceberg row-lineage column for this data file: the per-row coalesce of the file's materialized
     * value and the value computed from the manifest. A file that physically holds the reserved lineage field presents
     * a coalesce column where a stored cell wins and a null cell falls back to the computed value; otherwise
     * {@code _row_id} is the file's {@code first_row_id} plus each row's position, and
     * {@code _last_updated_sequence_number} is the file's data sequence number. A file with no row lineage presents
     * both columns as null.
     */
    private static Projection.Column lineageColumn(
            ColumnPath name, IcebergFileSchema file, IcebergManifests.DataFileRef ref) {
        if (name.equals(ROW_ID)) {
            return rowIdColumn(name, file, ref);
        }
        return lastUpdatedSequenceNumberColumn(name, file, ref);
    }

    private static Projection.Column rowIdColumn(
            ColumnPath name, IcebergFileSchema file, IcebergManifests.DataFileRef ref) {
        Optional<IcebergFileSchema.FileColumn> materialized = materializedLineageColumn(file, ROW_ID_FIELD_ID, name);
        if (materialized.isPresent() && ref.firstRowId() != null) {
            Projection.Column.Coalesce.Fallback fallback =
                    new Projection.Column.Coalesce.Fallback.Position(ref.firstRowId());
            return new Projection.Column.Coalesce(name, materialized.get().path(), fallback);
        }
        if (materialized.isPresent()) {
            return new Projection.Column.Physical(name, materialized.get().path());
        }
        if (ref.firstRowId() != null) {
            return new Projection.Column.RowPosition(name, ref.firstRowId());
        }
        return new Projection.Column.Null(name, new Value.LongVal(0L));
    }

    private static Projection.Column lastUpdatedSequenceNumberColumn(
            ColumnPath name, IcebergFileSchema file, IcebergManifests.DataFileRef ref) {
        Optional<IcebergFileSchema.FileColumn> materialized =
                materializedLineageColumn(file, LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID, name);
        if (materialized.isPresent()) {
            Projection.Column.Coalesce.Fallback fallback =
                    new Projection.Column.Coalesce.Fallback.Constant(new Value.LongVal(ref.dataSequenceNumber()));
            return new Projection.Column.Coalesce(name, materialized.get().path(), fallback);
        }
        if (ref.firstRowId() == null) {
            return new Projection.Column.Null(name, new Value.LongVal(0L));
        }
        return new Projection.Column.Constant(name, new Value.LongVal(ref.dataSequenceNumber()));
    }

    /**
     * ANDs the data file's positional deletes into {@code base} as a {@link Predicate.RowIndexExcluded} leaf over the
     * synthesized row-position column. A file with no positional delete keeps {@code base} unchanged.
     */
    private static Predicate withDeletes(Predicate base, Optional<RowPositionSet> deleted) {
        if (deleted.isEmpty()) {
            return base;
        }
        Predicate deleteLeaf = new Predicate.RowIndexExcluded(ROW_POSITION, deleted.orElseThrow());
        return conjoin(base, deleteLeaf);
    }

    /**
     * ANDs the data file's equality-delete anti-predicate into {@code base}. The anti-predicate keeps every row that
     * matches no applicable delete tuple; a file with no applicable equality delete keeps {@code base} unchanged.
     */
    private static Predicate andEqualityDeletes(Predicate base, Optional<Predicate> equalityDeletes) {
        if (equalityDeletes.isEmpty()) {
            return base;
        }
        return conjoin(base, equalityDeletes.orElseThrow());
    }

    /**
     * ANDs {@code add} into {@code base}, keeping a bare delete (the caller passed no predicate) a single leaf rather
     * than {@code ALWAYS_TRUE AND leaf}. The single leaf lets the reader eliminate fully-deleted row groups and pages
     * before decoding.
     */
    private static Predicate conjoin(Predicate base, Predicate add) {
        if (base.equals(Predicate.ALWAYS_TRUE)) {
            return add;
        }
        return base.and(add);
    }

    /**
     * The table columns reconciliation presents as a constant for every row, keyed by their table column path. Both an
     * omitted identity-partition column and an added column with an {@code initial-default} reconcile to a constant; a
     * predicate over either folds against its value rather than reaching the file, which does not hold the column.
     */
    private static Map<ColumnPath, Value> constantColumns(Reconciliation recon) {
        Map<ColumnPath, Value> constants = new HashMap<>();
        for (Projection.Column column : recon.columns()) {
            if (column instanceof Projection.Column.Constant(ColumnPath name, Value value)) {
                constants.put(name, value);
            }
        }
        return constants;
    }

    /** The names of the table columns this file does not hold, which reconciliation presents as typed nulls. */
    private static Set<ColumnPath> addedNullColumns(Reconciliation recon) {
        Set<ColumnPath> nulls = new LinkedHashSet<>();
        for (Projection.Column column : recon.columns()) {
            if (column instanceof Projection.Column.Null injected) {
                nulls.add(injected.name());
            }
        }
        return nulls;
    }

    /**
     * Test hook: the one-file {@link Query} reconciliation builds for {@code index}, identity for a pass-through file.
     */
    Query oneFileQueryForTest(int index, Predicate predicate, Projection projection) {
        return oneFileQuery(index, predicate, projection);
    }

    /**
     * Fails loud when a data file's nested field ids disagree with the table schema. Runs before the file is read and
     * is validated at most once after it passes: a passing file is memoized, an offending file throws on every read.
     */
    private void validateNested(int index, ParquetSchema fileSchema) {
        if (nestedSchema.isEmpty() || nestedValidated.contains(index)) {
            return;
        }
        IcebergNestedGuard.validate(nestedSchema, fileSchema);
        nestedValidated.add(index);
    }

    /**
     * The single-file {@link ParquetSource} over the data file at {@code index}, parsed once and reused across queries
     * and threads. {@code computeIfAbsent} gives one footer parse per index even under concurrent reads; the dataset
     * borrows the catalog's shared source, which the catalog owns and closes.
     */
    private ParquetSource perFile(int index) {
        return perFileDatasets.computeIfAbsent(
                index, i -> ParquetSource.open(new SurvivorFileset(sources, List.of(i)), openOptions));
    }

    private int maxConcurrentFiles() {
        return openOptions.runtime().maxConcurrentFiles();
    }

    /**
     * A {@link FilesetReader} over a dense slice of the catalog's pre-opened sources. Dense index {@code i} maps to the
     * shared source at {@code survivorIndices.get(i)}. The sources are borrowed; closing the per-query dataset's
     * streams never closes them.
     */
    private record SurvivorFileset(List<ByteRangeSource> sources, List<Integer> survivorIndices)
            implements FilesetReader {

        @Override
        public ByteRangeSource openFile(int index) {
            int sharedIndex = survivorIndices.get(index);
            return sources.get(sharedIndex);
        }

        @Override
        public int fileCount() {
            return survivorIndices.size();
        }
    }
}
