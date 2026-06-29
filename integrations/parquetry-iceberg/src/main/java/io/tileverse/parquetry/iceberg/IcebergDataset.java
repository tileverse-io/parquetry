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
package io.tileverse.parquetry.iceberg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.CatalogSnapshot;
import io.tileverse.parquetry.dataset.DatasetCapabilities;
import io.tileverse.parquetry.dataset.DatasetCapabilities.FileStatsSource;
import io.tileverse.parquetry.dataset.FilesetReader;
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
import io.tileverse.parquetry.iceberg.IcebergReconciliation.Reconciliation;
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
 * <p>The byte sources are pre-opened and owned by the {@link IcebergCatalog}; the per-query {@link ParquetSource}
 * borrows the survivor subset and never closes them. The dataset presents the table's current schema and reconciles
 * each data file's columns to it by Iceberg field id; a file whose schema already matches the table reads untouched.
 */
final class IcebergDataset implements ParquetDataset {

    /** The synthesized row-position column the reader produces; the delete predicate filters on it. */
    private static final ColumnPath ROW_POSITION = ColumnPath.of("_pos");

    private final String name;
    private final CatalogSnapshot snapshot;
    private final IcebergSchema icebergSchema;
    private final IcebergPartitionSpec partitionSpec;
    private final List<IcebergManifests.DataFileRef> dataFiles;
    private final List<FileStats> fileStats;
    private final List<ByteRangeSource> sources;
    private final IcebergDeletePlan deletePlan;
    private final Map<Integer, ParquetSource> perFileDatasets = new ConcurrentHashMap<>();

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
            IcebergDeletePlan deletePlan) {
        this.name = Objects.requireNonNull(name, "name");
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        this.icebergSchema = Objects.requireNonNull(icebergSchema, "icebergSchema");
        this.partitionSpec = Objects.requireNonNull(partitionSpec, "partitionSpec");
        this.dataFiles = List.copyOf(dataFiles);
        this.fileStats = List.copyOf(fileStats);
        this.sources = List.copyOf(sources);
        this.deletePlan = Objects.requireNonNull(deletePlan, "deletePlan");
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
        List<Integer> survivors = prune(predicate).survivorIndices();
        if (survivors.isEmpty()) {
            return Stream.empty();
        }
        return survivors.stream().flatMap(index -> readOneFile(index, predicate, projection, options));
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

    @Override
    @MustBeClosed
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        List<Integer> survivors = prune(predicate).survivorIndices();
        if (survivors.isEmpty()) {
            return Stream.empty();
        }
        return survivors.stream().flatMap(index -> readOneFileBatches(index, predicate, projection, options));
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
        List<Integer> survivors = prune(predicate).survivorIndices();
        if (survivors.isEmpty()) {
            return Stream.empty();
        }
        return survivors.stream().flatMap(index -> readOneFile(index, predicate, projection, materializer, options));
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
     * <p>A file that matches the table by name (or has no field ids) is read untouched by name through the identity
     * {@link Query#of}, which preserves nested and lineage columns the table schema does not model and keeps the corpus
     * fast path free of any output shaping.
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
        IcebergFileSchema file = IcebergFileSchema.of(perFile(index).schema());
        Reconciliation recon = IcebergReconciliation.reconcile(icebergSchema, file, partitionConstants);
        Predicate withEquality = andEqualityDeletes(predicate, equalityDeletes);
        if (recon.passThrough()) {
            return Query.of(withDeletes(withEquality, deleted), projection);
        }
        Predicate folded = ConstantFolding.fold(withEquality, constantColumns(recon), addedNullColumns(recon));
        if (folded.equals(Predicate.ALWAYS_FALSE)) {
            return null;
        }
        return Query.of(withDeletes(folded, deleted), Projection.of(recon.columns()));
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
     * The single-file {@link ParquetSource} over the data file at {@code index}, parsed once and reused across queries
     * and threads. {@code computeIfAbsent} gives one footer parse per index even under concurrent reads; the dataset
     * borrows the catalog's shared source, which the catalog owns and closes.
     */
    private ParquetSource perFile(int index) {
        return perFileDatasets.computeIfAbsent(
                index, i -> ParquetSource.open(new SurvivorFileset(sources, List.of(i))));
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
