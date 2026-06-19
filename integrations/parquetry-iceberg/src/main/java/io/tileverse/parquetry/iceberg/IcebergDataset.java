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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.CatalogSnapshot;
import io.tileverse.parquetry.dataset.Dataset;
import io.tileverse.parquetry.dataset.DatasetCapabilities;
import io.tileverse.parquetry.dataset.DatasetCapabilities.FileStatsSource;
import io.tileverse.parquetry.dataset.FilesetReader;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.dataset.explain.FileExplain;
import io.tileverse.parquetry.dataset.explain.Outcome;
import io.tileverse.parquetry.dataset.explain.Totals;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.prune.FilePruner;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * A {@link Dataset} over one Iceberg table at a pinned snapshot. Before each query the dataset prunes the snapshot's
 * data files by their manifest bounds: a file whose statistics prove no row can match the predicate is skipped, and the
 * query opens a {@link ParquetDataset} over only the survivors. Pruning is pure work-avoidance; survivors are still
 * filtered at row-group and record level during the read. The result is identical to scanning every file.
 *
 * <p>The byte sources are pre-opened and owned by the {@link IcebergCatalog}; the per-query {@link ParquetDataset}
 * borrows the survivor subset and never closes them. Data files are read by their own schema in this cut.
 */
final class IcebergDataset implements Dataset {

    private final String name;
    private final CatalogSnapshot snapshot;
    private final ParquetSchema schema;
    private final List<IcebergManifests.DataFileRef> dataFiles;
    private final List<FileStats> fileStats;
    private final List<ByteRangeSource> sources;

    IcebergDataset(
            String name,
            CatalogSnapshot snapshot,
            ParquetSchema schema,
            List<IcebergManifests.DataFileRef> dataFiles,
            List<FileStats> fileStats,
            List<ByteRangeSource> sources) {
        this.name = Objects.requireNonNull(name, "name");
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        this.schema = Objects.requireNonNull(schema, "schema");
        this.dataFiles = List.copyOf(dataFiles);
        this.fileStats = List.copyOf(fileStats);
        this.sources = List.copyOf(sources);
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
        return schema;
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
        ParquetDataset query = openSurvivors(survivors);
        return query.read(predicate, projection, options);
    }

    @Override
    @MustBeClosed
    public <T> Stream<T> read(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        List<Integer> survivors = prune(predicate).survivorIndices();
        if (survivors.isEmpty()) {
            return Stream.empty();
        }
        ParquetDataset query = openSurvivors(survivors);
        return query.read(predicate, projection, materializer, options);
    }

    @Override
    public long count(Predicate predicate, ReadOptions options) {
        List<Integer> survivors = prune(predicate).survivorIndices();
        if (survivors.isEmpty()) {
            return 0L;
        }
        ParquetDataset query = openSurvivors(survivors);
        return query.count(predicate, options);
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
        ParquetDataset survivor = openSurvivors(List.of(index));
        ExplainPlan plan = analyze
                ? survivor.explainAnalyze(predicate, projection, options)
                : survivor.explain(predicate, projection, options);
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

    private ParquetDataset openSurvivors(List<Integer> survivorIndices) {
        return ParquetDataset.open(new SurvivorFileset(sources, survivorIndices));
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
