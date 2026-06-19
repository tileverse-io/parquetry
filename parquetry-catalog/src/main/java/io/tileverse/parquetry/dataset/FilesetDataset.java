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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.data.ReadOptions;
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
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * A {@link GeoParquetDataset} composed from one or many same-schema Parquet files. Before each query it prunes files
 * whose Hive partition value cannot match the predicate (the same {@link FilePruner} path the Iceberg backend uses, fed
 * from path values as exact statistics) and reads only the survivors. When nothing prunes it reuses one pre-opened
 * {@link ParquetDataset} over every file, which keeps the common unpartitioned case reading each footer once.
 */
public final class FilesetDataset implements GeoParquetDataset {

    private final String name;
    private final ParquetDataset allFiles;
    private final List<ByteRangeSource> sources;
    private final List<String> locations;
    private final List<FileStats> partitionStats;
    private final DatasetCapabilities capabilities;
    private final Optional<GeoParquetMetadata> geoMetadata;
    private final Optional<BoundingBox> aggregatedBounds;

    public FilesetDataset(
            String name,
            ParquetDataset allFiles,
            List<ByteRangeSource> sources,
            List<String> locations,
            List<FileStats> partitionStats,
            DatasetCapabilities capabilities,
            Optional<GeoParquetMetadata> geoMetadata) {
        this.name = Objects.requireNonNull(name, "name");
        this.allFiles = Objects.requireNonNull(allFiles, "allFiles");
        this.sources = List.copyOf(sources);
        this.locations = List.copyOf(locations);
        this.partitionStats = List.copyOf(partitionStats);
        this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
        this.geoMetadata = Objects.requireNonNull(geoMetadata, "geoMetadata");
        this.aggregatedBounds = geoMetadata.flatMap(FilesetDataset::primaryBbox);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ParquetSchema schema() {
        return allFiles.schema();
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
        return Optional.empty();
    }

    @Override
    @MustBeClosed
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        ParquetDataset query = surviving(predicate);
        if (query == null) {
            return Stream.empty();
        }
        return query.read(predicate, projection, options);
    }

    @Override
    @MustBeClosed
    public <T> Stream<T> read(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        ParquetDataset query = surviving(predicate);
        if (query == null) {
            return Stream.empty();
        }
        return query.read(predicate, projection, materializer, options);
    }

    @Override
    public long count(Predicate predicate, ReadOptions options) {
        ParquetDataset query = surviving(predicate);
        if (query == null) {
            return 0L;
        }
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
        ParquetDataset survivor = ParquetDataset.open(new SurvivorFileset(sources, List.of(index)));
        ExplainPlan plan = analyze
                ? survivor.explainAnalyze(predicate, projection, options)
                : survivor.explain(predicate, projection, options);
        return new FileExplain(location, Outcome.KEEP, "kept", recordCount, Optional.of(plan));
    }

    /**
     * The dataset over the files surviving {@code predicate}: {@code allFiles} when nothing prunes, null when all
     * prune.
     */
    private ParquetDataset surviving(Predicate predicate) {
        List<Integer> survivors = pruneSurvivors(predicate);
        if (survivors.isEmpty()) {
            return null;
        }
        if (survivors.size() == sources.size()) {
            // Nothing pruned: pruneSurvivors emits [0..n) ascending, identical to allFiles.
            return allFiles;
        }
        return ParquetDataset.open(new SurvivorFileset(sources, survivors));
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
