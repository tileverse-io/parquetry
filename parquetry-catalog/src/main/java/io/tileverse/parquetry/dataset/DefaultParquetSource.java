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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import io.tileverse.parquetry.columnar.BatchMaterializer;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.RowGroupSummary;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * The {@link ParquetSource} over one file: every read, count, bounds and explain call goes to the file's
 * {@link ParquetFileReader}, which shares one parsed footer across them. Instances are safe to share across threads;
 * each call builds its own read state.
 */
final class DefaultParquetSource implements ParquetSource {

    private final ParquetFileReader reader;

    DefaultParquetSource(ParquetFileReader reader) {
        this.reader = Objects.requireNonNull(reader, "reader");
    }

    @Override
    public ParquetSchema schema() {
        return reader.schema();
    }

    @Override
    public Map<String, String> keyValueMetadata() {
        return reader.keyValueMetadata();
    }

    @Override
    public List<RowGroupSummary> rowGroups() {
        return reader.rowGroups();
    }

    @Override
    public FileStats fileStats() {
        return reader.fileStats();
    }

    @Override
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        return reader.read(predicate, projection, Materializer.defaultRecord(), options);
    }

    @Override
    public <T> Stream<T> read(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        return reader.read(predicate, projection, materializer, options);
    }

    @Override
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        return reader.readBatches(predicate, projection, options);
    }

    @Override
    public <T> Stream<T> readBatches(
            Predicate predicate, Projection projection, BatchMaterializer<T> materializer, ReadOptions options) {
        return reader.readBatches(predicate, projection, materializer, options);
    }

    @Override
    public ExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options) {
        return reader.explain(predicate, projection, options);
    }

    @Override
    public ExplainPlan explainAnalyze(Predicate predicate, Projection projection, ReadOptions options) {
        return reader.explainAnalyze(predicate, projection, options);
    }

    @Override
    public long count(Predicate predicate, ReadOptions options) {
        return reader.count(predicate, options);
    }

    @Override
    public Optional<BoundingBox> bounds(Predicate predicate, ReadOptions options) {
        return reader.bounds(predicate, options);
    }
}
