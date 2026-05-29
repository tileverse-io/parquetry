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
package io.tileverse.parquetry.data;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import io.tileverse.parquetry.batch.BatchMaterializer;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.filter.ExplainPlan;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Default {@link ParquetDataset} implementation: a collection of 1..N {@link ParquetReader} instances over files that
 * share the same schema. For a single reader the public methods passthrough; multi-reader orchestration lands in a
 * future release. Constructing with more than one reader is accepted, but any read-side method throws
 * {@link UnsupportedOperationException} until that work lands.
 *
 * <p>Schema check happens in the constructor: every reader must agree on {@link ParquetSchema} by equality.
 */
final class DefaultParquetDataset implements ParquetDataset {

    private final List<ParquetReader> readers;
    private final ParquetSchema schema;

    DefaultParquetDataset(List<ParquetReader> readers) {
        if (readers == null || readers.isEmpty()) {
            throw new IllegalArgumentException("readers must contain at least one ParquetReader");
        }
        ParquetSchema first = readers.get(0).schema();
        for (int i = 1; i < readers.size(); i++) {
            if (!first.equals(readers.get(i).schema())) {
                throw new IllegalArgumentException(
                        "ParquetReader at index " + i + " has a different schema than reader 0");
            }
        }
        this.readers = List.copyOf(readers);
        this.schema = first;
    }

    @Override
    public ParquetSchema schema() {
        return schema;
    }

    @Override
    public Map<String, String> keyValueMetadata() {
        return readers.get(0).keyValueMetadata();
    }

    @Override
    public List<RowGroupSummary> rowGroups() {
        return readers.get(0).rowGroups();
    }

    @Override
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        ensureSingleReader();
        return readers.get(0).read(predicate, projection, options);
    }

    @Override
    public <T> Stream<T> read(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        ensureSingleReader();
        return readers.get(0).read(predicate, projection, materializer, options);
    }

    @Override
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        ensureSingleReader();
        return readers.get(0).readBatches(predicate, projection, options);
    }

    @Override
    public <T> Stream<T> readBatches(
            Predicate predicate, Projection projection, BatchMaterializer<T> materializer, ReadOptions options) {
        ensureSingleReader();
        return readers.get(0).readBatches(predicate, projection, materializer, options);
    }

    @Override
    public ExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options) {
        ensureSingleReader();
        return readers.get(0).explain(predicate, projection, options);
    }

    private void ensureSingleReader() {
        if (readers.size() > 1) {
            throw new UnsupportedOperationException("Multi-file datasets land in a future release");
        }
    }
}
