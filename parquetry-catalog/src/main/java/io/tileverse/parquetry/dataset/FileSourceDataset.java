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

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * A {@link Dataset} over one {@link DatasetUnit}, delegating the read/count/explain calls to a {@link ParquetDataset}
 * opened over the unit's files. Pure-parquet: unversioned, name-based, with no field-id resolution.
 */
public final class FileSourceDataset implements Dataset {

    private final String name;
    private final ParquetDataset delegate;
    private final DatasetCapabilities capabilities;

    public FileSourceDataset(String name, ParquetDataset delegate, DatasetCapabilities capabilities) {
        this.name = Objects.requireNonNull(name, "name");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ParquetSchema schema() {
        return delegate.schema();
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
    @MustBeClosed
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        return delegate.read(predicate, projection, options);
    }

    @Override
    @MustBeClosed
    public <T> Stream<T> read(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        return delegate.read(predicate, projection, materializer, options);
    }

    @Override
    public long count(Predicate predicate, ReadOptions options) {
        return delegate.count(predicate, options);
    }

    @Override
    public ExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options) {
        return delegate.explain(predicate, projection, options);
    }
}
