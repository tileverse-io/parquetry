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
package io.tileverse.parquetry.geotools;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.catalog.CatalogCapabilities;
import io.tileverse.parquetry.catalog.DatasetCatalog;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.CatalogSnapshot;
import io.tileverse.parquetry.dataset.DatasetCapabilities;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

class GeoParquetDataStoreNonGeoCatalogTest {

    @Test
    void rejectsCatalogWithNonGeoDataset() {
        DatasetCatalog nonGeoCatalog = new NonGeoCatalog("plain");
        assertThatThrownBy(() -> new GeoParquetDataStore(nonGeoCatalog))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("plain")
                .hasMessageContaining("GeoParquet");
    }

    /** A catalog exposing one plain (non-geo) dataset; only enumeration and resolution are exercised. */
    private record NonGeoCatalog(String name) implements DatasetCatalog {

        @Override
        public CatalogCapabilities capabilities() {
            return CatalogCapabilities.builder().build();
        }

        @Override
        public List<String> datasets() {
            return List.of(name);
        }

        @Override
        public ParquetDataset dataset(String requested) {
            return new PlainDataset(requested);
        }

        @Override
        public void close() {
            // no resources to release
        }
    }

    /** A plain {@link ParquetDataset} that is not a {@code GeoParquetDataset}; query methods are never called. */
    private record PlainDataset(String name) implements ParquetDataset {

        @Override
        public ParquetSchema schema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<CatalogSnapshot> snapshot() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DatasetCapabilities capabilities() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Stream<T> read(
                Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long count(Predicate predicate, ReadOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DatasetExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DatasetExplainPlan explainAnalyze(Predicate predicate, Projection projection, ReadOptions options) {
            throw new UnsupportedOperationException();
        }
    }
}
