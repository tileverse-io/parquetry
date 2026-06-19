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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

class DatasetBoundsDefaultTest {

    @Test
    void boundsDefaultsToEmpty() {
        Dataset minimal = new Dataset() {
            public String name() {
                return "t";
            }

            public ParquetSchema schema() {
                return null;
            }

            public Optional<CatalogSnapshot> snapshot() {
                return Optional.empty();
            }

            public DatasetCapabilities capabilities() {
                return DatasetCapabilities.builder().build();
            }

            public Stream<ParquetRecord> read(Predicate p, Projection pr, ReadOptions o) {
                return Stream.empty();
            }

            public <T> Stream<T> read(Predicate p, Projection pr, Materializer<T> m, ReadOptions o) {
                return Stream.empty();
            }

            public long count(Predicate p, ReadOptions o) {
                return 0;
            }

            public DatasetExplainPlan explain(Predicate p, Projection pr, ReadOptions o) {
                return null;
            }

            public DatasetExplainPlan explainAnalyze(Predicate p, Projection pr, ReadOptions o) {
                return null;
            }
        };
        assertThat(minimal.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS)).isEmpty();
    }
}
