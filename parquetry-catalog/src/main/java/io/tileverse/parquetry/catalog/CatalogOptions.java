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
package io.tileverse.parquetry.catalog;

import java.util.Objects;
import java.util.Optional;

/**
 * Catalog construction options. Minimal today (an optional dataset-name override); hive-partitioning and schema-union
 * controls arrive in later increments without changing the {@code ParquetDatasetCatalog.open} signature.
 */
public record CatalogOptions(Optional<String> datasetName) {

    public CatalogOptions {
        Objects.requireNonNull(datasetName, "datasetName");
    }

    public static CatalogOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Optional<String> datasetName = Optional.empty();

        public Builder datasetName(String name) {
            this.datasetName = Optional.ofNullable(name);
            return this;
        }

        public CatalogOptions build() {
            return new CatalogOptions(datasetName);
        }
    }
}
