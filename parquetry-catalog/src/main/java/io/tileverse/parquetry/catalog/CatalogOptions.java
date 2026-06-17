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
 * Catalog construction options: an optional dataset-name override and an optional cap on the hive-partition depth used
 * to group files into datasets. Schema-union controls arrive in later increments without changing the {@code open}
 * signature.
 */
public record CatalogOptions(Optional<String> datasetName, Optional<Integer> maxHiveDepth) {

    public CatalogOptions {
        Objects.requireNonNull(datasetName, "datasetName");
        Objects.requireNonNull(maxHiveDepth, "maxHiveDepth");
    }

    public static CatalogOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Optional<String> datasetName = Optional.empty();
        private Integer maxHiveDepth;

        public Builder datasetName(String name) {
            this.datasetName = Optional.ofNullable(name);
            return this;
        }

        public Builder maxHiveDepth(Integer value) {
            this.maxHiveDepth = value;
            return this;
        }

        public CatalogOptions build() {
            return new CatalogOptions(datasetName, Optional.ofNullable(maxHiveDepth));
        }
    }
}
