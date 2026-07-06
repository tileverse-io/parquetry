/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.stac;

import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Options for opening a {@link StacDatasetCatalog}. {@code parquetMediaTypes} is the set of asset media types treated
 * as GeoParquet data (compared case-insensitively, media-type parameters ignored); an asset whose type is unset still
 * counts when its href ends in {@code .parquet}. {@code geometryColumn} names the collections' primary geometry column,
 * used to attach each item bbox as a pruning bound.
 */
public record StacCatalogOptions(Set<String> parquetMediaTypes, String geometryColumn) {

    private static final Set<String> DEFAULT_MEDIA_TYPES =
            Set.of("application/vnd.apache.parquet", "application/x-parquet", "application/parquet");

    public StacCatalogOptions {
        Objects.requireNonNull(parquetMediaTypes, "parquetMediaTypes");
        Objects.requireNonNull(geometryColumn, "geometryColumn");
        Set<String> normalized = new LinkedHashSet<>();
        for (String mediaType : parquetMediaTypes) {
            normalized.add(mediaType.toLowerCase(Locale.ROOT));
        }
        parquetMediaTypes = Set.copyOf(normalized);
    }

    public static StacCatalogOptions defaults() {
        return builder().build();
    }

    public boolean isParquet(String assetType, String href) {
        if (assetType != null) {
            String bare = assetType.split(";", 2)[0].trim().toLowerCase(Locale.ROOT);
            if (parquetMediaTypes.contains(bare)) {
                return true;
            }
        }
        return href != null && href.toLowerCase(Locale.ROOT).endsWith(".parquet");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Set<String> parquetMediaTypes = DEFAULT_MEDIA_TYPES;
        private String geometryColumn = "geometry";

        private Builder() {}

        public Builder parquetMediaTypes(Set<String> value) {
            this.parquetMediaTypes = Objects.requireNonNull(value, "parquetMediaTypes");
            return this;
        }

        public Builder geometryColumn(String value) {
            this.geometryColumn = Objects.requireNonNull(value, "geometryColumn");
            return this;
        }

        public StacCatalogOptions build() {
            return new StacCatalogOptions(parquetMediaTypes, geometryColumn);
        }
    }
}
