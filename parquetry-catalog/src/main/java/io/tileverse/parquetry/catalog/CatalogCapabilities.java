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

/**
 * Connection-wide capabilities of a {@link DatasetCatalog}, built once at open time. Callers read the snapshot and
 * branch on it; they never probe behavior by trial and error. Boolean defaults are safe-false; {@code require*} guards
 * convert an unsupported request into a precise exception at the boundary.
 */
public record CatalogCapabilities(boolean enumeratesDatasets, boolean timeTravel, SchemaSource schemaSource) {

    /** Where a dataset's unified schema comes from. */
    public enum SchemaSource {
        MERGED_FILES,
        TABLE_METADATA,
        COLLECTION
    }

    public CatalogCapabilities {
        Objects.requireNonNull(schemaSource, "schemaSource");
    }

    public void requireTimeTravel() {
        if (!timeTravel) {
            throw new UnsupportedOperationException("catalog capability not supported: timeTravel");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder with safe-false defaults. */
    public static final class Builder {
        private boolean enumeratesDatasets;
        private boolean timeTravel;
        private SchemaSource schemaSource = SchemaSource.MERGED_FILES;

        private Builder() {}

        public Builder enumeratesDatasets(boolean value) {
            this.enumeratesDatasets = value;
            return this;
        }

        public Builder timeTravel(boolean value) {
            this.timeTravel = value;
            return this;
        }

        public Builder schemaSource(SchemaSource value) {
            this.schemaSource = Objects.requireNonNull(value, "schemaSource");
            return this;
        }

        public CatalogCapabilities build() {
            return new CatalogCapabilities(enumeratesDatasets, timeTravel, schemaSource);
        }
    }
}
