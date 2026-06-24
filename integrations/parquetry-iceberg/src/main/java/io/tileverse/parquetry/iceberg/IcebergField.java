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
package io.tileverse.parquetry.iceberg;

import java.util.Objects;

/**
 * One field of an Iceberg table schema: its field id, column name, primitive type string, and whether the field is
 * required (non-nullable).
 */
record IcebergField(int fieldId, String name, String type, boolean required) {

    public IcebergField {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
    }

    public boolean isGeometry() {
        return "geometry".equals(type) || "geography".equals(type);
    }

    public boolean isGeography() {
        return "geography".equals(type);
    }
}
