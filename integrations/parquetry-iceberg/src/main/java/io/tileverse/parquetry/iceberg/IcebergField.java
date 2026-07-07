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
package io.tileverse.parquetry.iceberg;

import java.util.Objects;
import java.util.Optional;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.schema.geo.ParquetCrs;

/**
 * One field of an Iceberg table schema: its field id, column name, primitive type string, whether the field is required
 * (non-nullable), its {@code initial-default} value when the schema declares one (v3 column defaults), and - for a
 * geometry/geography field - the coordinate reference system and geography edge-interpolation algorithm parsed from the
 * Iceberg type token.
 *
 * <p>The {@code initialDefault} reads back for an added column that a data file written before the column existed does
 * not contain; an empty optional means the absent column reads as null. {@code crs} is empty for a non-geometry field
 * and for a geometry/geography token whose CRS is absent or unclassifiable; {@code geographyAlgorithm} is present only
 * for a geography token that declares an edge-interpolation algorithm and is empty otherwise.
 */
record IcebergField(
        int fieldId,
        String name,
        String type,
        boolean required,
        Optional<Value> initialDefault,
        Optional<ParquetCrs> crs,
        Optional<EdgeInterpolationAlgorithm> geographyAlgorithm) {

    public IcebergField {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(initialDefault, "initialDefault");
        Objects.requireNonNull(crs, "crs");
        Objects.requireNonNull(geographyAlgorithm, "geographyAlgorithm");
    }

    IcebergField(int fieldId, String name, String type, boolean required, Optional<Value> initialDefault) {
        this(fieldId, name, type, required, initialDefault, Optional.empty(), Optional.empty());
    }

    IcebergField(int fieldId, String name, String type, boolean required) {
        this(fieldId, name, type, required, Optional.empty());
    }

    public boolean isGeometry() {
        return "geometry".equals(type) || "geography".equals(type);
    }

    public boolean isGeography() {
        return "geography".equals(type);
    }
}
