/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.internal.write;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.columnar.AugmentedRecordBatch;
import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.CoveringMode;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

/**
 * Resolves whether a write generates a {@code bbox} covering for the primary geometry column, at what precision, and
 * over what augmented schema, then adds the derived struct to each batch. An unset {@link WriteOptions#bboxCovering()}
 * resolves to AUTO when the file emits GeoParquet 1.1 metadata and to NONE under
 * {@link GeoParquetMetadataMode#V2_0_ONLY}; a coherence problem is an error for an explicit request and a downgrade to
 * inactive for a defaulted one.
 */
public final class BboxCoveringPlan {

    private static final String BBOX = "bbox";

    private final boolean active;
    private final ParquetSchema writtenSchema;
    private final ColumnPath geometryColumn;
    private final ColumnPath xmin;
    private final ColumnPath xmax;
    private final ColumnPath ymin;
    private final ColumnPath ymax;
    private final boolean useFloat;

    private BboxCoveringPlan(boolean active, ParquetSchema writtenSchema, ColumnPath geometryColumn, boolean useFloat) {
        this.active = active;
        this.writtenSchema = writtenSchema;
        this.geometryColumn = geometryColumn;
        this.useFloat = useFloat;
        this.xmin = ColumnPath.of(BBOX, "xmin");
        this.xmax = ColumnPath.of(BBOX, "xmax");
        this.ymin = ColumnPath.of(BBOX, "ymin");
        this.ymax = ColumnPath.of(BBOX, "ymax");
    }

    public static BboxCoveringPlan resolve(WriteOptions options, ParquetSchema schema, GeoMetadataWriter geoWriter) {
        Optional<CoveringMode> requested = options.bboxCovering();
        boolean explicit = requested.isPresent() && requested.orElseThrow() != CoveringMode.NONE;
        List<ColumnPath> geometryColumns = geoWriter.geometryColumns(schema);

        if (requested.isPresent() && requested.orElseThrow() == CoveringMode.NONE) {
            return inactive(schema);
        }
        if (geometryColumns.isEmpty()) {
            return requireOrDowngrade(explicit, schema, "bbox covering requires a geometry column; none is configured");
        }
        if (schema.find(ColumnPath.of(BBOX)).isPresent()) {
            return requireOrDowngrade(
                    explicit, schema, "cannot derive a bbox covering: the schema already has a column named 'bbox'");
        }
        if (!explicit && options.geoParquetMetadata() == GeoParquetMetadataMode.V2_0_ONLY) {
            return inactive(schema);
        }

        ColumnPath geometryColumn = geometryColumns.get(0);
        boolean useFloat = resolvePrecision(options, requested, geometryColumn);
        ParquetSchema writtenSchema = schema.withAppendedGroup(coveringGroup(useFloat));
        return new BboxCoveringPlan(true, writtenSchema, geometryColumn, useFloat);
    }

    /**
     * FLOAT and DOUBLE fix the precision outright; AUTO picks FLOAT for the WGS84 lon/lat range (where float ordinates
     * hold enough precision) and DOUBLE otherwise. An unset request is AUTO.
     */
    private static boolean resolvePrecision(
            WriteOptions options, Optional<CoveringMode> requested, ColumnPath geometryColumn) {
        CoveringMode mode = requested.orElse(CoveringMode.AUTO);
        return switch (mode) {
            case FLOAT -> true;
            case DOUBLE -> false;
            case AUTO -> isCrs84Compatible(options.crs().get(geometryColumn.dot()));
            case NONE -> false;
        };
    }

    /** A geometry column with no configured CRS defaults to CRS84, hence a missing entry is treated as compatible. */
    private static boolean isCrs84Compatible(CoordinateReferenceSystem crs) {
        if (crs == null) {
            return true;
        }
        boolean epsg4326 = crs.id().map(id -> id.epsgCode().orElse(-1) == 4326).orElse(false);
        return epsg4326 || crs.equals(CoordinateReferenceSystems.ogcCrs84());
    }

    private static SchemaNode.Group coveringGroup(boolean useFloat) {
        PrimitiveKind kind = useFloat ? PrimitiveKind.FLOAT : PrimitiveKind.DOUBLE;
        List<SchemaNode> leaves = List.of(
                coveringLeaf("xmin", kind),
                coveringLeaf("xmax", kind),
                coveringLeaf("ymin", kind),
                coveringLeaf("ymax", kind));
        return new SchemaNode.Group(BBOX, Repetition.REQUIRED, leaves, Optional.empty(), -1);
    }

    private static SchemaNode.Primitive coveringLeaf(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static BboxCoveringPlan requireOrDowngrade(boolean explicit, ParquetSchema schema, String message) {
        if (explicit) {
            throw new ParquetWriteException(message);
        }
        return inactive(schema);
    }

    private static BboxCoveringPlan inactive(ParquetSchema schema) {
        return new BboxCoveringPlan(false, schema, null, false);
    }

    public boolean active() {
        return active;
    }

    public ParquetSchema writtenSchema() {
        return writtenSchema;
    }

    public ColumnPath geometryColumn() {
        return geometryColumn;
    }

    public ColumnPath xmin() {
        return xmin;
    }

    public ColumnPath xmax() {
        return xmax;
    }

    public ColumnPath ymin() {
        return ymin;
    }

    public ColumnPath ymax() {
        return ymax;
    }

    /** Returns {@code raw} with the derived {@code bbox} struct added; returns {@code raw} unchanged when inactive. */
    public ParquetRecordBatch augment(ParquetRecordBatch raw) {
        if (!active) {
            return raw;
        }
        BinaryVector geometry = (BinaryVector) raw.columns().get(geometryColumn);
        StructVector bbox = BboxCoveringDeriver.derive(geometry, raw.rowCount(), useFloat);
        Map<ColumnPath, ColumnVector> augmented = new LinkedHashMap<>(raw.columns());
        augmented.put(ColumnPath.of(BBOX), bbox);
        return AugmentedRecordBatch.of(raw, writtenSchema, augmented);
    }
}
