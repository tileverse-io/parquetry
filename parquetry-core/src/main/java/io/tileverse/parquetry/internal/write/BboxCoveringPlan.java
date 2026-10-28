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
import io.tileverse.parquetry.data.WriteOptions.ExistingBboxCovering;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.BboxCovering;
import io.tileverse.parquetry.schema.geo.geoparquet.BoundingBoxColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.ColumnCrs;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

/**
 * Resolves the bbox covering of a write. The covering is either declared over leaves already in the schema, or derived:
 * a {@code bbox} group for the primary geometry column at a chosen precision, added to each batch. An AUTO request
 * declares an existing {@code bbox} struct instead of deriving one. An unset {@link WriteOptions#bboxCovering()}
 * resolves to AUTO in any metadata mode. A coherence problem is an error for an explicit request and a downgrade to
 * inactive for a defaulted one.
 */
public final class BboxCoveringPlan {

    private static final String BBOX = "bbox";

    private final boolean active;
    private final boolean derived;
    private final ParquetSchema writtenSchema;
    private final ColumnPath geometryColumn;
    private final CoveringPaths paths;
    private final boolean useFloat;

    /** The four leaves of one covering, in the order declared by the GeoParquet spec. */
    private record CoveringPaths(ColumnPath xmin, ColumnPath ymin, ColumnPath xmax, ColumnPath ymax) {}

    private BboxCoveringPlan(
            boolean active,
            boolean derived,
            ParquetSchema writtenSchema,
            ColumnPath geometryColumn,
            CoveringPaths paths,
            boolean useFloat) {
        this.active = active;
        this.derived = derived;
        this.writtenSchema = writtenSchema;
        this.geometryColumn = geometryColumn;
        this.paths = paths;
        this.useFloat = useFloat;
    }

    /** The leaves of the {@code bbox} struct, derived by the plan or declared as found in the schema. */
    private static CoveringPaths bboxStructPaths() {
        BboxCovering conventional = BboxCovering.conventional();
        return new CoveringPaths(conventional.xmin(), conventional.ymin(), conventional.xmax(), conventional.ymax());
    }

    public static BboxCoveringPlan resolve(WriteOptions options, ParquetSchema schema, GeoMetadataWriter geoWriter) {
        List<ColumnPath> geometryColumns = geoWriter.geometryColumns(schema);
        Optional<ExistingBboxCovering> existing = options.existingBboxCovering();
        if (existing.isPresent()) {
            return declareExisting(existing.orElseThrow(), schema, geometryColumns);
        }
        return resolveRequested(options, schema, geometryColumns);
    }

    /**
     * Declares a covering over a struct already in {@code schema}. The geometry column must be one of the configured
     * geometry columns. The four paths must name the fields of one top-level struct, and the struct must meet the
     * {@link BoundingBoxColumn} rules. The struct is written with its fields in the spec order, and batches are not
     * augmented.
     */
    private static BboxCoveringPlan declareExisting(
            ExistingBboxCovering existing, ParquetSchema schema, List<ColumnPath> geometryColumns) {
        ColumnPath geometryColumn = ColumnPath.parse(existing.geometryColumn());
        if (!geometryColumns.contains(geometryColumn)) {
            throw new ParquetWriteException("existing bbox covering names '" + existing.geometryColumn()
                    + "' as its geometry column, but no CRS is configured for that column");
        }
        CoveringPaths paths = new CoveringPaths(
                ColumnPath.parse(existing.xmin()),
                ColumnPath.parse(existing.ymin()),
                ColumnPath.parse(existing.xmax()),
                ColumnPath.parse(existing.ymax()));
        String struct = declaredStruct(paths);
        Optional<String> defect = BoundingBoxColumn.defect(schema, struct, geometryColumn);
        if (defect.isPresent()) {
            throw new ParquetWriteException("cannot declare the existing bbox covering: " + defect.orElseThrow());
        }
        ParquetSchema writtenSchema = BoundingBoxColumn.inSpecOrder(schema, struct);
        return new BboxCoveringPlan(true, false, writtenSchema, geometryColumn, paths, false);
    }

    /** The top-level struct named by the four paths of a declared covering. */
    private static String declaredStruct(CoveringPaths paths) {
        BboxCovering declared = new BboxCovering(
                paths.xmin(), paths.xmax(), paths.ymin(), paths.ymax(), Optional.empty(), Optional.empty());
        Optional<String> struct = BoundingBoxColumn.columnNamedBy(declared);
        if (struct.isEmpty()) {
            throw new ParquetWriteException("cannot declare the existing bbox covering: its paths must name the "
                    + "fields xmin, ymin, xmax and ymax of one top-level struct");
        }
        return struct.orElseThrow();
    }

    /**
     * Resolves the covering requested by {@link WriteOptions#bboxCovering()}: a derived {@code bbox} group, or the
     * {@code bbox} column already in the schema.
     */
    private static BboxCoveringPlan resolveRequested(
            WriteOptions options, ParquetSchema schema, List<ColumnPath> geometryColumns) {
        Optional<CoveringMode> requested = options.bboxCovering();
        boolean explicit = requested.isPresent() && requested.orElseThrow() != CoveringMode.NONE;

        if (requested.isPresent() && requested.orElseThrow() == CoveringMode.NONE) {
            return inactive(schema);
        }
        if (geometryColumns.isEmpty()) {
            return requireOrDowngrade(explicit, schema, "bbox covering requires a geometry column; none is configured");
        }
        ColumnPath geometryColumn = geometryColumns.get(0);
        if (schema.find(ColumnPath.of(BBOX)).isPresent()) {
            return resolveOverBboxColumn(requested, schema, geometryColumn);
        }
        boolean useFloat = resolvePrecision(options, requested, geometryColumn);
        Repetition repetition = schema.find(geometryColumn).orElseThrow().repetition();
        ParquetSchema writtenSchema = schema.withAppendedGroup(coveringGroup(useFloat, repetition));
        return new BboxCoveringPlan(true, true, writtenSchema, geometryColumn, bboxStructPaths(), useFloat);
    }

    /**
     * Resolves a covering request over a schema that already has a {@code bbox} column. AUTO declares that column as
     * the covering when it meets the {@link BoundingBoxColumn} rules, and writes its fields in the order required by
     * the spec. GeoParquet 1.0 writers such as DuckDB write this struct without declaring it, and DuckDB puts its
     * fields in another order. FLOAT and DOUBLE ask for derived values, and a derived {@code bbox} would clash with the
     * existing column.
     */
    private static BboxCoveringPlan resolveOverBboxColumn(
            Optional<CoveringMode> requested, ParquetSchema schema, ColumnPath geometryColumn) {
        Optional<String> defect = BoundingBoxColumn.defect(schema, BBOX, geometryColumn);
        boolean autoRequested = requested.orElse(CoveringMode.AUTO) == CoveringMode.AUTO;
        if (!autoRequested) {
            throw new ParquetWriteException(precisionClashMessage(defect.isEmpty()));
        }
        if (defect.isPresent()) {
            String message = "cannot declare the 'bbox' column as the bbox covering: " + defect.orElseThrow();
            return requireOrDowngrade(requested.isPresent(), schema, message);
        }
        ParquetSchema writtenSchema = BoundingBoxColumn.inSpecOrder(schema, BBOX);
        return new BboxCoveringPlan(true, false, writtenSchema, geometryColumn, bboxStructPaths(), false);
    }

    private static String precisionClashMessage(boolean bboxStructUsable) {
        String message = "cannot derive a bbox covering: the schema already has a column named 'bbox'";
        if (bboxStructUsable) {
            return message + "; request AUTO to declare that column as the covering";
        }
        return message;
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

    /**
     * Whether float ordinates hold enough precision for the column. A column with no configured CRS defaults to CRS84,
     * hence a missing entry is compatible. An unknown CRS is not: its data may be in metres, where float loses
     * precision.
     */
    private static boolean isCrs84Compatible(ColumnCrs declared) {
        if (declared == null) {
            return true;
        }
        Optional<CoordinateReferenceSystem> crs = declared.projjson();
        if (crs.isEmpty()) {
            return declared instanceof ColumnCrs.Defaulted;
        }
        boolean epsg4326 =
                crs.get().id().map(id -> id.epsgCode().orElse(-1) == 4326).orElse(false);
        return epsg4326 || crs.get().equals(CoordinateReferenceSystems.ogcCrs84());
    }

    /**
     * The derived {@code bbox} group, shaped as a GeoParquet bounding box column: the four fields in the spec order,
     * and the repetition of the geometry column. The fields stay OPTIONAL to hold the nulls of an empty geometry.
     */
    private static SchemaNode.Group coveringGroup(boolean useFloat, Repetition geometryRepetition) {
        PrimitiveKind kind = useFloat ? PrimitiveKind.FLOAT : PrimitiveKind.DOUBLE;
        List<SchemaNode> leaves = List.of(
                coveringLeaf("xmin", kind),
                coveringLeaf("ymin", kind),
                coveringLeaf("xmax", kind),
                coveringLeaf("ymax", kind));
        return new SchemaNode.Group(BBOX, geometryRepetition, leaves, Optional.empty(), -1);
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
        return new BboxCoveringPlan(false, false, schema, null, bboxStructPaths(), false);
    }

    public boolean active() {
        return active;
    }

    /** Whether the plan adds a derived {@code bbox} group to the written schema and to every batch. */
    public boolean derivesColumns() {
        return derived;
    }

    public ParquetSchema writtenSchema() {
        return writtenSchema;
    }

    public ColumnPath geometryColumn() {
        return geometryColumn;
    }

    public ColumnPath xmin() {
        return paths.xmin();
    }

    public ColumnPath xmax() {
        return paths.xmax();
    }

    public ColumnPath ymin() {
        return paths.ymin();
    }

    public ColumnPath ymax() {
        return paths.ymax();
    }

    /**
     * Returns {@code raw} with the derived {@code bbox} struct added; returns {@code raw} unchanged when inactive or
     * when the covering is declared over existing columns.
     */
    public ParquetRecordBatch augment(ParquetRecordBatch raw) {
        if (!derived) {
            return raw;
        }
        BinaryVector geometry = (BinaryVector) raw.columns().get(geometryColumn);
        StructVector bbox = BboxCoveringDeriver.derive(geometry, raw.rowCount(), useFloat);
        Map<ColumnPath, ColumnVector> augmented = new LinkedHashMap<>(raw.columns());
        augmented.put(ColumnPath.of(BBOX), bbox);
        return AugmentedRecordBatch.of(raw, writtenSchema, augmented);
    }
}
