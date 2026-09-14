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
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

/**
 * Resolves the bbox covering of a write: either a covering declared over leaves already in the schema, or a derived
 * {@code bbox} group for the primary geometry column at a chosen precision, added to each batch. An unset
 * {@link WriteOptions#bboxCovering()} resolves to AUTO when the file emits GeoParquet 1.1 metadata and to NONE under
 * {@link GeoParquetMetadataMode#V2_0_ONLY}; a coherence problem is an error for an explicit request and a downgrade to
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

    private static CoveringPaths derivedPaths() {
        return new CoveringPaths(
                ColumnPath.of(BBOX, "xmin"),
                ColumnPath.of(BBOX, "ymin"),
                ColumnPath.of(BBOX, "xmax"),
                ColumnPath.of(BBOX, "ymax"));
    }

    public static BboxCoveringPlan resolve(WriteOptions options, ParquetSchema schema, GeoMetadataWriter geoWriter) {
        List<ColumnPath> geometryColumns = geoWriter.geometryColumns(schema);
        Optional<ExistingBboxCovering> existing = options.existingBboxCovering();
        if (existing.isPresent()) {
            return declareExisting(existing.orElseThrow(), schema, geometryColumns);
        }
        return resolveDerived(options, schema, geometryColumns);
    }

    /**
     * Declares a covering over leaves already in {@code schema}. The geometry column must be one of the configured
     * geometry columns and each of the four paths must resolve to a FLOAT or DOUBLE leaf; the schema is written as-is
     * and batches are not augmented.
     */
    private static BboxCoveringPlan declareExisting(
            ExistingBboxCovering existing, ParquetSchema schema, List<ColumnPath> geometryColumns) {
        ColumnPath geometryColumn = ColumnPath.parse(existing.geometryColumn());
        if (!geometryColumns.contains(geometryColumn)) {
            throw new ParquetWriteException("existing bbox covering names '" + existing.geometryColumn()
                    + "' as its geometry column, but no CRS is configured for that column");
        }
        CoveringPaths paths = new CoveringPaths(
                numericLeaf(schema, existing.xmin()),
                numericLeaf(schema, existing.ymin()),
                numericLeaf(schema, existing.xmax()),
                numericLeaf(schema, existing.ymax()));
        return new BboxCoveringPlan(true, false, schema, geometryColumn, paths, false);
    }

    private static ColumnPath numericLeaf(ParquetSchema schema, String dotted) {
        ColumnPath path = ColumnPath.parse(dotted);
        Optional<SchemaNode> node = schema.find(path);
        if (node.isEmpty()) {
            throw new ParquetWriteException(
                    "existing bbox covering names '" + dotted + "', which is not in the schema");
        }
        if (!(node.orElseThrow() instanceof SchemaNode.Primitive leaf) || !isFloatOrDouble(leaf.kind())) {
            throw new ParquetWriteException(
                    "existing bbox covering names '" + dotted + "', which is not a FLOAT or DOUBLE leaf");
        }
        return path;
    }

    private static boolean isFloatOrDouble(PrimitiveKind kind) {
        return kind == PrimitiveKind.FLOAT || kind == PrimitiveKind.DOUBLE;
    }

    private static BboxCoveringPlan resolveDerived(
            WriteOptions options, ParquetSchema schema, List<ColumnPath> geometryColumns) {
        Optional<CoveringMode> requested = options.bboxCovering();
        boolean explicit = requested.isPresent() && requested.orElseThrow() != CoveringMode.NONE;

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
        return new BboxCoveringPlan(true, true, writtenSchema, geometryColumn, derivedPaths(), useFloat);
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
        return new BboxCoveringPlan(false, false, schema, null, derivedPaths(), false);
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
