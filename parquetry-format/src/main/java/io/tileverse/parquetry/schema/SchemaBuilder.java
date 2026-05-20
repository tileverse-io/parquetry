/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Converts the depth-first flat {@code List<SchemaElement>} from
 * {@link io.tileverse.parquetry.format.FileMetaData#schema()} into the nested {@link ParquetSchema} tree.
 *
 * <p>The Parquet Thrift wire format stores the schema as a pre-order traversal of the tree: the first element is always
 * the root group, followed by its children recursively. Group elements carry {@code numChildren > 0}; primitive (leaf)
 * elements have {@code type} present and no {@code numChildren}.
 *
 * <p>A single mutable {@link Cursor} tracks the current position in the list and is threaded through recursive calls.
 * Each call to {@link #consumeNext} advances the cursor by one (the current element) plus however many descendants it
 * owns.
 *
 * <p>The two-argument overload {@link #build(List, Map)} additionally folds in GeoParquet 1.x's {@code "geo"} key-value
 * metadata: WKB columns identified by the JSON gain a synthesized {@link LogicalType.Geometry} or
 * {@link LogicalType.Geography} annotation so downstream consumers see one uniform schema shape regardless of file
 * version. Native (2.0) annotations always win over the {@code "geo"} JSON when both are present.
 */
public final class SchemaBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaBuilder.class);
    private static final String GEO_KEY = "geo";

    private SchemaBuilder() {}

    /**
     * Builds a {@link ParquetSchema} from the depth-first flattened schema element list.
     *
     * @param elements the flat list from {@link io.tileverse.parquetry.format.FileMetaData#schema()}
     * @return the reconstructed schema tree
     * @throws IllegalArgumentException if {@code elements} is empty or the root is not a group
     */
    public static ParquetSchema build(List<SchemaElement> elements) {
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("ParquetSchema elements list is empty");
        }
        Cursor cursor = new Cursor(elements, 0);
        Field rootField = consumeNext(cursor);
        if (!(rootField instanceof Field.Group rootGroup)) {
            throw new IllegalArgumentException("ParquetSchema root must be a group, got: "
                    + rootField.getClass().getSimpleName());
        }
        return new ParquetSchema(rootGroup);
    }

    /**
     * Builds a {@link ParquetSchema} and folds in GeoParquet {@code "geo"} key-value metadata.
     *
     * <p>For each column named in the JSON's {@code columns} map whose schema leaf has no native logical-type
     * annotation, this synthesizes a {@link LogicalType.Geometry} (default / {@code "planar"} edges) or
     * {@link LogicalType.Geography} ({@code "spherical"} edges) so downstream code looks at one shape regardless of
     * file version. Native annotations always win; when both are present and disagree, a WARN is logged and the native
     * value is kept.
     *
     * <p>Missing, empty, or malformed {@code "geo"} metadata leaves the schema unchanged and logs at WARN. Parquet
     * files with no geometry columns must not be rejected.
     *
     * @param elements depth-first flat schema element list
     * @param keyValueMetadata collapsed file-level key/value metadata (see
     *     {@link io.tileverse.parquetry.format.FileMetaData#keyValueMetadata()})
     */
    public static ParquetSchema build(List<SchemaElement> elements, Map<String, String> keyValueMetadata) {
        ParquetSchema schema = build(elements);
        return synthesizeGeoLogicalTypes(schema, keyValueMetadata);
    }

    /**
     * Returns {@code schema} with each WKB column named in the {@code "geo"} JSON rewritten to carry a native
     * {@link LogicalType.Geometry} / {@link LogicalType.Geography} annotation, unless the column already has one. The
     * single-argument {@link #build(List)} path skips this step entirely; this method is the GeoParquet-specific seam.
     */
    static ParquetSchema synthesizeGeoLogicalTypes(ParquetSchema schema, Map<String, String> keyValueMetadata) {
        String geoJson = keyValueMetadata.get(GEO_KEY);
        if (geoJson == null || geoJson.isEmpty()) {
            return schema;
        }
        GeoParquetMetadata metadata;
        try {
            metadata = GeoParquetMetadata.parse(geoJson);
        } catch (RuntimeException e) {
            LOG.warn("Malformed GeoParquet 'geo' metadata; leaving schema unchanged: {}", e.getMessage());
            return schema;
        }
        Map<ColumnPath, LogicalType> overrides = new HashMap<>();
        metadata.columns().forEach((columnName, geoColumn) -> recordOverride(schema, columnName, geoColumn, overrides));
        return schema.withLogicalTypes(overrides);
    }

    /**
     * Picks the {@link LogicalType} this {@code GeoColumn} would synthesize and records it under {@code path}, unless
     * the leaf already carries a native Geometry / Geography annotation. When the native annotation disagrees with what
     * {@code "geo"} would produce, logs a WARN so the stale {@code "geo"} entry doesn't silently lie - native still
     * wins.
     */
    private static void recordOverride(
            ParquetSchema schema, String columnName, GeoColumn geoColumn, Map<ColumnPath, LogicalType> overrides) {
        ColumnPath path = ColumnPath.of(columnName);
        Optional<Field> field = schema.find(path);
        if (field.isEmpty()) {
            LOG.debug("'geo' references unknown column '{}'; skipping", columnName);
            return;
        }
        LogicalType derived = deriveLogicalType(geoColumn);
        Optional<LogicalType> existing = nativeGeoAnnotation(field.get());
        if (existing.isPresent()) {
            if (!derived.equals(existing.get())) {
                LOG.warn(
                        "Column '{}' has native annotation {} but 'geo' KV metadata says {}; native wins.",
                        columnName,
                        existing.get(),
                        derived);
            }
            return;
        }
        overrides.put(path, derived);
    }

    /**
     * Translates one GeoParquet column entry into a {@link LogicalType.Geometry} or {@link LogicalType.Geography}.
     * GeoParquet 1.x's {@code "edges"} value ({@code "planar"} / {@code "spherical"}) maps to Geometry / Geography
     * respectively, mirroring the GeoParquet 2.0 logical-type split. GeoParquet 1.x carries no Geography
     * {@code algorithm}, so Geography always surfaces with {@code Optional.empty()} there (consumer falls back to the
     * spec default {@code SPHERICAL}).
     */
    private static LogicalType deriveLogicalType(GeoColumn geoColumn) {
        String edges = geoColumn.edges().orElse("planar");
        if ("spherical".equalsIgnoreCase(edges)) {
            return new LogicalType.Geography(geoColumn.crs(), Optional.<EdgeInterpolationAlgorithm>empty());
        }
        return new LogicalType.Geometry(geoColumn.crs());
    }

    /**
     * Returns the leaf's native Geometry / Geography logical type when present. Non-primitive fields and primitives
     * with any other (or no) logical-type annotation surface as {@link Optional#empty()}.
     */
    private static Optional<LogicalType> nativeGeoAnnotation(Field field) {
        if (!(field instanceof Field.Primitive primitive)) {
            return Optional.empty();
        }
        return primitive.logicalType().filter(SchemaBuilder::isGeoAnnotation);
    }

    private static boolean isGeoAnnotation(LogicalType lt) {
        return lt instanceof LogicalType.Geometry || lt instanceof LogicalType.Geography;
    }

    /**
     * Consumes the next element (and all its descendants) from {@code cursor}, returning the corresponding
     * {@link Field}.
     *
     * <p>After this method returns, {@code cursor.position} points to the element immediately after the subtree rooted
     * at the consumed element.
     */
    private static Field consumeNext(Cursor cursor) {
        SchemaElement element = cursor.elements.get(cursor.position);
        cursor.position++;

        Repetition repetition = mapRepetition(element.repetitionType());
        int fieldId = element.fieldId().orElse(-1);

        if (element.numChildren().isPresent()) {
            return consumeGroup(cursor, element, repetition, fieldId);
        }
        return consumePrimitive(element, repetition, fieldId);
    }

    private static Field.Group consumeGroup(Cursor cursor, SchemaElement element, Repetition repetition, int fieldId) {
        int childCount = element.numChildren().getAsInt();
        List<Field> children = new ArrayList<>(childCount);
        for (int i = 0; i < childCount; i++) {
            children.add(consumeNext(cursor));
        }
        return new Field.Group(element.name(), repetition, children, element.logicalType(), fieldId);
    }

    private static Field.Primitive consumePrimitive(SchemaElement element, Repetition repetition, int fieldId) {
        PhysicalType type = element.type()
                .orElseThrow(() -> new IllegalStateException("Leaf SchemaElement missing type: " + element.name()));
        PrimitiveKind kind = mapKind(type);
        OptionalInt typeLength = element.typeLength();
        return new Field.Primitive(element.name(), repetition, kind, typeLength, element.logicalType(), fieldId);
    }

    private static Repetition mapRepetition(Optional<FieldRepetitionType> rep) {
        // Thrift spec: missing repetition type defaults to REQUIRED
        return switch (rep.orElse(FieldRepetitionType.REQUIRED)) {
            case REQUIRED -> Repetition.REQUIRED;
            case OPTIONAL -> Repetition.OPTIONAL;
            case REPEATED -> Repetition.REPEATED;
        };
    }

    private static PrimitiveKind mapKind(PhysicalType type) {
        return switch (type) {
            case BOOLEAN -> PrimitiveKind.BOOLEAN;
            case INT32 -> PrimitiveKind.INT32;
            case INT64 -> PrimitiveKind.INT64;
            case INT96 -> PrimitiveKind.INT96;
            case FLOAT -> PrimitiveKind.FLOAT;
            case DOUBLE -> PrimitiveKind.DOUBLE;
            case BYTE_ARRAY -> PrimitiveKind.BYTE_ARRAY;
            case FIXED_LEN_BYTE_ARRAY -> PrimitiveKind.FIXED_LEN_BYTE_ARRAY;
        };
    }

    /**
     * Mutable position pointer threaded through recursive calls to track where we are in the flat schema element list.
     * Using a small wrapper object is cleaner than returning index values from each recursive call.
     */
    private static final class Cursor {
        final List<SchemaElement> elements;
        int position;

        Cursor(List<SchemaElement> elements, int position) {
            this.elements = elements;
            this.position = position;
        }
    }
}
