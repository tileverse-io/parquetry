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
package io.tileverse.parquetry.geotools.export;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.feature.type.GeometryDescriptor;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.data.nested.NestedType;
import org.geotools.referencing.CRS;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

/**
 * Inverts a GeoTools {@link SimpleFeatureType} into the Parquet write schema and per-attribute plan a feature writer
 * needs to encode rows. This is the write-side mirror of {@code FeatureTypeMapper} (in
 * {@code io.tileverse.parquetry.geotools.data}): where that class turns a Parquet schema into a feature type, this
 * class turns a feature type back into a Parquet schema.
 *
 * <p>Each top-level attribute becomes one leaf or nested column, all {@link Repetition#OPTIONAL}: a geometry attribute
 * becomes a {@code BYTE_ARRAY} column with no logical type (WKB-encoded on write) and, when its coordinate reference
 * system resolves to an EPSG code, an entry in {@link #geometryEpsg()}; a scalar attribute becomes a primitive leaf
 * whose physical kind and logical-type annotation follow the Java binding; a {@link List} or {@link Map} attribute is a
 * nested attribute, converted through {@link NestedSchemaNodes} from its recorded {@link NestedType}.
 */
record FeatureWriteSchema(
        SimpleFeatureType featureType,
        ParquetSchema schema,
        List<WriteAttribute> attributes,
        Optional<GeoParquetMetadata> geoMetadata,
        Map<String, Integer> geometryEpsg) {

    /**
     * One attribute of the source feature type mapped to its write-time plan.
     *
     * @param featureIndex the attribute's index in the source {@link SimpleFeatureType}
     * @param path the Parquet column path this attribute writes to
     * @param binding the attribute's Java binding class
     * @param geometry true when this attribute is a geometry attribute of the feature type; a feature type can declare
     *     more than one, and this flag is set for every one of them, not only the default geometry
     * @param nestedType the recorded {@link NestedType} shape for a {@code List}/{@code Map} attribute; empty for a
     *     scalar or geometry attribute
     */
    record WriteAttribute(
            int featureIndex, ColumnPath path, Class<?> binding, boolean geometry, Optional<NestedType> nestedType) {}

    private static final Map<Class<?>, ScalarShape> SCALAR_SHAPES = buildScalarShapes();

    /** Builds the write schema and per-attribute plan for {@code featureType}. */
    static FeatureWriteSchema of(SimpleFeatureType featureType) {
        List<SchemaNode> children = new ArrayList<>();
        List<WriteAttribute> attributes = new ArrayList<>();
        Map<String, Integer> geometryEpsg = new LinkedHashMap<>();
        for (int i = 0; i < featureType.getAttributeCount(); i++) {
            AttributeDescriptor descriptor = featureType.getDescriptor(i);
            String name = descriptor.getLocalName();
            Class<?> binding = descriptor.getType().getBinding();
            if (descriptor instanceof GeometryDescriptor geometryDescriptor) {
                children.add(binaryLeaf(name));
                lookupEpsg(geometryDescriptor.getCoordinateReferenceSystem())
                        .ifPresent(code -> geometryEpsg.put(name, code));
                attributes.add(new WriteAttribute(i, ColumnPath.of(name), binding, true, Optional.empty()));
            } else if (List.class.isAssignableFrom(binding) || Map.class.isAssignableFrom(binding)) {
                NestedType nestedType = requireNestedType(descriptor);
                children.add(NestedSchemaNodes.toGroup(name, nestedType));
                attributes.add(new WriteAttribute(i, ColumnPath.of(name), binding, false, Optional.of(nestedType)));
            } else {
                children.add(scalarLeaf(name, binding, descriptor));
                attributes.add(new WriteAttribute(i, ColumnPath.of(name), binding, false, Optional.empty()));
            }
        }
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);
        Optional<GeoParquetMetadata> geo = geoMetadata(featureType, attributes, geometryEpsg);
        return new FeatureWriteSchema(featureType, schema, List.copyOf(attributes), geo, Map.copyOf(geometryEpsg));
    }

    /** A {@code BYTE_ARRAY} leaf with no logical type, the write-side encoding for a WKB geometry column. */
    private static SchemaNode.Primitive binaryLeaf(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    /** Resolves the EPSG code of a geometry attribute's CRS, or empty when it has none or none resolves. */
    private static OptionalInt lookupEpsg(CoordinateReferenceSystem crs) {
        if (crs == null) {
            return OptionalInt.empty();
        }
        try {
            Integer code = CRS.lookupEpsgCode(crs, true);
            return code == null ? OptionalInt.empty() : OptionalInt.of(code);
        } catch (FactoryException e) {
            return OptionalInt.empty();
        }
    }

    /**
     * The {@link NestedType} recorded on {@code descriptor}'s user data. Without it, the shape of a {@code List}/
     * {@code Map} attribute is unknowable, which is a caller error.
     */
    private static NestedType requireNestedType(AttributeDescriptor descriptor) {
        String name = descriptor.getLocalName();
        Object nestedType = descriptor.getUserData().get(NestedType.USER_DATA_KEY);
        if (nestedType == null) {
            throw new IllegalArgumentException("attribute '" + name
                    + "' is a List/Map attribute with no recorded NestedType user data; its element type is"
                    + " unknowable");
        }
        return (NestedType) nestedType;
    }

    /** A scalar leaf whose physical kind and logical-type annotation follow {@code binding}. */
    private static SchemaNode.Primitive scalarLeaf(String name, Class<?> binding, AttributeDescriptor descriptor) {
        ScalarShape shape = SCALAR_SHAPES.get(binding);
        if (shape == null) {
            throw new IllegalArgumentException(
                    "attribute '" + descriptor.getLocalName() + "' has an unsupported binding " + binding.getName());
        }
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, shape.kind(), shape.typeLength(), shape.logicalType(), -1);
    }

    /**
     * The GeoParquet metadata for {@code featureType}, one {@link GeoColumn} per geometry attribute, or empty when the
     * feature type has no geometry attribute.
     */
    private static Optional<GeoParquetMetadata> geoMetadata(
            SimpleFeatureType featureType, List<WriteAttribute> attributes, Map<String, Integer> geometryEpsg) {
        List<WriteAttribute> geometryAttributes =
                attributes.stream().filter(WriteAttribute::geometry).toList();
        if (geometryAttributes.isEmpty()) {
            return Optional.empty();
        }
        Map<String, GeoColumn> columns = new LinkedHashMap<>();
        for (WriteAttribute attribute : geometryAttributes) {
            String columnName = attribute.path().dot();
            columns.put(columnName, geoColumn(columnName, geometryEpsg));
        }
        String primaryColumn = primaryGeometryColumn(featureType, geometryAttributes);
        return Optional.of(new GeoParquetMetadata.V1_1("1.1.0", primaryColumn, columns));
    }

    /**
     * The GeoParquet primary geometry column name: the feature type's default geometry when it has one, otherwise the
     * first geometry attribute encountered.
     */
    private static String primaryGeometryColumn(
            SimpleFeatureType featureType, List<WriteAttribute> geometryAttributes) {
        GeometryDescriptor defaultGeometry = featureType.getGeometryDescriptor();
        if (defaultGeometry != null) {
            return defaultGeometry.getLocalName();
        }
        return geometryAttributes.get(0).path().dot();
    }

    /**
     * The {@link GeoColumn} for a geometry column: WKB encoding, an unknown geometry-types list (GeoParquet semantics
     * for "not declared"), and the PROJJSON CRS resolved from its EPSG code when one was found.
     */
    private static GeoColumn geoColumn(String columnName, Map<String, Integer> geometryEpsg) {
        Integer epsg = geometryEpsg.get(columnName);
        Optional<io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem> crs =
                epsg == null ? Optional.empty() : CoordinateReferenceSystems.forEpsg(epsg);
        return GeoColumn.builder()
                .encoding(Optional.of("WKB"))
                .geometryTypes(List.of())
                .crs(crs)
                .build();
    }

    /** The physical kind, byte length, and logical-type annotation for one scalar Java binding. */
    private record ScalarShape(PrimitiveKind kind, OptionalInt typeLength, Optional<LogicalType> logicalType) {

        private static ScalarShape plain(PrimitiveKind kind) {
            return new ScalarShape(kind, OptionalInt.empty(), Optional.empty());
        }

        private static ScalarShape annotated(PrimitiveKind kind, LogicalType logicalType) {
            return new ScalarShape(kind, OptionalInt.empty(), Optional.of(logicalType));
        }
    }

    /**
     * The scalar binding table this class inverts from {@code FeatureTypeMapper#resolveBinding} (in
     * {@code io.tileverse.parquetry.geotools.data}): every Java type a scalar attribute may bind to, mapped to its
     * Parquet physical kind and logical-type annotation. {@code Short} and {@code Byte} widen to {@code INT32};
     * {@link BigInteger} widens to {@code INT64}; {@link BigDecimal} widens (lossily) to {@code DOUBLE}.
     */
    private static Map<Class<?>, ScalarShape> buildScalarShapes() {
        Map<Class<?>, ScalarShape> shapes = new LinkedHashMap<>();
        shapes.put(String.class, ScalarShape.annotated(PrimitiveKind.BYTE_ARRAY, new LogicalType.StringType()));
        shapes.put(Integer.class, ScalarShape.plain(PrimitiveKind.INT32));
        shapes.put(Short.class, ScalarShape.plain(PrimitiveKind.INT32));
        shapes.put(Byte.class, ScalarShape.plain(PrimitiveKind.INT32));
        shapes.put(Long.class, ScalarShape.plain(PrimitiveKind.INT64));
        shapes.put(BigInteger.class, ScalarShape.plain(PrimitiveKind.INT64));
        shapes.put(Float.class, ScalarShape.plain(PrimitiveKind.FLOAT));
        shapes.put(Double.class, ScalarShape.plain(PrimitiveKind.DOUBLE));
        shapes.put(BigDecimal.class, ScalarShape.plain(PrimitiveKind.DOUBLE));
        shapes.put(Boolean.class, ScalarShape.plain(PrimitiveKind.BOOLEAN));
        shapes.put(byte[].class, ScalarShape.plain(PrimitiveKind.BYTE_ARRAY));
        shapes.put(
                UUID.class,
                new ScalarShape(
                        PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                        OptionalInt.of(16),
                        Optional.of(new LogicalType.UuidType())));
        shapes.put(LocalDate.class, ScalarShape.annotated(PrimitiveKind.INT32, new LogicalType.DateType()));
        shapes.put(java.sql.Date.class, ScalarShape.annotated(PrimitiveKind.INT32, new LogicalType.DateType()));
        shapes.put(
                Instant.class,
                ScalarShape.annotated(PrimitiveKind.INT64, new LogicalType.Timestamp(true, TimeUnit.MICROS)));
        shapes.put(
                Date.class,
                ScalarShape.annotated(PrimitiveKind.INT64, new LogicalType.Timestamp(true, TimeUnit.MICROS)));
        shapes.put(
                java.sql.Timestamp.class,
                ScalarShape.annotated(PrimitiveKind.INT64, new LogicalType.Timestamp(true, TimeUnit.MICROS)));
        shapes.put(
                LocalDateTime.class,
                ScalarShape.annotated(PrimitiveKind.INT64, new LogicalType.Timestamp(false, TimeUnit.MICROS)));
        return Map.copyOf(shapes);
    }
}
