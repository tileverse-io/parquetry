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
package io.tileverse.parquetry.geotools.data;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.data.nested.NestedType;
import org.geotools.data.nested.NestedType.ListType;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.ParquetCrs;
import io.tileverse.parquetry.schema.geo.geoparquet.Covering;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.projjson.Identifier;

/**
 * Maps a GeoParquet {@link ParquetSchema} to a GeoTools {@link SimpleFeatureType} plus a reader-facing attribute
 * mapping.
 *
 * <p>Each top-level field becomes one attribute. A primitive field becomes a typed scalar attribute; a geometry field
 * (GEOMETRY or GEOGRAPHY logical type) becomes a geometry attribute with a resolved CRS; a {@code STRUCT}/{@code LIST}/
 * {@code MAP} field becomes a single nested attribute whose value-object shape is recorded as a {@link NestedType} in
 * the descriptor's user data (under {@link NestedType#USER_DATA_KEY}). Nested fields are not flattened into dotted
 * attribute names. Primitive kinds without a natural Java binding are skipped.
 *
 * <p>The GeoParquet primary column (from the {@code "geo"} key-value metadata) becomes the default geometry on the
 * resulting feature type; if no primary column is declared, the first geometry column found takes that role.
 */
final class FeatureTypeMapper {

    /** Default feature id column name, used when no column is explicitly configured. */
    private static final String DEFAULT_FID_COLUMN = "id";

    private FeatureTypeMapper() {}

    /**
     * One mapped attribute: its feature-type local name, the parquet path it reads from, whether it is a geometry
     * attribute, the Java binding class, and - for {@code STRUCT}/{@code LIST}/{@code MAP} attributes - the nested
     * value-object shape ({@code null} for scalar and geometry attributes).
     */
    record AttributeMapping(String name, ColumnPath path, boolean geometry, Class<?> binding, NestedType nestedType) {

        /** Convenience for a scalar or geometry attribute, which has no nested type. */
        AttributeMapping(String name, ColumnPath path, boolean geometry, Class<?> binding) {
            this(name, path, geometry, binding, null);
        }
    }

    /**
     * The result of a schema mapping: the built feature type, the source schema (for resolving nested property paths
     * during filter push-down), the ordered list of attribute mappings, the attribute that provides the feature id
     * ({@link Optional#empty()} when none could be resolved, in which case the reader falls back to a synthetic
     * per-read feature id), and the resolved geometry SRIDs.
     */
    record Mapping(
            SimpleFeatureType featureType,
            ParquetSchema schema,
            List<AttributeMapping> attributes,
            Optional<AttributeMapping> fidAttribute,
            Map<ColumnPath, Integer> geometrySrids) {

        /** Convenience for the common case of a mapping with no source schema and no feature id column. */
        Mapping(SimpleFeatureType featureType, List<AttributeMapping> attributes) {
            this(featureType, null, attributes, Optional.empty(), Map.of());
        }

        /** Convenience for a mapping with a feature id but no source schema or resolved geometry SRIDs. */
        Mapping(
                SimpleFeatureType featureType,
                List<AttributeMapping> attributes,
                Optional<AttributeMapping> fidAttribute) {
            this(featureType, null, attributes, fidAttribute, Map.of());
        }
    }

    /**
     * Maps the given schema and GeoParquet metadata to a {@link Mapping}.
     *
     * @param typeName local name for the resulting feature type
     * @param namespaceUri optional namespace URI; may be null
     * @param schema the parquet schema from the file footer
     * @param geo the dataset's aggregated GeoParquet metadata; empty yields a geometryless feature type
     * @param configuredFidColumn the column to use as the feature id, or null to auto-detect a column named
     *     {@code "id"}
     * @return the completed mapping
     */
    static Mapping map(
            String typeName,
            String namespaceUri,
            ParquetSchema schema,
            Optional<GeoParquetMetadata> geo,
            String configuredFidColumn) {
        String primaryGeometryColumn =
                geo.map(GeoParquetMetadata::primaryColumn).orElse(null);

        SimpleFeatureTypeBuilder ftb = new SimpleFeatureTypeBuilder();
        ftb.setName(typeName);
        if (namespaceUri != null) {
            ftb.setNamespaceURI(namespaceUri);
        }

        List<AttributeMapping> attributes = new ArrayList<>();
        Map<ColumnPath, Integer> geometrySrids = new LinkedHashMap<>();
        String defaultGeometryName = null;
        Set<ColumnPath> coveringColumns = coveringColumns(geo);

        for (SchemaNode child : schema.root().children()) {
            Optional<AttributeMapping> mapped = mapTopLevelField(schema, child, geo, coveringColumns, ftb);
            if (mapped.isEmpty()) {
                continue;
            }
            AttributeMapping attr = mapped.get();
            attributes.add(attr);
            if (attr.geometry()) {
                if (isDefaultGeometry(attr.name(), primaryGeometryColumn, defaultGeometryName)) {
                    defaultGeometryName = attr.name();
                }
                Optional<LogicalType> logical = ((SchemaNode.Primitive) child).logicalType();
                resolveSrid(attr.path(), logical, geo).ifPresent(srid -> geometrySrids.put(attr.path(), srid));
            }
        }

        if (defaultGeometryName != null) {
            ftb.setDefaultGeometry(defaultGeometryName);
        }
        List<AttributeMapping> mappedAttributes = List.copyOf(attributes);
        Optional<AttributeMapping> fid = resolveFidAttribute(typeName, configuredFidColumn, mappedAttributes);
        return new Mapping(ftb.buildFeatureType(), schema, mappedAttributes, fid, Map.copyOf(geometrySrids));
    }

    /**
     * Maps a single top-level field to an {@link AttributeMapping} and registers its descriptor on the builder. A
     * primitive field becomes a geometry or scalar attribute; a group becomes a nested attribute unless it is the
     * GeoParquet bbox covering struct, which is skipped. Returns empty when the field has no natural binding.
     */
    private static Optional<AttributeMapping> mapTopLevelField(
            ParquetSchema schema,
            SchemaNode child,
            Optional<GeoParquetMetadata> geo,
            Set<ColumnPath> coveringColumns,
            SimpleFeatureTypeBuilder ftb) {
        ColumnPath path = ColumnPath.of(child.name());
        if (child instanceof SchemaNode.Primitive primitive) {
            return mapPrimitiveField(path, primitive, geo, ftb);
        }
        SchemaNode.Group group = (SchemaNode.Group) child;
        if (isCoveringStruct(schema, group, coveringColumns)) {
            return Optional.empty();
        }
        return Optional.of(mapNestedField(path, group, ftb));
    }

    /** Maps a top-level primitive field to a geometry attribute or a scalar attribute. */
    private static Optional<AttributeMapping> mapPrimitiveField(
            ColumnPath path,
            SchemaNode.Primitive primitive,
            Optional<GeoParquetMetadata> geo,
            SimpleFeatureTypeBuilder ftb) {
        Optional<LogicalType> logical = primitive.logicalType();
        if (isGeometryType(logical)) {
            return Optional.of(mapGeometryField(path, logical, geo, ftb));
        }
        return mapScalarField(path, primitive.kind(), logical, ftb);
    }

    /** Adds a geometry attribute descriptor and returns its mapping. */
    private static AttributeMapping mapGeometryField(
            ColumnPath path,
            Optional<LogicalType> logical,
            Optional<GeoParquetMetadata> geo,
            SimpleFeatureTypeBuilder ftb) {
        String attrName = path.dot();
        CoordinateReferenceSystem crs = resolveCrs(path, logical, geo);
        ftb.add(attrName, Geometry.class, crs);
        return new AttributeMapping(attrName, path, true, Geometry.class);
    }

    /** Adds a scalar attribute descriptor and returns its mapping, or empty when the kind has no natural binding. */
    private static Optional<AttributeMapping> mapScalarField(
            ColumnPath path, PrimitiveKind kind, Optional<LogicalType> logical, SimpleFeatureTypeBuilder ftb) {
        Optional<Class<?>> binding = resolveBinding(kind, logical);
        if (binding.isEmpty()) {
            return Optional.empty();
        }
        String attrName = path.dot();
        ftb.add(attrName, binding.get());
        return Optional.of(new AttributeMapping(attrName, path, false, binding.get()));
    }

    /**
     * Adds a nested ({@code STRUCT}/{@code LIST}/{@code MAP}) attribute descriptor and returns its mapping. A list
     * binds to {@link List}; a struct or map binds to {@link Map} (a struct value presents as a map keyed by field
     * name). The {@link NestedType} is recorded in the descriptor user data for the reader and the filter push-down.
     */
    private static AttributeMapping mapNestedField(
            ColumnPath path, SchemaNode.Group group, SimpleFeatureTypeBuilder ftb) {
        NestedType nestedType = NestedTypes.of(group);
        Class<?> binding = nestedBinding(nestedType);
        String attrName = path.dot();
        ftb.userData(NestedType.USER_DATA_KEY, nestedType);
        ftb.add(attrName, binding);
        return new AttributeMapping(attrName, path, false, binding, nestedType);
    }

    private static Class<?> nestedBinding(NestedType nestedType) {
        if (nestedType instanceof ListType) {
            return List.class;
        }
        return Map.class;
    }

    /**
     * True when {@code group} is a GeoParquet bbox covering struct: a top-level group all of whose primitive leaf
     * descendants are declared covering columns. Such a group holds per-row bounding boxes for spatial pruning, not
     * user data, and must not appear as a feature attribute.
     */
    private static boolean isCoveringStruct(
            ParquetSchema schema, SchemaNode.Group group, Set<ColumnPath> coveringColumns) {
        if (coveringColumns.isEmpty()) {
            return false;
        }
        List<ColumnPath> leaves = leafDescendants(schema, group.name());
        if (leaves.isEmpty()) {
            return false;
        }
        return coveringColumns.containsAll(leaves);
    }

    /** The leaf column paths under the top-level group named {@code groupName}. */
    private static List<ColumnPath> leafDescendants(ParquetSchema schema, String groupName) {
        List<ColumnPath> leaves = new ArrayList<>();
        for (ColumnPath leaf : schema.leafColumns()) {
            if (leaf.numParts() > 0 && leaf.part(0).equals(groupName)) {
                leaves.add(leaf);
            }
        }
        return leaves;
    }

    /**
     * Collects the bbox covering sidecar columns declared in the GeoParquet metadata. These columns hold each row's
     * bounding box for spatial pruning, not user data; keeping them out of the feature type stops them showing up as
     * attributes in WFS DescribeFeatureType. The spatial filter pushdown still reads them directly from the file.
     */
    private static Set<ColumnPath> coveringColumns(Optional<GeoParquetMetadata> geo) {
        if (geo.isEmpty()) {
            return Set.of();
        }
        Set<ColumnPath> columns = new HashSet<>();
        for (GeoColumn column : geo.orElseThrow().columns().values()) {
            column.covering().map(Covering::bbox).ifPresent(bbox -> {
                columns.add(bbox.xmin());
                columns.add(bbox.xmax());
                columns.add(bbox.ymin());
                columns.add(bbox.ymax());
                bbox.zmin().ifPresent(columns::add);
                bbox.zmax().ifPresent(columns::add);
            });
        }
        return columns;
    }

    /**
     * Resolves which attribute provides the feature id.
     *
     * <p>An explicitly configured column name must match a usable attribute, or it is a configuration error. With no
     * explicit name, an attribute literally named {@code "id"} is used when present and usable. When neither resolves,
     * the result is empty and the reader falls back to a synthetic per-read feature id.
     */
    static Optional<AttributeMapping> resolveFidAttribute(
            String typeName, String configuredColumn, List<AttributeMapping> attributes) {
        if (configuredColumn != null && !configuredColumn.isBlank()) {
            return Optional.of(requireUsableFid(typeName, configuredColumn, attributes));
        }
        return attributes.stream()
                .filter(attr -> attr.name().equals(DEFAULT_FID_COLUMN))
                .filter(FeatureTypeMapper::isUsableFid)
                .findFirst();
    }

    private static AttributeMapping requireUsableFid(
            String typeName, String column, List<AttributeMapping> attributes) {
        return attributes.stream()
                .filter(attr -> attr.name().equals(column))
                .filter(FeatureTypeMapper::isUsableFid)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "feature id column '%s' is not a usable (non-geometry, string or numeric) attribute of feature type '%s'"
                                .formatted(column, typeName)));
    }

    /** A usable feature id attribute is non-geometry and binds to a string or numeric type that stringifies cleanly. */
    private static boolean isUsableFid(AttributeMapping attr) {
        if (attr.geometry()) {
            return false;
        }
        Class<?> binding = attr.binding();
        return binding == String.class
                || binding == Integer.class
                || binding == Long.class
                || binding == Float.class
                || binding == Double.class
                || binding == UUID.class;
    }

    /**
     * Returns true when {@code attrName} should become the default geometry. A column is the default when it matches
     * the declared primary column, or - when no primary is declared - it is the first geometry column encountered.
     */
    private static boolean isDefaultGeometry(String attrName, String primaryGeometryColumn, String currentDefault) {
        return attrName.equals(primaryGeometryColumn) || (primaryGeometryColumn == null && currentDefault == null);
    }

    /** Returns true when the logical type is a GEOMETRY or GEOGRAPHY annotation (GeoParquet 2.0 native types). */
    private static boolean isGeometryType(Optional<LogicalType> logical) {
        return logical.map(lt -> lt instanceof LogicalType.Geometry || lt instanceof LogicalType.Geography)
                .orElse(false);
    }

    /**
     * Resolves the GeoTools CRS for a geometry column.
     *
     * <p>Resolution order: (1) CRS embedded in the native GEOMETRY/GEOGRAPHY logical-type annotation; (2) CRS from the
     * GeoParquet 1.x {@code "geo"} column metadata; (3) WGS84 default (handled by {@link ProjJsonCrsConverter}).
     */
    private static CoordinateReferenceSystem resolveCrs(
            ColumnPath path, Optional<LogicalType> logical, Optional<GeoParquetMetadata> geo) {
        Optional<ParquetCrs> fromLogical = logical.flatMap(FeatureTypeMapper::extractCrsFromLogicalType);
        if (fromLogical.isPresent()) {
            return ProjJsonCrsConverter.toGeoTools(fromLogical.get());
        }
        Optional<io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem> fromMetadata = geo.flatMap(
                        g -> Optional.ofNullable(g.columns().get(path.dot())))
                .flatMap(GeoColumn::crs);
        return ProjJsonCrsConverter.toGeoTools(fromMetadata);
    }

    /**
     * Resolves the EPSG SRID for a geometry column, to stamp on each decoded JTS geometry. Resolution mirrors
     * {@link #resolveCrs}: the native GEOMETRY/GEOGRAPHY logical-type CRS first, then the GeoParquet 1.x {@code "geo"}
     * column CRS. Empty when no CRS resolves an EPSG code (the geometry keeps JTS's default SRID 0).
     */
    private static OptionalInt resolveSrid(
            ColumnPath path, Optional<LogicalType> logical, Optional<GeoParquetMetadata> geo) {
        Optional<ParquetCrs> fromLogical = logical.flatMap(FeatureTypeMapper::extractCrsFromLogicalType);
        if (fromLogical.isPresent()) {
            return fromLogical.get().epsgCode();
        }
        return geo.flatMap(g -> Optional.ofNullable(g.columns().get(path.dot())))
                .flatMap(GeoColumn::crs)
                .flatMap(io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem::id)
                .map(Identifier::epsgCode)
                .orElse(OptionalInt.empty());
    }

    /**
     * Extracts the native CRS from a GEOMETRY or GEOGRAPHY logical type. Returns empty for any other logical type. This
     * method is only called after {@link #isGeometryType} has returned true; the other-type case should not arise.
     */
    private static Optional<ParquetCrs> extractCrsFromLogicalType(LogicalType lt) {
        if (lt instanceof LogicalType.Geometry(Optional<ParquetCrs> crs)) {
            return crs;
        }
        if (lt instanceof LogicalType.Geography(Optional<ParquetCrs> crs, var _)) {
            return crs;
        }
        return Optional.empty();
    }

    /**
     * Maps a Parquet primitive kind and optional logical type to a Java binding class.
     *
     * <p>Returns empty for {@code INT96} (a deprecated Impala timestamp format with no clean Java mapping) and for any
     * other kind where the logical type does not clarify the intended Java type.
     */
    static Optional<Class<?>> resolveBinding(PrimitiveKind kind, Optional<LogicalType> logical) {
        boolean isString = logical.map(FeatureTypeMapper::isStringLogicalType).orElse(false);
        boolean isUuid = logical.map(lt -> lt instanceof LogicalType.UuidType).orElse(false);
        boolean isDate = logical.map(lt -> lt instanceof LogicalType.DateType).orElse(false);
        return switch (kind) {
            case BOOLEAN -> Optional.of(Boolean.class);
            case INT32 -> Optional.of(isDate ? Date.class : Integer.class);
            case INT64 -> Optional.of(Long.class);
            case FLOAT -> Optional.of(Float.class);
            case DOUBLE -> Optional.of(Double.class);
            case BYTE_ARRAY -> Optional.of(isString ? String.class : byte[].class);
            case FIXED_LEN_BYTE_ARRAY -> Optional.of(isUuid ? UUID.class : byte[].class);
            // INT96 is a deprecated Impala timestamp with no standard Java binding; skip it.
            case INT96 -> Optional.empty();
        };
    }

    /** Returns true when the logical type annotation indicates the physical bytes encode a UTF-8 string. */
    private static boolean isStringLogicalType(LogicalType lt) {
        return lt instanceof LogicalType.StringType
                || lt instanceof LogicalType.EnumType
                || lt instanceof LogicalType.JsonType
                || lt instanceof LogicalType.BsonType;
    }
}
