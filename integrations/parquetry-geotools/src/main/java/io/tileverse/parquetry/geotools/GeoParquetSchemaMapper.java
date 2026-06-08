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
package io.tileverse.parquetry.geotools;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.Covering;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.projjson.Identifier;

/**
 * Maps a GeoParquet {@link ParquetSchema} to a GeoTools {@link SimpleFeatureType} plus a reader-facing attribute
 * mapping.
 *
 * <p>Primitive leaves become typed attribute descriptors; geometry leaves (GEOMETRY or GEOGRAPHY logical type) become
 * geometry attributes with a resolved CRS. REPEATED leaves and primitive kinds without a natural Java binding are
 * silently skipped. Nested group paths flatten to dotted attribute names (e.g. {@code "address.city"}).
 *
 * <p>The GeoParquet primary column (from the {@code "geo"} key-value metadata) becomes the default geometry on the
 * resulting feature type; if no primary column is declared, the first geometry column found takes that role.
 */
final class GeoParquetSchemaMapper {

    /** Default feature id column name, used when no column is explicitly configured. */
    private static final String DEFAULT_FID_COLUMN = "id";

    private GeoParquetSchemaMapper() {}

    /**
     * One mapped attribute: its feature-type local name, the parquet leaf path it reads from, whether it is a geometry
     * attribute, and the Java binding class.
     */
    record AttributeMapping(String name, ColumnPath path, boolean geometry, Class<?> binding) {}

    /**
     * The result of a schema mapping: the built feature type, the ordered list of attribute mappings, and the attribute
     * that provides the feature id ({@link Optional#empty()} when none could be resolved, in which case the reader
     * falls back to a synthetic per-read feature id).
     */
    record Mapping(
            SimpleFeatureType featureType,
            List<AttributeMapping> attributes,
            Optional<AttributeMapping> fidAttribute,
            Map<ColumnPath, Integer> geometrySrids) {

        /** Convenience for the common case of a mapping with no feature id column. */
        Mapping(SimpleFeatureType featureType, List<AttributeMapping> attributes) {
            this(featureType, attributes, Optional.empty(), Map.of());
        }

        /** Convenience for a mapping with a feature id but no resolved geometry SRIDs. */
        Mapping(
                SimpleFeatureType featureType,
                List<AttributeMapping> attributes,
                Optional<AttributeMapping> fidAttribute) {
            this(featureType, attributes, fidAttribute, Map.of());
        }
    }

    /**
     * Maps the given schema and key-value metadata to a {@link Mapping}.
     *
     * @param typeName local name for the resulting feature type
     * @param namespaceUri optional namespace URI; may be null
     * @param schema the parquet schema from the file footer
     * @param kvMetadata file-level key-value metadata (used to extract the GeoParquet {@code "geo"} block)
     * @param configuredFidColumn the column to use as the feature id, or null to auto-detect a column named
     *     {@code "id"}
     * @return the completed mapping
     */
    static Mapping map(
            String typeName,
            String namespaceUri,
            ParquetSchema schema,
            Map<String, String> kvMetadata,
            String configuredFidColumn) {
        Optional<GeoParquetMetadata> geo = parseGeoMetadata(kvMetadata);
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

        for (ColumnPath path : schema.leafColumns()) {
            if (coveringColumns.contains(path)) {
                continue;
            }
            SchemaNode.Primitive primitive = asMappablePrimitive(schema, path);
            if (primitive == null) {
                continue;
            }
            Optional<AttributeMapping> mapped = mapLeaf(path, primitive, geo, ftb);
            if (mapped.isEmpty()) {
                continue;
            }
            AttributeMapping attr = mapped.get();
            attributes.add(attr);
            if (attr.geometry()) {
                if (isDefaultGeometry(attr.name(), primaryGeometryColumn, defaultGeometryName)) {
                    defaultGeometryName = attr.name();
                }
                resolveSrid(path, primitive.logicalType(), geo).ifPresent(srid -> geometrySrids.put(path, srid));
            }
        }

        if (defaultGeometryName != null) {
            ftb.setDefaultGeometry(defaultGeometryName);
        }
        List<AttributeMapping> mappedAttributes = List.copyOf(attributes);
        Optional<AttributeMapping> fid = resolveFidAttribute(typeName, configuredFidColumn, mappedAttributes);
        return new Mapping(ftb.buildFeatureType(), mappedAttributes, fid, Map.copyOf(geometrySrids));
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
                .filter(GeoParquetSchemaMapper::isUsableFid)
                .findFirst();
    }

    private static AttributeMapping requireUsableFid(
            String typeName, String column, List<AttributeMapping> attributes) {
        return attributes.stream()
                .filter(attr -> attr.name().equals(column))
                .filter(GeoParquetSchemaMapper::isUsableFid)
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
                || binding == Double.class;
    }

    /**
     * Returns the leaf as a non-repeated {@link SchemaNode.Primitive}, or {@code null} when the path is absent, refers
     * to a group node, or is a repeated field (which cannot be represented as a flat attribute).
     */
    private static SchemaNode.Primitive asMappablePrimitive(ParquetSchema schema, ColumnPath path) {
        Optional<SchemaNode> nodeOpt = schema.find(path);
        if (nodeOpt.isEmpty()) {
            return null;
        }
        if (!(nodeOpt.get() instanceof SchemaNode.Primitive primitive)) {
            return null;
        }
        if (primitive.repetition() == Repetition.REPEATED) {
            // Repeated fields map to lists - not representable as a flat SimpleFeatureType attribute.
            return null;
        }
        return primitive;
    }

    /**
     * Maps a single primitive leaf to an {@link AttributeMapping} and registers the corresponding descriptor on the
     * builder. Returns empty when the leaf has no natural Java binding and should be skipped.
     */
    private static Optional<AttributeMapping> mapLeaf(
            ColumnPath path,
            SchemaNode.Primitive primitive,
            Optional<GeoParquetMetadata> geo,
            SimpleFeatureTypeBuilder ftb) {
        String attrName = path.dot();
        Optional<LogicalType> logical = primitive.logicalType();
        if (isGeometryType(logical)) {
            return Optional.of(mapGeometryLeaf(path, attrName, logical, geo, ftb));
        }
        return mapPrimitiveLeaf(path, attrName, primitive.kind(), logical, ftb);
    }

    /** Adds a geometry attribute descriptor and returns its mapping. */
    private static AttributeMapping mapGeometryLeaf(
            ColumnPath path,
            String attrName,
            Optional<LogicalType> logical,
            Optional<GeoParquetMetadata> geo,
            SimpleFeatureTypeBuilder ftb) {
        CoordinateReferenceSystem crs = resolveCrs(path, logical, geo);
        ftb.add(attrName, Geometry.class, crs);
        return new AttributeMapping(attrName, path, true, Geometry.class);
    }

    /**
     * Adds a non-geometry attribute descriptor and returns its mapping, or empty when the kind has no natural Java
     * binding.
     */
    private static Optional<AttributeMapping> mapPrimitiveLeaf(
            ColumnPath path,
            String attrName,
            PrimitiveKind kind,
            Optional<LogicalType> logical,
            SimpleFeatureTypeBuilder ftb) {
        Optional<Class<?>> binding = resolveBinding(kind, logical);
        if (binding.isEmpty()) {
            return Optional.empty();
        }
        ftb.add(attrName, binding.get());
        return Optional.of(new AttributeMapping(attrName, path, false, binding.get()));
    }

    /**
     * Returns true when {@code attrName} should become the default geometry. A column is the default when it matches
     * the declared primary column, or - when no primary is declared - it is the first geometry column encountered.
     */
    private static boolean isDefaultGeometry(String attrName, String primaryGeometryColumn, String currentDefault) {
        return attrName.equals(primaryGeometryColumn) || (primaryGeometryColumn == null && currentDefault == null);
    }

    /**
     * Parses the GeoParquet {@code "geo"} JSON from the file's key-value metadata. Returns empty when the key is absent
     * or the JSON is malformed.
     */
    private static Optional<GeoParquetMetadata> parseGeoMetadata(Map<String, String> kvMetadata) {
        String geoJson = kvMetadata.get("geo");
        if (geoJson == null || geoJson.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(GeoParquetMetadata.parse(geoJson));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
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
        Optional<io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem> fromLogical =
                logical.flatMap(lt -> extractCrsFromLogicalType(lt));
        if (fromLogical.isPresent()) {
            return ProjJsonCrsConverter.toGeoTools(fromLogical);
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
        Optional<io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem> crs =
                logical.flatMap(GeoParquetSchemaMapper::extractCrsFromLogicalType);
        if (crs.isEmpty()) {
            crs = geo.flatMap(g -> Optional.ofNullable(g.columns().get(path.dot())))
                    .flatMap(GeoColumn::crs);
        }
        return crs.flatMap(io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem::id)
                .map(Identifier::epsgCode)
                .orElse(OptionalInt.empty());
    }

    /**
     * Extracts the CRS from a GEOMETRY or GEOGRAPHY logical type. Returns empty for any other logical type. This method
     * is only called after {@link #isGeometryType} has returned true; the other-type case should not arise.
     */
    private static Optional<io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem>
            extractCrsFromLogicalType(LogicalType lt) {
        if (lt
                instanceof
                LogicalType.Geometry(
                        Optional<io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem> crs)) {
            return crs;
        }
        if (lt
                instanceof
                LogicalType.Geography(
                        Optional<io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem> crs,
                        var ignoredAlgorithm)) {
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
    private static Optional<Class<?>> resolveBinding(PrimitiveKind kind, Optional<LogicalType> logical) {
        boolean isString =
                logical.map(GeoParquetSchemaMapper::isStringLogicalType).orElse(false);
        return switch (kind) {
            case BOOLEAN -> Optional.of(Boolean.class);
            case INT32 -> Optional.of(Integer.class);
            case INT64 -> Optional.of(Long.class);
            case FLOAT -> Optional.of(Float.class);
            case DOUBLE -> Optional.of(Double.class);
            case BYTE_ARRAY -> Optional.of(isString ? String.class : byte[].class);
            case FIXED_LEN_BYTE_ARRAY -> Optional.of(byte[].class);
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
