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

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.ParquetCrs;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;
import io.tileverse.parquetry.schema.geo.projjson.ProjJsonModule;

import lombok.NonNull;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.json.JsonMapper;

/**
 * Emits GeoParquet metadata in the two flavours parquetry supports.
 *
 * <p>The v1.1 flavour is a JSON document keyed under {@code "geo"} in the footer's {@code key_value_metadata}; the v2.0
 * flavour is a {@link LogicalType.Geometry} / {@link LogicalType.Geography} annotation on the geometry column's
 * primitive leaf. Mode {@link GeoParquetMetadataMode#DUAL_V1_1_AND_V2_0} emits both for maximum reader compatibility;
 * {@link GeoParquetMetadataMode#V1_1_ONLY} emits only the JSON; {@link GeoParquetMetadataMode#V2_0_ONLY} emits only the
 * logical-type annotations.
 *
 * <p>The set of geometry columns is taken from {@link WriteOptions#crs()}: every column path with a CRS configured is
 * treated as a geometry column. The writer does not infer geometry-ness from the schema's existing logical types -- the
 * user opts in by configuring a CRS for that column.
 *
 * <p>{@link #applyV2LogicalTypes} returns a copy of the schema with {@link LogicalType.Geometry} attached to every
 * configured column, carrying its typed PROJJSON CRS (the spec default OGC:CRS84 is implied by an empty CRS, so we
 * always include the CRS explicitly so downstream readers see a complete annotation).
 *
 * <p>{@link #v1JsonPayload} produces the JSON document that lands as the value side of the {@code "geo"} KV entry. The
 * shape mirrors GeoParquet 1.1: a {@code "version"} field, a {@code "primary_column"} pointing at the first geometry
 * column in schema order, and a {@code "columns"} object with one entry per geometry column carrying the WKB encoding,
 * the typed CRS as PROJJSON, the list of WKB geometry-type names, the bounding box, and {@code "edges": "planar"} by
 * default. The bbox is omitted when the per-column summary recorded no observations.
 */
public final class GeoMetadataWriter {

    /** GeoParquet 1.1 spec version embedded in the JSON document. */
    private static final String GEOPARQUET_VERSION = "1.1.0";

    /** Default edge interpolation for {@link LogicalType.Geometry} columns under GeoParquet 1.1. */
    private static final String PLANAR_EDGES = "planar";

    private static final JsonMapper MAPPER =
            JsonMapper.builder().addModule(new ProjJsonModule()).build();

    private final WriteOptions options;

    public GeoMetadataWriter(@NonNull WriteOptions options) {
        this.options = options;
    }

    /**
     * Returns a schema with {@link LogicalType.Geometry} applied to every column path configured in
     * {@link WriteOptions#crs()}.
     *
     * <p>Returns the input schema unchanged when {@link WriteOptions#geoParquetMetadata()} is
     * {@link GeoParquetMetadataMode#V1_1_ONLY} or no geometry columns are configured: the v1.1 KV blob carries the CRS
     * on its own, and emitting redundant logical types would mislead a v1-only reader into thinking the file is a v2
     * native GeoParquet.
     */
    public ParquetSchema applyV2LogicalTypes(@NonNull ParquetSchema schema) {
        if (options.geoParquetMetadata() == GeoParquetMetadataMode.V1_1_ONLY) {
            return schema;
        }
        Map<String, CoordinateReferenceSystem> crsConfig = options.crs();
        if (crsConfig.isEmpty()) {
            return schema;
        }
        Map<ColumnPath, LogicalType> overrides = LinkedHashMap.newLinkedHashMap(crsConfig.size());
        for (Map.Entry<String, CoordinateReferenceSystem> entry : crsConfig.entrySet()) {
            ColumnPath path = parseDotted(entry.getKey());
            overrides.put(path, new LogicalType.Geometry(Optional.of(ParquetCrs.inline(entry.getValue()))));
        }
        return schema.withLogicalTypes(overrides);
    }

    private static ColumnPath parseDotted(String dotted) {
        if (dotted.isEmpty()) {
            throw new ParquetWriteException("Empty column path in WriteOptions.crs()");
        }
        return ColumnPath.parse(dotted);
    }

    /**
     * Returns the v1.1 KV JSON payload with no covering declaration. Equivalent to {@link #v1JsonPayload(ParquetSchema,
     * Map, Optional)} with an empty covering.
     */
    public Optional<String> v1JsonPayload(
            @NonNull ParquetSchema schema, @NonNull Map<ColumnPath, GeoColumnSummary> perColumnSummaries) {
        return v1JsonPayload(schema, perColumnSummaries, Optional.empty());
    }

    /**
     * Returns the v1.1 KV JSON payload, or {@link Optional#empty()} when the configured mode is
     * {@link GeoParquetMetadataMode#V2_0_ONLY} or no geometry columns are configured.
     *
     * @param schema the schema as it appears in the footer (after {@link #applyV2LogicalTypes}); only used to determine
     *     which configured CRS columns are present and to fix the {@code primary_column} to the first geometry leaf in
     *     schema order
     * @param perColumnSummaries dataset-level aggregates per configured geometry column; missing entries fall back to
     *     an empty summary (bbox absent, no geometry types observed)
     * @param covering the active bbox covering plan, present when the write derives a covering for its geometry column;
     *     empty when no covering is written. The declaration is emitted only on the plan's geometry column.
     */
    public Optional<String> v1JsonPayload(
            @NonNull ParquetSchema schema,
            @NonNull Map<ColumnPath, GeoColumnSummary> perColumnSummaries,
            @NonNull Optional<BboxCoveringPlan> covering) {
        if (options.geoParquetMetadata() == GeoParquetMetadataMode.V2_0_ONLY) {
            return Optional.empty();
        }
        List<ColumnPath> geoColumns = orderedGeoColumns(schema);
        if (geoColumns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(renderJson(geoColumns, perColumnSummaries, covering));
    }

    private List<ColumnPath> orderedGeoColumns(ParquetSchema schema) {
        Set<String> configured = options.crs().keySet();
        List<ColumnPath> ordered = new ArrayList<>();
        for (ColumnPath leaf : schema.leafColumns()) {
            if (configured.contains(leaf.dot())) {
                ordered.add(leaf);
            }
        }
        return ordered;
    }

    private String renderJson(
            List<ColumnPath> geoColumns,
            Map<ColumnPath, GeoColumnSummary> summaries,
            Optional<BboxCoveringPlan> covering) {
        StringWriter sink = new StringWriter();
        try (JsonGenerator gen = MAPPER.createGenerator(sink)) {
            gen.writeStartObject();
            gen.writeStringProperty("version", GEOPARQUET_VERSION);
            gen.writeStringProperty("primary_column", primaryColumnName(geoColumns.get(0)));

            gen.writeName("columns");
            gen.writeStartObject();
            for (ColumnPath column : geoColumns) {
                gen.writeName(primaryColumnName(column));
                writeColumnObject(gen, column, summaries.get(column), covering);
            }
            gen.writeEndObject();

            gen.writeEndObject();
        }
        return sink.toString();
    }

    private void writeColumnObject(
            JsonGenerator gen, ColumnPath column, GeoColumnSummary summary, Optional<BboxCoveringPlan> covering) {
        gen.writeStartObject();
        String encoding = summary == null ? "WKB" : summary.encoding();
        gen.writeStringProperty("encoding", encoding);

        gen.writeName("geometry_types");
        gen.writeStartArray();
        for (String name : geometryTypeNames(summary)) {
            gen.writeString(name);
        }
        gen.writeEndArray();

        CoordinateReferenceSystem crs = options.crs().get(column.dot());
        gen.writeName("crs");
        if (crs == null) {
            crs = CoordinateReferenceSystems.ogcCrs84();
        }
        gen.writePOJO(crs);

        gen.writeStringProperty("edges", PLANAR_EDGES);

        Optional<BoundingBox> bbox = summary == null ? Optional.empty() : summary.bbox();
        if (bbox.isPresent()) {
            writeBboxArray(gen, bbox.get());
        }
        if (coveringAppliesTo(covering, column)) {
            writeCovering(gen, covering.orElseThrow());
        }
        gen.writeEndObject();
    }

    private static boolean coveringAppliesTo(Optional<BboxCoveringPlan> covering, ColumnPath column) {
        return covering.isPresent() && covering.orElseThrow().geometryColumn().equals(column);
    }

    /**
     * Declares the derived {@code bbox} covering per GeoParquet 1.1: a {@code covering.bbox} object mapping each of the
     * four extrema to the two-part path of its sidecar column. The spec order is xmin, ymin, xmax, ymax.
     */
    private static void writeCovering(JsonGenerator gen, BboxCoveringPlan plan) {
        gen.writeName("covering");
        gen.writeStartObject();
        gen.writeName("bbox");
        gen.writeStartObject();
        writePathArray(gen, "xmin", plan.xmin());
        writePathArray(gen, "ymin", plan.ymin());
        writePathArray(gen, "xmax", plan.xmax());
        writePathArray(gen, "ymax", plan.ymax());
        gen.writeEndObject();
        gen.writeEndObject();
    }

    private static void writePathArray(JsonGenerator gen, String key, ColumnPath path) {
        gen.writeName(key);
        gen.writeStartArray();
        for (int part = 0; part < path.numParts(); part++) {
            gen.writeString(path.part(part));
        }
        gen.writeEndArray();
    }

    /**
     * The GeoParquet spec keys columns by their dotted path. A future nested-schema use case will need this; for the
     * flat-schema flavour the writer supports today, the dotted form is just the leaf name.
     */
    private static String primaryColumnName(ColumnPath path) {
        return path.dot();
    }

    private static List<String> geometryTypeNames(GeoColumnSummary summary) {
        if (summary == null || summary.geometryTypeCodes().isEmpty()) {
            return List.of();
        }
        Set<String> names = new TreeSet<>();
        for (Integer code : summary.geometryTypeCodes()) {
            names.add(wkbTypeName(code));
        }
        return new ArrayList<>(names);
    }

    /**
     * Maps a WKB type code into the GeoParquet spec's display name. Codes {@code 1..7} are the seven base kinds; the
     * {@code +1000 / +2000 / +3000} prefixes signal Z, M, and ZM variants per the WKB extension convention shared by
     * PostGIS and the parquet-format Geospatial specification.
     */
    private static String wkbTypeName(int code) {
        int base = code;
        String suffix = "";
        if (base >= 3000) {
            base -= 3000;
            suffix = " ZM";
        } else if (base >= 2000) {
            base -= 2000;
            suffix = " M";
        } else if (base >= 1000) {
            base -= 1000;
            suffix = " Z";
        }
        return baseGeometryName(base) + suffix;
    }

    private static String baseGeometryName(int base) {
        return switch (base) {
            case 1 -> "Point";
            case 2 -> "LineString";
            case 3 -> "Polygon";
            case 4 -> "MultiPoint";
            case 5 -> "MultiLineString";
            case 6 -> "MultiPolygon";
            case 7 -> "GeometryCollection";
            default -> "Unknown(" + base + ")";
        };
    }

    private static void writeBboxArray(JsonGenerator gen, BoundingBox bbox) {
        gen.writeName("bbox");
        gen.writeStartArray();
        gen.writeNumber(bbox.xmin());
        gen.writeNumber(bbox.ymin());
        if (bbox.zmin().isPresent()) {
            gen.writeNumber(bbox.zmin().getAsDouble());
        }
        gen.writeNumber(bbox.xmax());
        gen.writeNumber(bbox.ymax());
        if (bbox.zmax().isPresent()) {
            gen.writeNumber(bbox.zmax().getAsDouble());
        }
        gen.writeEndArray();
    }

    /** Returns the dotted column paths the writer recognises as geometry columns for the supplied schema. */
    public List<ColumnPath> geometryColumns(ParquetSchema schema) {
        return List.copyOf(orderedGeoColumns(schema));
    }

    /** Returns the {@link GeoParquetMetadataMode} this writer was constructed with. */
    public GeoParquetMetadataMode mode() {
        return options.geoParquetMetadata();
    }
}
