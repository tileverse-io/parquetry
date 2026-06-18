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
package io.tileverse.parquetry.schema.geo.geoparquet;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.ProjJsonModule;

import tools.jackson.databind.json.JsonMapper;

/**
 * Typed GeoParquet {@code "geo"} key-value metadata. One of three concrete record permits per the GeoParquet spec
 * versions in circulation:
 *
 * <ul>
 *   <li>{@link V1_0} - 1.0.0 (oldest live producers; no {@code covering}).
 *   <li>{@link V1_1} - 1.1.0 and the 1.1.0+p1 errata variant (adds {@code covering}, {@code orientation},
 *       {@code epoch}).
 *   <li>{@link V2} - 2.0.0-dev and 2.0.0; native logical-type companion (still carries {@code geometry_types},
 *       {@code bbox}, {@code covering} per column).
 * </ul>
 *
 * <p>Polymorphic dispatch is driven by the JSON {@code "version"} field via {@link GeoParquetMetadataDeserializer}.
 * Unknown versions parse as {@link V2} (the newest known shape).
 *
 * @see <a href="https://github.com/opengeospatial/geoparquet/blob/main/format-specs/metadata.md">GeoParquet Metadata
 *     Specification</a>
 */
@SuppressWarnings("java:S101") // V1_0 / V1_1 mirror the GeoParquet spec version strings; underscore is intentional.
public sealed interface GeoParquetMetadata
        permits GeoParquetMetadata.V1_0, GeoParquetMetadata.V1_1, GeoParquetMetadata.V2 {

    String version();

    String primaryColumn();

    Map<String, GeoColumn> columns();

    /** Parses a GeoParquet {@code "geo"} JSON document into the typed ADT. */
    static GeoParquetMetadata parse(String geoJson) {
        GeoParquetMetadata metadata = DEFAULT_MAPPER.readValue(geoJson, GeoParquetMetadata.class);
        rejectTypelessColumnCrs(metadata);
        return metadata;
    }

    /**
     * Rejects a column whose {@code crs} is JSON but not valid PROJJSON because it omits the required {@code "type"}. A
     * missing type is legal in a nested PROJJSON position (a {@code base_crs} may omit it), but a column's top-level
     * CRS may not; the deserializer marks that case as a {@link CoordinateReferenceSystem.Unknown} with an empty
     * {@code rawType} (distinct from a forward-compat unknown discriminator, which keeps its name).
     */
    private static void rejectTypelessColumnCrs(GeoParquetMetadata metadata) {
        metadata.columns()
                .forEach((columnName, column) -> column.crs().ifPresent(crs -> {
                    if (crs instanceof CoordinateReferenceSystem.Unknown unknown
                            && unknown.rawType().isEmpty()) {
                        throw new MalformedFileException("GeoParquet column \"" + columnName
                                + "\" has a crs that is not valid PROJJSON: the required \"type\" field is missing");
                    }
                }));
    }

    /** GeoParquet 1.0.0 metadata. {@code covering} and {@code epoch} are not part of this spec generation. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record V1_0(
            String version, @JsonProperty("primary_column") String primaryColumn, Map<String, GeoColumn> columns)
            implements GeoParquetMetadata {

        public V1_0 {
            columns = columns == null ? Map.of() : Map.copyOf(columns);
        }
    }

    /** GeoParquet 1.1.0 (and 1.1.0+p1) metadata. Adds {@code covering}, {@code orientation}, {@code epoch}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record V1_1(
            String version, @JsonProperty("primary_column") String primaryColumn, Map<String, GeoColumn> columns)
            implements GeoParquetMetadata {

        public V1_1 {
            columns = columns == null ? Map.of() : Map.copyOf(columns);
        }
    }

    /**
     * GeoParquet 2.0-dev / 2.0 metadata. The native GEOMETRY / GEOGRAPHY logical-type annotations on individual columns
     * carry the CRS and algorithm; the {@code "geo"} JSON parsed here still carries {@code geometry_types},
     * {@code bbox}, and {@code covering} per column for downstream consumers.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record V2(
            String version, @JsonProperty("primary_column") String primaryColumn, Map<String, GeoColumn> columns)
            implements GeoParquetMetadata {

        public V2 {
            columns = columns == null ? Map.of() : Map.copyOf(columns);
        }
    }

    /**
     * Default mapper used by {@link #parse(String)}. Both {@link ProjJsonModule} and {@link GeoParquetModule} are
     * wired.
     */
    JsonMapper DEFAULT_MAPPER = JsonMapper.builder()
            .addModule(new ProjJsonModule())
            .addModule(new GeoParquetModule())
            .build();
}
