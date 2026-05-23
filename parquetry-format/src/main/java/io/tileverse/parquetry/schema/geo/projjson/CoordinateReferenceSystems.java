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
package io.tileverse.parquetry.schema.geo.projjson;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import tools.jackson.databind.json.JsonMapper;

/**
 * Convenience facade over {@link CoordinateReferenceSystem} for the parquetry write path and for downstream callers
 * that need a typed CRS without owning a Jackson mapper.
 *
 * <p>Supports three entry points:
 *
 * <ul>
 *   <li>{@link #forEpsg(int)} -- look up a typed CRS from the bundled EPSG resource set. The bundle is curated rather
 *       than exhaustive; common cases (WGS 84, ETRS89, NAD83, Web Mercator, World Mercator, the 120 UTM zones) ship in
 *       v1. Misses return {@link Optional#empty()}.
 *   <li>{@link #fromProjjson(String)} / {@link #toProjjson(CoordinateReferenceSystem)} -- string round-trip through the
 *       bundled {@link ProjJsonModule}.
 *   <li>{@link #ogcCrs84()} -- the GeoParquet 1.1 implicit default for geometry columns with no CRS specified.
 * </ul>
 *
 * <p>{@link #forEpsg(int)} caches both positive and negative results, so repeat lookups are O(1) and do not retry
 * resource I/O.
 */
public final class CoordinateReferenceSystems {

    private static final String EPSG_RESOURCE_PREFIX = "/io/tileverse/parquetry/schema/geo/projjson/epsg/EPSG_";

    @SuppressWarnings("java:S1075") // bundled classpath resource, not a configurable URI
    private static final String OGC_RESOURCE_PATH = "/io/tileverse/parquetry/schema/geo/projjson/ogc/CRS84.json";

    private static final JsonMapper MAPPER =
            JsonMapper.builder().addModule(new ProjJsonModule()).build();

    /**
     * Cache of EPSG-code lookups. Both hits and misses memoise: a missing resource maps to {@link Optional#empty()} so
     * repeated misses do not re-hit the classpath.
     */
    private static final ConcurrentMap<Integer, Optional<CoordinateReferenceSystem>> EPSG_CACHE =
            new ConcurrentHashMap<>();

    /** Lazy holder so the OGC:CRS84 resource is loaded on first use rather than at class init. */
    private static final class OgcCrs84Holder {
        static final CoordinateReferenceSystem INSTANCE = readClasspathResource(OGC_RESOURCE_PATH)
                .orElseThrow(() -> new IllegalStateException("classpath:" + OGC_RESOURCE_PATH + " is missing"));
    }

    private CoordinateReferenceSystems() {}

    /**
     * Returns the typed CRS for the given EPSG code from the bundled resource set, or {@link Optional#empty()} if the
     * code is not in the bundle. Both positive and negative results are memoised.
     */
    public static Optional<CoordinateReferenceSystem> forEpsg(int code) {
        return EPSG_CACHE.computeIfAbsent(code, CoordinateReferenceSystems::loadEpsg);
    }

    /**
     * Parses a raw PROJJSON document into the typed {@link CoordinateReferenceSystem} ADT. Forwards to the bundled
     * {@link ProjJsonModule}: documents whose {@code "type"} discriminator this package does not model yet are
     * preserved as {@link CoordinateReferenceSystem.Unknown}.
     *
     * @throws tools.jackson.core.JacksonException (unchecked) if {@code projjson} is malformed JSON
     */
    public static CoordinateReferenceSystem fromProjjson(String projjson) {
        return MAPPER.readValue(projjson, CoordinateReferenceSystem.class);
    }

    /**
     * Renders a typed CRS back to a PROJJSON string via the symmetric serializer registered on {@link ProjJsonModule}.
     * The output carries the {@code "type"} discriminator for every concrete record;
     * {@link CoordinateReferenceSystem.Unknown} re-emits its captured raw JSON verbatim.
     */
    public static String toProjjson(CoordinateReferenceSystem crs) {
        return MAPPER.writeValueAsString(crs);
    }

    /**
     * The OGC:CRS84 CRS -- WGS 84 with longitude-before-latitude axis order. GeoParquet 1.1 treats this as the implicit
     * default for any geometry column with no CRS specified, and the parquetry writer emits this resource into the
     * footer's {@code "geo"} metadata when {@code WriteOptions} does not configure a per-column CRS.
     */
    public static CoordinateReferenceSystem ogcCrs84() {
        return OgcCrs84Holder.INSTANCE;
    }

    private static Optional<CoordinateReferenceSystem> loadEpsg(int code) {
        String resource = EPSG_RESOURCE_PREFIX + code + ".json";
        return readClasspathResource(resource);
    }

    private static Optional<CoordinateReferenceSystem> readClasspathResource(String path) {
        try (InputStream in = CoordinateReferenceSystems.class.getResourceAsStream(path)) {
            if (in == null) {
                return Optional.empty();
            }
            return Optional.of(MAPPER.readValue(in, CoordinateReferenceSystem.class));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read classpath:" + path, e);
        }
    }
}
