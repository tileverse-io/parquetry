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
package io.tileverse.parquetry.geo.jts;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.Identifier;

/**
 * Materializer that surfaces geometry-tagged columns as {@link Geometry} instances decoded from WKB.
 *
 * <p>Each row is returned as a {@code Map<ColumnPath, Object>}: the entries for columns tagged with
 * {@link LogicalType.Geometry} or {@link LogicalType.Geography} are JTS {@link Geometry} objects; entries for every
 * other column pass through the values the record provides (boxed primitives, {@link java.lang.foreign.MemorySegment}
 * for binary, {@link java.util.List} / {@link java.util.Map} for repeated / map columns).
 *
 * <p>Uniform across GeoParquet 1.x and 2.0: parquetry-format's {@code SchemaBuilder} folds the {@code "geo"} JSON into
 * the schema at footer-read time on 1.x files, so the materializer only needs to inspect the schema's leaf annotations
 * to know which columns to decode.
 *
 * <p>SRID handling: the typed {@link CoordinateReferenceSystem} on the leaf's logical-type annotation is consulted at
 * construction time; when it has an {@code EPSG} {@link Identifier}, the decoded {@link Geometry} carries that SRID via
 * {@link Geometry#setSRID(int)}. Columns with no CRS, or with a CRS whose {@link Identifier#epsgCode()} is empty (e.g.
 * an inline PROJJSON document without an EPSG entry, or {@code OGC:CRS84}), keep JTS's default SRID (0). The spec maps
 * {@code Optional.empty()} CRS on a Geometry leaf to the GeoParquet default (OGC:CRS84); consumers that need
 * {@code 4326} for that case set it themselves on top of this materializer's output.
 *
 * <p>This class is thread-safe: it holds only immutable column lookups and delegates WKB decoding to a shared,
 * thread-safe reader, hence a single materializer instance may be shared across reader threads.
 */
public final class JtsMaterializer implements Materializer<Map<ColumnPath, Object>> {

    private final Set<ColumnPath> geoColumns;
    private final Map<ColumnPath, Integer> sridByColumn;
    private final MemorySegmentWkbReader wkbReader = new MemorySegmentWkbReader();

    /**
     * Creates a materializer that decodes the geometry / geography columns of {@code projectedSchema}. The materializer
     * caches the set of geo column paths and any resolvable EPSG SRIDs so each {@link #materialize(ParquetSchema,
     * ParquetRecord)} call only does map lookups, not schema walks.
     */
    public JtsMaterializer(ParquetSchema projectedSchema) {
        Map<ColumnPath, Integer> sridScratch = new LinkedHashMap<>();
        this.geoColumns = collectGeoColumns(projectedSchema, sridScratch);
        this.sridByColumn = Map.copyOf(sridScratch);
    }

    /**
     * Discovery hook: returns the column paths that {@link #materialize(ParquetSchema, ParquetRecord)} will decode as
     * JTS geometries. Useful for callers that want to know in advance which columns the materializer will rewrite.
     */
    public Set<ColumnPath> geometryColumns() {
        return Set.copyOf(geoColumns);
    }

    /**
     * Returns the EPSG SRID this materializer will stamp on geometries decoded from {@code path}, when one is
     * resolvable from the column's typed CRS. {@link OptionalInt#empty()} means either the column is not a geo column,
     * its CRS is absent (spec-default OGC:CRS84), or its {@link Identifier} did not yield an EPSG code.
     */
    public OptionalInt sridFor(ColumnPath path) {
        Integer srid = sridByColumn.get(path);
        return srid == null ? OptionalInt.empty() : OptionalInt.of(srid);
    }

    @Override
    public Map<ColumnPath, Object> materialize(ParquetSchema projectedSchema, ParquetRecord row) {
        int count = row.columnCount();
        Map<ColumnPath, Object> out = HashMap.newHashMap(count);
        for (int i = 0; i < count; i++) {
            ColumnPath path = row.columnPath(i);
            if (geoColumns.contains(path)) {
                out.put(path, decodeGeometry(row, i, path));
            } else {
                out.put(path, row.get(i));
            }
        }
        return out;
    }

    private Geometry decodeGeometry(ParquetRecord row, int col, ColumnPath path) {
        Geometry geometry = row.readBinary(col, wkbReader::read);
        Integer srid = sridByColumn.get(path);
        if (geometry != null && srid != null) {
            geometry.setSRID(srid);
        }
        return geometry;
    }

    /**
     * Walks {@code schema}'s leaves to find columns annotated with Geometry / Geography. For each such column,
     * additionally records the resolvable EPSG SRID into {@code sridSink} so {@link #materialize} can stamp it on the
     * decoded geometry without re-traversing the schema.
     */
    private static Set<ColumnPath> collectGeoColumns(ParquetSchema schema, Map<ColumnPath, Integer> sridSink) {
        Set<ColumnPath> out = new LinkedHashSet<>();
        for (ColumnPath leaf : schema.leafColumns()) {
            schema.find(leaf)
                    .filter(SchemaNode.Primitive.class::isInstance)
                    .map(SchemaNode.Primitive.class::cast)
                    .flatMap(SchemaNode.Primitive::logicalType)
                    .filter(JtsMaterializer::isGeo)
                    .ifPresent(lt -> {
                        out.add(leaf);
                        epsgSridFor(lt).ifPresent(srid -> sridSink.put(leaf, srid));
                    });
        }
        return out;
    }

    private static boolean isGeo(LogicalType lt) {
        return lt instanceof LogicalType.Geometry || lt instanceof LogicalType.Geography;
    }

    /**
     * Extracts the EPSG SRID from a Geometry / Geography logical type's typed CRS, if present. Surfaces empty when the
     * CRS is absent (spec-default), the CRS has no {@link Identifier}, or the identifier is not an EPSG one.
     */
    private static OptionalInt epsgSridFor(LogicalType lt) {
        return crsOf(lt)
                .flatMap(CoordinateReferenceSystem::id)
                .map(Identifier::epsgCode)
                .orElse(OptionalInt.empty());
    }

    // S7475 (drop the unused type from the unnamed pattern) is informational only - palantirJavaFormat 2.90 cannot
    // parse
    // the bare-underscore form Sonar suggests; see memory feedback-palantir-unnamed-pattern.
    @SuppressWarnings("java:S7475")
    private static Optional<CoordinateReferenceSystem> crsOf(LogicalType lt) {
        return switch (lt) {
            case LogicalType.Geometry(Optional<CoordinateReferenceSystem> crs) -> crs;
            case LogicalType.Geography(
                    Optional<CoordinateReferenceSystem> crs,
                    Optional<EdgeInterpolationAlgorithm> _) -> crs;
            default -> Optional.empty();
        };
    }
}
