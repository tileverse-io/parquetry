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
package io.tileverse.parquetry.geo.jts;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Materializer that surfaces geometry-tagged columns as {@link Geometry} instances decoded from WKB.
 *
 * <p>Each row is returned as a {@code Map<ColumnPath, Object>}: the entries for columns tagged with
 * {@link LogicalType.Geometry} or {@link LogicalType.Geography} are JTS {@link Geometry} objects; entries for every
 * other column pass through the values the row accessor provides (boxed primitives, {@link java.nio.ByteBuffer} for
 * binary, {@link java.util.List} / {@link java.util.Map} for repeated / map columns).
 *
 * <p>Uniform across GeoParquet 1.x and 2.0: parquetry-core's {@code GeoMetadataBridge} synthesizes the Geometry /
 * Geography logical type on 1.x files at {@code ParquetDataset.open()}, so the materializer only needs to inspect the
 * schema's leaf annotations to know which columns to decode.
 *
 * <p>This class is thread-confined: a single {@code WKBReader} instance is reused per materializer instance. Pass one
 * materializer per reader thread.
 */
public final class JtsMaterializer implements Materializer<Map<ColumnPath, Object>> {

    private final WKBReader wkbReader;
    private final Set<ColumnPath> geoColumns;

    /**
     * Creates a materializer that decodes the geometry / geography columns of {@code projectedSchema}. The materializer
     * caches the set of geo column paths so each {@link #materialize(ParquetSchema, RowAccessor)} call only does map
     * lookups, not schema walks.
     */
    public JtsMaterializer(ParquetSchema projectedSchema) {
        this.wkbReader = new WKBReader();
        this.geoColumns = collectGeoColumns(projectedSchema);
    }

    /**
     * Discovery hook: returns the column paths that {@link #materialize(ParquetSchema, RowAccessor)} will decode as JTS
     * geometries. Useful for callers that want to know in advance which columns the materializer will rewrite.
     */
    public Set<ColumnPath> geometryColumns() {
        return Set.copyOf(geoColumns);
    }

    @Override
    public Map<ColumnPath, Object> materialize(ParquetSchema projectedSchema, RowAccessor row) {
        Map<ColumnPath, Object> values = row.values();
        Map<ColumnPath, Object> out = HashMap.newHashMap(values.size());
        for (Map.Entry<ColumnPath, Object> entry : values.entrySet()) {
            ColumnPath path = entry.getKey();
            Object value = entry.getValue();
            if (geoColumns.contains(path) && value instanceof ByteBuffer wkb) {
                out.put(path, decodeWkb(wkb));
            } else {
                out.put(path, value);
            }
        }
        return out;
    }

    private Geometry decodeWkb(ByteBuffer wkb) {
        ByteBuffer view = wkb.duplicate();
        byte[] bytes = new byte[view.remaining()];
        view.get(bytes);
        try {
            return wkbReader.read(bytes);
        } catch (ParseException e) {
            throw new IllegalStateException("Failed to decode WKB geometry: " + e.getMessage(), e);
        }
    }

    private static Set<ColumnPath> collectGeoColumns(ParquetSchema schema) {
        Set<ColumnPath> out = new LinkedHashSet<>();
        for (ColumnPath leaf : schema.leafColumns()) {
            schema.find(leaf)
                    .filter(Field.Primitive.class::isInstance)
                    .map(Field.Primitive.class::cast)
                    .flatMap(Field.Primitive::logicalType)
                    .filter(JtsMaterializer::isGeo)
                    .ifPresent(_ -> out.add(leaf));
        }
        return out;
    }

    private static boolean isGeo(LogicalType lt) {
        return lt instanceof LogicalType.Geometry || lt instanceof LogicalType.Geography;
    }
}
