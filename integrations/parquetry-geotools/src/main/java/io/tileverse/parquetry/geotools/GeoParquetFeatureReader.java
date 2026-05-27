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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Stream;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.feature.simple.SimpleFeatureBuilder;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.geo.jts.JtsMaterializer;
import io.tileverse.parquetry.geotools.GeoParquetSchemaMapper.AttributeMapping;
import io.tileverse.parquetry.schema.ColumnPath;

/** Streams a {@link ParquetDataset} and builds one {@link SimpleFeature} per row. */
final class GeoParquetFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final SimpleFeatureType featureType;
    private final List<AttributeMapping> attributes;
    private final Optional<AttributeMapping> fidAttribute;
    private final SimpleFeatureBuilder builder;
    private final Stream<Map<ColumnPath, Object>> stream;
    private final Iterator<Map<ColumnPath, Object>> rows;
    private long syntheticId;

    GeoParquetFeatureReader(
            SimpleFeatureType readType,
            List<AttributeMapping> attributes,
            ParquetDataset dataset,
            Predicate predicate,
            Projection projection,
            Optional<AttributeMapping> fidAttribute) {
        this.featureType = readType;
        this.attributes = attributes;
        this.fidAttribute = fidAttribute;
        this.builder = new SimpleFeatureBuilder(readType);
        JtsMaterializer materializer = new JtsMaterializer(dataset.schema());
        this.stream = dataset.read(predicate, projection, materializer, ReadOptions.DEFAULTS);
        this.rows = stream.iterator();
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return featureType;
    }

    @Override
    public boolean hasNext() {
        return rows.hasNext();
    }

    @Override
    public SimpleFeature next() {
        if (!rows.hasNext()) {
            throw new NoSuchElementException();
        }
        Map<ColumnPath, Object> row = rows.next();
        for (AttributeMapping attr : attributes) {
            Object value = row.get(attr.path());
            builder.set(attr.name(), attr.geometry() ? value : convert(value, attr.binding()));
        }
        return builder.buildFeature(featureId(row));
    }

    /**
     * The feature id for the current row: the configured feature id column's value when one is set, falling back to a
     * synthetic per-read sequence when no column is configured or its value is null.
     */
    private String featureId(Map<ColumnPath, Object> row) {
        if (fidAttribute.isEmpty()) {
            return Long.toString(syntheticId++);
        }
        AttributeMapping fid = fidAttribute.get();
        Object value = convert(row.get(fid.path()), fid.binding());
        if (value == null) {
            return Long.toString(syntheticId++);
        }
        return String.valueOf(value);
    }

    /**
     * Converts a raw materializer value to the declared attribute binding.
     *
     * <p>Geometry values pass through unchanged because the materializer already decoded them from WKB. Numerics and
     * booleans are already boxed Java types. Only {@link MemorySegment} values (BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY
     * columns) need explicit conversion: to {@code String} when the binding is {@code String.class}, or to
     * {@code byte[]} otherwise.
     */
    private static Object convert(Object value, Class<?> binding) {
        if (!(value instanceof MemorySegment segment)) {
            return value;
        }
        byte[] bytes = segment.toArray(ValueLayout.JAVA_BYTE);
        return binding == String.class ? new String(bytes, StandardCharsets.UTF_8) : bytes;
    }

    @Override
    public void close() {
        stream.close();
    }
}
