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
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.geo.jts.MemorySegmentWkbReader;
import io.tileverse.parquetry.geotools.GeoParquetSchemaMapper.AttributeMapping;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Streams a {@link ParquetDataset} and builds one {@link SimpleFeature} per row.
 *
 * <p>Each row is the lazy {@link ParquetRecord} view: attributes are pulled by typed accessor as the feature is built,
 * and geometry columns decode in place from the record's WKB with no intermediate map or per-value slice.
 */
final class GeoParquetFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final SimpleFeatureType featureType;
    private final List<AttributeMapping> attributes;
    private final Optional<AttributeMapping> fidAttribute;
    private final Map<ColumnPath, Integer> geometrySrids;
    private final SimpleFeatureBuilder builder;
    private final MemorySegmentWkbReader wkbReader = new MemorySegmentWkbReader();
    private final Stream<ParquetRecord> stream;
    private final Iterator<ParquetRecord> rows;
    private long syntheticId;

    GeoParquetFeatureReader(
            SimpleFeatureType readType,
            List<AttributeMapping> attributes,
            Map<ColumnPath, Integer> geometrySrids,
            ParquetDataset dataset,
            Predicate predicate,
            Projection projection,
            Optional<AttributeMapping> fidAttribute) {
        this.featureType = readType;
        this.attributes = attributes;
        this.geometrySrids = geometrySrids;
        this.fidAttribute = fidAttribute;
        this.builder = new SimpleFeatureBuilder(readType);
        this.stream = dataset.read(predicate, projection, ReadOptions.DEFAULTS);
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
        ParquetRecord row = rows.next();
        for (AttributeMapping attr : attributes) {
            builder.set(attr.name(), attributeValue(row, attr));
        }
        return builder.buildFeature(featureId(row));
    }

    /**
     * The GeoTools attribute value for {@code attr} on this row: a decoded {@link Geometry} for geometry columns, an
     * owned {@code String} or {@code byte[]} for binary, a boxed primitive otherwise, or {@code null} for a null cell.
     */
    private Object attributeValue(ParquetRecord row, AttributeMapping attr) {
        ColumnPath path = attr.path();
        if (row.isNull(path)) {
            return null;
        }
        if (attr.geometry()) {
            return decodeGeometry(row, attr);
        }
        Class<?> binding = attr.binding();
        if (binding == String.class) {
            return row.getString(path);
        }
        if (binding == byte[].class) {
            return row.getBinary(path);
        }
        return row.get(path);
    }

    /** Decodes the geometry column in place from the record's WKB and stamps the column's EPSG SRID when resolved. */
    private Geometry decodeGeometry(ParquetRecord row, AttributeMapping attr) {
        Geometry geometry = row.readBinary(attr.path(), wkbReader::read);
        Integer srid = geometrySrids.get(attr.path());
        if (geometry != null && srid != null) {
            geometry.setSRID(srid);
        }
        return geometry;
    }

    /**
     * The feature id for the current row: the configured feature id column's value when one is set, falling back to a
     * synthetic per-read sequence when no column is configured or its value is null.
     */
    private String featureId(ParquetRecord row) {
        if (fidAttribute.isEmpty()) {
            return Long.toString(syntheticId++);
        }
        AttributeMapping fid = fidAttribute.get();
        Object value = attributeValue(row, fid);
        if (value == null) {
            return Long.toString(syntheticId++);
        }
        return String.valueOf(value);
    }

    @Override
    public void close() {
        stream.close();
    }
}
