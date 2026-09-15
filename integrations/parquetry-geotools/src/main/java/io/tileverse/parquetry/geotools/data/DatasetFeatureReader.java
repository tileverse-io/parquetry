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
package io.tileverse.parquetry.geotools.data;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.geo.MemorySegmentWkbReader;
import io.tileverse.parquetry.geotools.data.FeatureTypeMapper.AttributeMapping;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Streams a {@link ParquetDataset} and builds one {@link SimpleFeature} per row.
 *
 * <p>Each row is the lazy {@link ParquetRecord} view: attributes are pulled by typed accessor as the feature is built,
 * and geometry columns decode in place from the record's WKB with no intermediate map or per-value slice.
 */
final class DatasetFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final SimpleFeatureType featureType;
    private final List<AttributeMapping> attributes;
    private final Optional<AttributeMapping> fidAttribute;
    private final Map<ColumnPath, Integer> geometrySrids;
    private final SimpleFeatureBuilder builder;
    private final MemorySegmentWkbReader wkbReader = new MemorySegmentWkbReader();
    private final Stream<ParquetRecord> stream;
    private final Iterator<ParquetRecord> rows;
    private long syntheticId;

    @SuppressWarnings("java:S107") // cohesive read collaborators; a parameter object would only relocate the arity
    DatasetFeatureReader(
            SimpleFeatureType readType,
            List<AttributeMapping> attributes,
            Map<ColumnPath, Integer> geometrySrids,
            ParquetDataset dataset,
            Predicate predicate,
            Projection projection,
            Optional<AttributeMapping> fidAttribute,
            ReadOptions options) {
        this.featureType = readType;
        this.attributes = attributes;
        this.geometrySrids = geometrySrids;
        this.fidAttribute = fidAttribute;
        this.builder = new SimpleFeatureBuilder(readType);
        this.stream = dataset.read(predicate, projection, options);
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
     * owned, batch-independent value for nested columns (struct/list/map, translated by {@link NestedValues}), an owned
     * {@code String} for text, a {@link UUID} for a UUID column, an owned {@code byte[]} for other binary, the
     * {@code java.time} or {@link BigDecimal} value of an annotated column (see {@link ScalarAttributeValues}), a boxed
     * primitive otherwise, or {@code null} for a null cell.
     */
    private Object attributeValue(ParquetRecord row, AttributeMapping attr) {
        ColumnPath path = attr.path();
        if (row.isNull(path)) {
            return null;
        }
        if (attr.geometry()) {
            return decodeGeometry(row, attr);
        }
        if (attr.nestedType() != null) {
            return NestedValues.translate(row.get(path), attr.nestedType());
        }
        Class<?> binding = attr.binding();
        if (binding == String.class) {
            return row.getString(path);
        }
        if (binding == UUID.class) {
            return row.getUuid(path);
        }
        if (binding == byte[].class) {
            return row.getBinary(path);
        }
        if (binding == LocalDate.class) {
            return TemporalValues.toLocalDate(row.getInt(path));
        }
        if (binding == Instant.class) {
            return TemporalValues.toInstant(row.getLong(path), temporalUnit(attr));
        }
        if (binding == LocalDateTime.class) {
            return TemporalValues.toLocalDateTime(row.getLong(path), temporalUnit(attr));
        }
        if (binding == BigDecimal.class) {
            return decimalValue(row, attr);
        }
        if (binding == LocalTime.class) {
            return timeValue(row, attr);
        }
        return row.get(path);
    }

    /** The decimal value of a DECIMAL attribute, read from its column's physical encoding at the column's scale. */
    private static BigDecimal decimalValue(ParquetRecord row, AttributeMapping attr) {
        ColumnPath path = attr.path();
        int scale = decimalScale(attr);
        return switch (attr.kind()) {
            case INT32 -> ScalarAttributeValues.decimal(row.getInt(path), scale);
            case INT64 -> ScalarAttributeValues.decimal(row.getLong(path), scale);
            case FIXED_LEN_BYTE_ARRAY -> fixedLengthDecimal(row, path, scale);
            default ->
                throw new IllegalStateException("decimal attribute on an unsupported column kind: " + attr.name());
        };
    }

    /** The decimal held by a fixed-length cell, decoded from its big-endian two's-complement bytes at {@code scale}. */
    private static BigDecimal fixedLengthDecimal(ParquetRecord row, ColumnPath path, int scale) {
        return row.readBinary(
                path,
                (backing, offset, length) -> ScalarAttributeValues.decimal(backing.asSlice(offset, length), scale));
    }

    /** The time-of-day value of a TIME attribute, read from its column's physical encoding in the column's unit. */
    private static LocalTime timeValue(ParquetRecord row, AttributeMapping attr) {
        ColumnPath path = attr.path();
        long value = attr.kind() == PrimitiveKind.INT32 ? row.getInt(path) : row.getLong(path);
        return ScalarAttributeValues.time(value, attr.kind(), temporalUnit(attr));
    }

    /** The scale of a decimal attribute, read from the leaf's DECIMAL logical type. */
    private static int decimalScale(AttributeMapping attr) {
        LogicalType logical = attr.logicalType()
                .orElseThrow(() -> new IllegalStateException("decimal attribute has no logical type: " + attr.name()));
        if (logical instanceof LogicalType.Decimal(int scale, int _)) {
            return scale;
        }
        throw new IllegalStateException("decimal attribute is not annotated as a decimal: " + attr.name());
    }

    /** The time unit of a temporal attribute, read from the leaf's TIME or TIMESTAMP logical type. */
    private static LogicalType.TimeUnit temporalUnit(AttributeMapping attr) {
        LogicalType logical = attr.logicalType()
                .orElseThrow(() -> new IllegalStateException("temporal attribute has no logical type: " + attr.name()));
        if (logical instanceof LogicalType.Timestamp timestamp) {
            return timestamp.unit();
        }
        if (logical instanceof LogicalType.Time time) {
            return time.unit();
        }
        throw new IllegalStateException("temporal attribute is not a time or timestamp: " + attr.name());
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
