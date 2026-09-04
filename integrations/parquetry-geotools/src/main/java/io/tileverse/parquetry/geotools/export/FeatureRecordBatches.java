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
package io.tileverse.parquetry.geotools.export;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.nested.NestedType;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.coordinatesequence.CoordinateSequences;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

/**
 * Lazily streams a GeoTools {@link FeatureCollection} as {@link ParquetRecordBatch}es sized to a row target and a byte
 * budget, whichever is reached first.
 *
 * <p>{@link #forType(SimpleFeatureType)} derives the write schema once via {@link FeatureWriteSchema}; every call to
 * {@link #batches(FeatureCollection, int)} then walks the given collection lazily, freezing one batch at a time. A
 * batch closes as soon as it holds {@code batchRows} rows or its authored bytes reach the internal 16 MB byte budget,
 * whichever comes first; the last batch of a collection may hold fewer rows. Closing the returned {@link Stream} closes
 * the underlying {@link FeatureIterator}.
 *
 * <p>An instance is immutable and safe for concurrent use. Each {@code batches} call builds its own iterator and its
 * own {@link WKBWriter}: two threads can stream the same {@link FeatureRecordBatches} against different collections at
 * once. The returned {@link Stream}s themselves are single-use, as any stream is.
 */
public final class FeatureRecordBatches {

    /** The default row target for {@link #batches(FeatureCollection, int)}: 8192 rows per batch. */
    public static final int DEFAULT_BATCH_ROWS = 8192;

    private static final long MAX_BATCH_BYTES = 16L * 1024 * 1024;

    private final FeatureWriteSchema writeSchema;

    private FeatureRecordBatches(FeatureWriteSchema writeSchema) {
        this.writeSchema = writeSchema;
    }

    /** Derives the write schema and per-attribute plan for {@code featureType}. */
    public static FeatureRecordBatches forType(SimpleFeatureType featureType) {
        return new FeatureRecordBatches(FeatureWriteSchema.of(featureType));
    }

    /** The Parquet schema {@code featureType} was inverted into. */
    public ParquetSchema parquetSchema() {
        return writeSchema.schema();
    }

    /** The GeoParquet metadata for the source feature type, or empty when it has no geometry attribute. */
    public Optional<GeoParquetMetadata> geoMetadata() {
        return writeSchema.geoMetadata();
    }

    /**
     * Streams {@code features} as record batches of up to {@code batchRows} rows each. Closing the returned stream
     * closes the underlying {@link FeatureIterator}.
     *
     * <p>The returned stream must be closed (try-with-resources) to release the underlying feature iterator.
     *
     * @throws IllegalArgumentException when {@code batchRows} is not positive
     */
    public Stream<ParquetRecordBatch> batches(
            FeatureCollection<SimpleFeatureType, SimpleFeature> features, int batchRows) {
        return batches(features, batchRows, MAX_BATCH_BYTES);
    }

    /**
     * Streams {@code features} as record batches, freezing a batch once it holds {@code batchRows} rows or its authored
     * bytes reach {@code maxBatchBytes}, whichever comes first. Package-private: the internal byte budget is a fixed
     * constant for callers of the public overload; this overload exists so a caller (or a test) can drive the
     * byte-budget freeze directly.
     *
     * @throws IllegalArgumentException when {@code batchRows} or {@code maxBatchBytes} is not positive
     */
    Stream<ParquetRecordBatch> batches(
            FeatureCollection<SimpleFeatureType, SimpleFeature> features, int batchRows, long maxBatchBytes) {
        if (batchRows < 1) {
            throw new IllegalArgumentException("batchRows must be positive: " + batchRows);
        }
        if (maxBatchBytes < 1) {
            throw new IllegalArgumentException("maxBatchBytes must be positive: " + maxBatchBytes);
        }
        FeatureIterator<SimpleFeature> iterator = features.features();
        BatchIterator batchIterator = new BatchIterator(iterator, batchRows, maxBatchBytes);
        Spliterator<ParquetRecordBatch> spliterator =
                Spliterators.spliteratorUnknownSize(batchIterator, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false).onClose(iterator::close);
    }

    /**
     * Returns {@code base} with a CRS entry added for every geometry attribute whose dotted column path is absent from
     * {@link WriteOptions#crs()}: the attribute's resolved EPSG CRS when one was found at schema derivation, or
     * explicit CRS84 otherwise (the GeoParquet default). Returns {@code base} unchanged when every geometry attribute
     * already has an entry.
     */
    WriteOptions withGeometryCrs(WriteOptions base) {
        Map<String, CoordinateReferenceSystem> additions = resolveMissingGeometryCrs(base.crs());
        if (additions.isEmpty()) {
            return base;
        }
        Map<String, CoordinateReferenceSystem> merged = new LinkedHashMap<>(base.crs());
        merged.putAll(additions);
        return rebuildWithCrs(base, merged);
    }

    /** The CRS entries to add for geometry attributes not already covered by {@code existingCrs}. */
    private Map<String, CoordinateReferenceSystem> resolveMissingGeometryCrs(
            Map<String, CoordinateReferenceSystem> existingCrs) {
        Map<String, CoordinateReferenceSystem> additions = new LinkedHashMap<>();
        for (FeatureWriteSchema.WriteAttribute attribute : writeSchema.attributes()) {
            if (!attribute.geometry()) {
                continue;
            }
            String columnPath = attribute.path().dot();
            if (existingCrs.containsKey(columnPath)) {
                continue;
            }
            additions.put(columnPath, resolveCrs(columnPath));
        }
        return additions;
    }

    /** The CRS for {@code columnPath}: its resolved EPSG code when one was found, else explicit CRS84. */
    private CoordinateReferenceSystem resolveCrs(String columnPath) {
        Integer epsg = writeSchema.geometryEpsg().get(columnPath);
        if (epsg == null) {
            return CoordinateReferenceSystems.ogcCrs84();
        }
        return CoordinateReferenceSystems.forEpsg(epsg).orElseGet(CoordinateReferenceSystems::ogcCrs84);
    }

    /** Rebuilds {@code base} through its canonical constructor with {@code crs} replacing its own CRS map. */
    private static WriteOptions rebuildWithCrs(WriteOptions base, Map<String, CoordinateReferenceSystem> crs) {
        return new WriteOptions(
                base.parquetVersion(),
                base.geoParquetMetadata(),
                base.rowGroupSize(),
                base.expectedRowCount(),
                base.pageValueLimit(),
                base.pageByteLimit(),
                base.dictionaryByteLimit(),
                base.defaultCompression(),
                base.compression(),
                base.encodingPolicies(),
                base.indexedColumns(),
                base.bloomFilters(),
                crs,
                base.keyValueMetadata(),
                base.tempDir(),
                base.writeObserver(),
                base.writeObserverCadenceRows(),
                base.bboxCovering());
    }

    /**
     * Freezes one {@link ParquetRecordBatch} per {@link #next()} call, stopping a batch once it reaches
     * {@code batchRows} rows or {@code maxBatchBytes} of authored bytes, whichever comes first. Owns its
     * {@link WKBWriter}s: WKB writers are not thread-safe, and every batch this iterator produces shares the same
     * instances.
     */
    private final class BatchIterator implements Iterator<ParquetRecordBatch> {

        private final FeatureIterator<SimpleFeature> features;
        private final int batchRows;
        private final long maxBatchBytes;
        private final WKBWriter wkbWriter2d = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
        private final WKBWriter wkbWriter3d = new WKBWriter(3, ByteOrderValues.LITTLE_ENDIAN);

        BatchIterator(FeatureIterator<SimpleFeature> features, int batchRows, long maxBatchBytes) {
            this.features = features;
            this.batchRows = batchRows;
            this.maxBatchBytes = maxBatchBytes;
        }

        @Override
        public boolean hasNext() {
            return features.hasNext();
        }

        @Override
        public ParquetRecordBatch next() {
            if (!features.hasNext()) {
                throw new NoSuchElementException();
            }
            ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(writeSchema.schema());
            while (features.hasNext() && builder.rowCount() < batchRows && builder.approxBatchBytes() < maxBatchBytes) {
                appendFeature(builder, features.next());
            }
            return builder.build();
        }

        /**
         * Authors one row from {@code feature}'s attributes. A null attribute value is skipped: the column stays unset
         * and an unset {@code OPTIONAL} leaf becomes null on {@link ParquetRecordBatchBuilder#endRow()}.
         */
        private void appendFeature(ParquetRecordBatchBuilder builder, SimpleFeature feature) {
            for (FeatureWriteSchema.WriteAttribute attribute : writeSchema.attributes()) {
                Object value = feature.getAttribute(attribute.featureIndex());
                if (value == null) {
                    continue;
                }
                if (attribute.nestedType().isPresent()) {
                    appendNestedAttribute(builder, attribute, value);
                } else if (attribute.geometry()) {
                    appendGeometry(builder, attribute.path(), (Geometry) value);
                } else {
                    setScalar(builder, attribute.path(), attribute.binding(), value);
                }
            }
            builder.endRow();
        }

        /**
         * Encodes {@code geometry} as WKB at its own coordinate dimension and stages it on the geometry column at
         * {@code path}. The dimension must follow the geometry: asking a 2D sequence for a Z ordinate fails on packed
         * coordinate sequences (the shape every geometry read from a parquetry store has) and fabricates a NaN Z on
         * array-backed ones.
         */
        private void appendGeometry(ParquetRecordBatchBuilder builder, ColumnPath path, Geometry geometry) {
            WKBWriter wkbWriter = CoordinateSequences.coordinateDimension(geometry) >= 3 ? wkbWriter3d : wkbWriter2d;
            byte[] wkb = wkbWriter.write(geometry);
            builder.setBinary(path, MemorySegment.ofArray(wkb));
        }

        /** Authors a List/Map attribute's value onto {@code builder} through {@link NestedValueAuthor}. */
        private void appendNestedAttribute(
                ParquetRecordBatchBuilder builder, FeatureWriteSchema.WriteAttribute attribute, Object value) {
            NestedType nestedType = attribute.nestedType().orElseThrow();
            NestedValueAuthor.author(builder, attribute.path(), nestedType, value);
        }

        /** Stages {@code value} on the scalar column at {@code path}, converting it per its Java binding. */
        private void setScalar(ParquetRecordBatchBuilder builder, ColumnPath path, Class<?> binding, Object value) {
            if (binding == String.class) {
                builder.setString(path, (String) value);
            } else if (binding == Integer.class || binding == Short.class || binding == Byte.class) {
                builder.setInt(path, ((Number) value).intValue());
            } else if (binding == Long.class || binding == BigInteger.class) {
                builder.setLong(path, ((Number) value).longValue());
            } else if (binding == Float.class) {
                builder.setFloat(path, (Float) value);
            } else if (binding == Double.class || binding == BigDecimal.class) {
                builder.setDouble(path, ((Number) value).doubleValue());
            } else if (binding == Boolean.class) {
                builder.setBoolean(path, (Boolean) value);
            } else if (binding == byte[].class) {
                builder.setBinary(path, MemorySegment.ofArray((byte[]) value));
            } else if (binding == UUID.class) {
                builder.setUuid(path, (UUID) value);
            } else if (binding == LocalDate.class) {
                builder.setInt(path, (int) ((LocalDate) value).toEpochDay());
            } else if (binding == java.sql.Date.class) {
                builder.setInt(path, (int) ((java.sql.Date) value).toLocalDate().toEpochDay());
            } else if (binding == LocalDateTime.class) {
                builder.setLong(path, toEpochMicros(((LocalDateTime) value).toInstant(ZoneOffset.UTC)));
            } else if (binding == Instant.class) {
                builder.setLong(path, toEpochMicros((Instant) value));
            } else if (binding == java.sql.Timestamp.class || binding == java.util.Date.class) {
                builder.setLong(path, toEpochMicros(((java.util.Date) value).toInstant()));
            } else {
                throw new IllegalArgumentException("Unsupported attribute binding " + binding.getName());
            }
        }

        private static long toEpochMicros(Instant instant) {
            return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000L);
        }
    }
}
