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

import java.io.IOException;
import java.io.OutputStream;
import java.util.stream.Stream;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.feature.FeatureCollection;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;

/**
 * Writes a GeoTools {@link FeatureCollection} to a GeoParquet file.
 *
 * <p>Derives the write schema from the collection's {@link SimpleFeatureType}, streams the features as
 * {@link ParquetRecordBatch}es through {@link FeatureRecordBatches}, and hands each batch to a
 * {@link ParquetFileWriter}. The given {@link OutputStream} is never closed by this class; closing it is the caller's
 * responsibility.
 */
public final class GeoParquetExporter {

    private GeoParquetExporter() {}

    /**
     * Writes every feature in {@code features} to {@code out} as a GeoParquet file.
     *
     * <p>{@code out} is flushed by the underlying writer's footer but never closed; a caller that owns the stream (a
     * servlet response, for example) keeps control of its lifecycle.
     *
     * @throws IOException on a write failure
     */
    public static void export(
            FeatureCollection<SimpleFeatureType, SimpleFeature> features, OutputStream out, WriteOptions options)
            throws IOException {
        FeatureRecordBatches bridge = FeatureRecordBatches.forType(features.getSchema());
        WriteOptions effective = bridge.withGeometryCrs(options);
        try (Stream<ParquetRecordBatch> batches = bridge.batches(features, FeatureRecordBatches.DEFAULT_BATCH_ROWS);
                ParquetFileWriter writer = ParquetFileWriter.create(out, bridge.parquetSchema(), effective)) {
            batches.forEach(writer::writeBatch);
        }
    }
}
