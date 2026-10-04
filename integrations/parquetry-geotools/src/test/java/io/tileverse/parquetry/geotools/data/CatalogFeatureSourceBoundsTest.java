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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStore;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Feature source bounds over a GeoParquet file with a {@code "geo"} metadata box wrapping the antimeridian. A
 * GEOGRAPHY-aware writer declares the extent of points at longitudes 175 and -175 that way; GeoTools receives the
 * planar enclosure of that box, never a box with its minimum east of its maximum.
 */
class CatalogFeatureSourceBoundsTest {

    private static final String GEOMETRY = "geometry";
    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @TempDir
    Path dir;

    @Test
    void aDeclaredBboxWrappingTheAntimeridianIsReportedAsItsPlanarEnclosure() throws Exception {
        Path planar = writePoints(dir.resolve("planar.parquet"), new double[][] {{175, -5}, {-175, 5}});
        Path file = withDeclaredBbox(planar, dir.resolve("wrapping.parquet"), new double[] {175, -5, -175, 5});
        FilesetCatalog catalog = FilesetCatalog.open(
                LocalFileSource.file(file),
                CatalogOptions.builder().datasetName("points").build());
        try (GeoParquetDataStore store = new GeoParquetDataStore(catalog)) {
            CatalogFeatureSource source = (CatalogFeatureSource) store.getFeatureSource("points");

            ReferencedEnvelope bounds = source.getBounds();

            assertThat(bounds.getMinX()).isEqualTo(-180);
            assertThat(bounds.getMaxX()).isEqualTo(180);
            assertThat(bounds.getMinY()).isEqualTo(-5);
            assertThat(bounds.getMaxY()).isEqualTo(5);
        }
    }

    private static Path writePoints(Path file, double[][] points) throws Exception {
        ParquetSchema schema = geometrySchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(file.getParent())
                .crsEpsg(GEOMETRY, 4326)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options);
                ParquetRecordBatch batch = pointBatch(schema, points)) {
            writer.writeBatch(batch);
        }
        return file;
    }

    private static ParquetSchema geometrySchema() {
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                GEOMETRY, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetRecordBatch pointBatch(ParquetSchema schema, double[][] points) {
        MemorySegment[] wkb = new MemorySegment[points.length];
        for (int i = 0; i < points.length; i++) {
            wkb[i] = MemorySegment.ofArray(wkbPoint(points[i][0], points[i][1]));
        }
        BitSet allValid = new BitSet(points.length);
        allValid.set(0, points.length);
        Validity validity = Validity.of(allValid, points.length);
        Map<ColumnPath, ColumnVector> columns =
                Map.of(ColumnPath.of(GEOMETRY), BinaryVector.materialized(wkb, validity));
        return new DefaultParquetRecordBatch(schema, columns, points.length, Arena.ofShared());
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(1);
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
    }

    /** Writes {@code target} as a copy of {@code source} with {@code bbox} declared by its {@code "geo"} metadata. */
    private static Path withDeclaredBbox(Path source, Path target, double[] bbox) throws IOException {
        FileMetaData footer = readFooter(source);
        FileMetaData edited = withKeyValues(footer, keyValuesDeclaring(footer.keyValueMetadata(), bbox));
        return rewriteFooter(source, target, edited);
    }

    private static FileMetaData readFooter(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFormat.readFooter(source);
        }
    }

    private static List<KeyValue> keyValuesDeclaring(List<KeyValue> keyValues, double[] bbox) {
        List<KeyValue> edited = new ArrayList<>(keyValues.size());
        for (KeyValue keyValue : keyValues) {
            if ("geo".equals(keyValue.key())) {
                String geo = keyValue.value().orElseThrow();
                edited.add(new KeyValue("geo", Optional.of(declaring(geo, bbox))));
            } else {
                edited.add(keyValue);
            }
        }
        return edited;
    }

    private static String declaring(String geo, double[] bbox) {
        ObjectNode document = (ObjectNode) JSON.readTree(geo);
        ObjectNode column = (ObjectNode) document.get("columns").get(GEOMETRY);
        ArrayNode corners = column.putArray("bbox");
        for (double corner : bbox) {
            corners.add(corner);
        }
        return JSON.writeValueAsString(document);
    }

    private static FileMetaData withKeyValues(FileMetaData footer, List<KeyValue> keyValues) {
        return new FileMetaData(
                footer.version(),
                footer.schema(),
                footer.numRows(),
                footer.rowGroups(),
                keyValues,
                footer.createdBy(),
                footer.columnOrders(),
                footer.encryptionAlgorithm(),
                footer.footerSigningKeyMetadata());
    }

    /** Writes {@code target}: the bytes of {@code source} ahead of its footer, then {@code footer}. */
    private static Path rewriteFooter(Path source, Path target, FileMetaData footer) throws IOException {
        byte[] bytes = Files.readAllBytes(source);
        byte[] encoded = ParquetFormat.toBytes(footer);
        try (OutputStream out = Files.newOutputStream(target)) {
            out.write(bytes, 0, footerOffset(bytes));
            out.write(encoded);
            out.write(littleEndian(encoded.length));
            out.write(MAGIC);
        }
        return target;
    }

    private static int footerOffset(byte[] file) {
        int lengthOffset = file.length - Integer.BYTES - MAGIC.length;
        ByteBuffer trailer = ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN);
        return lengthOffset - trailer.getInt(lengthOffset);
    }

    private static byte[] littleEndian(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(value);
        return buffer.array();
    }
}
