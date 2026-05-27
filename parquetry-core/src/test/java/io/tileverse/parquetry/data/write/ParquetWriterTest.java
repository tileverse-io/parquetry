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
package io.tileverse.parquetry.data.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ParquetWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void writesMagicAndFooterAroundOneRecord() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().build();
        Path parquetFile = tempDir.resolve("one-record.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            writer.write(row(Map.of(ColumnPath.of("id"), 7)));
        }

        byte[] bytes = Files.readAllBytes(parquetFile);
        assertThat(Arrays.copyOfRange(bytes, 0, 4)).containsExactly('P', 'A', 'R', '1');
        assertThat(Arrays.copyOfRange(bytes, bytes.length - 4, bytes.length)).containsExactly('P', 'A', 'R', '1');
        int footerLen = ByteBuffer.wrap(bytes, bytes.length - 8, 4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
        assertThat(footerLen).isPositive();

        List<Integer> ids = readIds(parquetFile);
        assertThat(ids).containsExactly(7);
    }

    @Test
    void rowGroupSizeRowsPolicyEmitsMultipleRowGroups() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().rowGroupSize(RowGroupSize.rows(5)).build();
        Path parquetFile = tempDir.resolve("multi-rg.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            for (int i = 0; i < 12; i++) {
                writer.write(row(Map.of(ColumnPath.of("id"), i)));
            }
            assertThat(writer.rowGroupsWritten()).isEqualTo(2L);
        }

        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            io.tileverse.parquetry.format.FileMetaData footer =
                    io.tileverse.parquetry.format.ParquetFormat.readFooter(source);
            assertThat(footer.rowGroups()).hasSize(3);
            assertThat(footer.rowGroups().get(0).numRows()).isEqualTo(5L);
            assertThat(footer.rowGroups().get(1).numRows()).isEqualTo(5L);
            assertThat(footer.rowGroups().get(2).numRows()).isEqualTo(2L);
            long previous = 0L;
            for (io.tileverse.parquetry.format.RowGroup rg : footer.rowGroups()) {
                long fileOffset =
                        rg.fileOffset().orElseThrow(() -> new AssertionError("file_offset must be set on every RG"));
                assertThat(fileOffset).isGreaterThanOrEqualTo(previous);
                previous = fileOffset;
            }
        }

        List<Integer> ids = readIds(parquetFile);
        assertThat(ids).containsExactlyElementsOf(rangeBoxed(0, 12));
    }

    @Test
    void explicitFlushRowGroupSplitsRowGroups() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().build();
        Path parquetFile = tempDir.resolve("explicit-flush.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            for (int i = 0; i < 3; i++) {
                writer.write(row(Map.of(ColumnPath.of("id"), i)));
            }
            writer.flushRowGroup();
            for (int i = 3; i < 7; i++) {
                writer.write(row(Map.of(ColumnPath.of("id"), i)));
            }
            assertThat(writer.rowGroupsWritten()).isEqualTo(1L);
        }

        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            io.tileverse.parquetry.format.FileMetaData footer =
                    io.tileverse.parquetry.format.ParquetFormat.readFooter(source);
            assertThat(footer.rowGroups()).hasSize(2);
            assertThat(footer.rowGroups().get(0).numRows()).isEqualTo(3L);
            assertThat(footer.rowGroups().get(1).numRows()).isEqualTo(4L);
        }
    }

    @Test
    void closeWithNoWritesProducesEmptyFooter() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().build();
        Path parquetFile = tempDir.resolve("empty.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            assertThat(writer.totalRows()).isZero();
            assertThat(writer.rowGroupsWritten()).isZero();
        }
        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            io.tileverse.parquetry.format.FileMetaData footer =
                    io.tileverse.parquetry.format.ParquetFormat.readFooter(source);
            assertThat(footer.rowGroups()).isEmpty();
            assertThat(footer.numRows()).isZero();
            assertThat(footer.schema()).isNotEmpty();
        }
    }

    @Test
    void geoMetadataLandsInFooterUnderGeoKey() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredInt32("id"));
        WriteOptions options = options().crsEpsg("geometry", 4326).build();
        Path parquetFile = tempDir.resolve("geo.parquet");
        byte[] wkbPoint = wkbPoint(1.0, 2.0);
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            writer.write(
                    row(Map.of(ColumnPath.of("geometry"), MemorySegment.ofArray(wkbPoint), ColumnPath.of("id"), 1)));
        }

        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            io.tileverse.parquetry.format.FileMetaData footer =
                    io.tileverse.parquetry.format.ParquetFormat.readFooter(source);
            List<io.tileverse.parquetry.format.KeyValue> kv = footer.keyValueMetadata();
            String geoJson = kv.stream()
                    .filter(e -> e.key().equals("geo"))
                    .findFirst()
                    .orElseThrow()
                    .value()
                    .orElseThrow();
            assertThat(geoJson).contains("primary_column");
            assertThat(geoJson).contains("geometry");

            io.tileverse.parquetry.format.SchemaElement geomElement = footer.schema().stream()
                    .filter(e -> "geometry".equals(e.name()))
                    .findFirst()
                    .orElseThrow();
            assertThat(geomElement.logicalType()).isPresent();
            assertThat(geomElement.logicalType().orElseThrow()).isInstanceOf(LogicalType.Geometry.class);
        }
    }

    @Test
    void footerDeclaresTypeDefinedColumnOrderPerLeaf() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"), requiredInt32("code"));
        WriteOptions options = options().build();
        Path parquetFile = tempDir.resolve("column-orders.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            writer.write(row(Map.of(ColumnPath.of("id"), 1, ColumnPath.of("code"), 2)));
        }

        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            io.tileverse.parquetry.format.FileMetaData footer =
                    io.tileverse.parquetry.format.ParquetFormat.readFooter(source);
            assertThat(footer.columnOrders()).hasValueSatisfying(orders -> {
                assertThat(orders).hasSize(2);
                assertThat(orders).allMatch(io.tileverse.parquetry.format.ColumnOrder.TypeDefined.class::isInstance);
            });
        }
    }

    @Test
    void footerCreatedByReportsParquetryBuildVersion() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().build();
        Path parquetFile = tempDir.resolve("created-by.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            writer.write(row(Map.of(ColumnPath.of("id"), 1)));
        }

        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            io.tileverse.parquetry.format.FileMetaData footer =
                    io.tileverse.parquetry.format.ParquetFormat.readFooter(source);
            String createdBy = footer.createdBy().orElseThrow();
            assertThat(createdBy).startsWith("parquetry version ");
            // The build version must be resolved from the Maven-filtered resource: neither the unresolved
            // placeholder nor the missing-resource fallback may leak into a published file.
            assertThat(createdBy).doesNotContain("${").doesNotContain("unknown");
        }
    }

    @Test
    void footerCarriesCallerKeyValueMetadataAlongsideGeo() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredInt32("id"));
        WriteOptions options = options()
                .crsEpsg("geometry", 4326)
                .keyValueMetadata("pandas", "{\"version\":\"1.0\"}")
                .build();
        Path parquetFile = tempDir.resolve("geo-with-kv.parquet");
        byte[] wkbPoint = wkbPoint(1.0, 2.0);
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            writer.write(
                    row(Map.of(ColumnPath.of("geometry"), MemorySegment.ofArray(wkbPoint), ColumnPath.of("id"), 1)));
        }

        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            Map<String, String> kv = dataset.keyValueMetadata();
            assertThat(kv).containsKey("geo").containsEntry("pandas", "{\"version\":\"1.0\"}");
        }
    }

    @Test
    void closeIsIdempotent() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().build();
        Path parquetFile = tempDir.resolve("idempotent.parquet");
        ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options);
        writer.write(row(Map.of(ColumnPath.of("id"), 1)));
        writer.close();
        long firstSize = Files.size(parquetFile);
        writer.close();
        assertThat(Files.size(parquetFile)).isEqualTo(firstSize);
    }

    @Test
    void schemaIsRequired() {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        WriteOptions defaults = WriteOptions.defaults();
        assertThatThrownBy(() -> ParquetWriter.create(sink, null, defaults)).isInstanceOf(NullPointerException.class);
    }

    // --- helpers ---

    private WriteOptions.Builder options() {
        return WriteOptions.builder().tempDir(tempDir);
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static WriteRow row(Map<ColumnPath, Object> values) {
        Map<ColumnPath, Object> copy = new HashMap<>(values);
        return copy::get;
    }

    private static List<Integer> readIds(Path file) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                stream.forEach(r -> ids.add(r.getInt(ColumnPath.of("id"))));
            }
        }
        return ids;
    }

    private static List<Integer> rangeBoxed(int from, int to) {
        List<Integer> out = new ArrayList<>(to - from);
        for (int i = from; i < to; i++) {
            out.add(i);
        }
        return out;
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer bb = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 1);
        bb.putInt(1);
        bb.putDouble(x);
        bb.putDouble(y);
        return bb.array();
    }
}
