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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class RowGroupWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void singleColumnInt32RoundTripsThroughConsolidatedOutput() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options()
                .pageValueLimit(64)
                .encodingPolicy("id", EncodingPolicy.FORCE_PLAIN)
                .build();

        ByteArrayWritableChannel sink = new ByteArrayWritableChannel();
        RowGroupFlushResult flushed;
        try (RowGroupWriter rgw = new RowGroupWriter(options, schema, tempDir)) {
            for (int i = 0; i < 100; i++) {
                rgw.append(row(Map.of(ColumnPath.of("id"), i)));
            }
            flushed = rgw.flushTo(sink);
        }

        RowGroup rowGroup = flushed.rowGroup();
        assertThat(rowGroup.numRows()).isEqualTo(100L);
        assertThat(rowGroup.columns()).hasSize(1);

        ColumnChunk chunk = rowGroup.columns().get(0);
        ColumnMetaData meta = chunk.metaData().orElseThrow();
        assertThat(meta.type()).isEqualTo(PhysicalType.INT32);
        assertThat(meta.numValues()).isEqualTo(100L);
        assertThat(meta.pathInSchema()).containsExactly("id");
        assertThat(chunk.fileOffset()).isEqualTo(meta.dataPageOffset());

        byte[] bytes = sink.toByteArray();
        assertThat(bytes).hasSize((int) meta.totalCompressedSize());
        PageHeader header = ParquetFormat.readPageHeader(new ByteArrayInputStream(bytes));
        assertThat(header.type()).isEqualTo(PageType.DATA_PAGE_V2);
    }

    @Test
    void multiColumnSchemaPlacesChunksBackToBackInSchemaOrder() throws Exception {
        SchemaNode.Primitive idLeaf = requiredInt32("id");
        SchemaNode.Primitive timestampLeaf = requiredInt64("timestamp");
        SchemaNode.Primitive nameLeaf = requiredBinary("name");
        ParquetSchema schema = flatSchema(idLeaf, timestampLeaf, nameLeaf);

        WriteOptions options = options()
                .pageValueLimit(32)
                .encodingPolicy("id", EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy("timestamp", EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy("name", EncodingPolicy.FORCE_PLAIN)
                .build();

        ByteArrayWritableChannel sink = new ByteArrayWritableChannel();
        RowGroupFlushResult flushed;
        try (RowGroupWriter rgw = new RowGroupWriter(options, schema, tempDir)) {
            for (int i = 0; i < 50; i++) {
                rgw.append(row(Map.of(
                        ColumnPath.of("id"),
                        i,
                        ColumnPath.of("timestamp"),
                        1_000_000L + i,
                        ColumnPath.of("name"),
                        wrap("name-" + i))));
            }
            flushed = rgw.flushTo(sink);
        }

        RowGroup rg = flushed.rowGroup();
        assertThat(rg.columns()).hasSize(3);
        assertThat(rg.columns().get(0).metaData().orElseThrow().type()).isEqualTo(PhysicalType.INT32);
        assertThat(rg.columns().get(1).metaData().orElseThrow().type()).isEqualTo(PhysicalType.INT64);
        assertThat(rg.columns().get(2).metaData().orElseThrow().type()).isEqualTo(PhysicalType.BYTE_ARRAY);

        long previousEnd = 0L;
        for (ColumnChunk chunk : rg.columns()) {
            ColumnMetaData meta = chunk.metaData().orElseThrow();
            assertThat(meta.dataPageOffset())
                    .as("column %s data-page offset must come after previous chunk", meta.pathInSchema())
                    .isGreaterThanOrEqualTo(previousEnd);
            previousEnd = meta.dataPageOffset() + meta.totalCompressedSize();
        }
        assertThat(previousEnd)
                .as("sum of chunk lengths must match the bytes written to the sink")
                .isEqualTo(sink.size());
        assertThat(flushed.bytesWritten()).isEqualTo(sink.size());
    }

    @Test
    void emptyRowGroupRejectsFlush() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().build();
        ByteArrayWritableChannel sink = new ByteArrayWritableChannel();
        try (RowGroupWriter rgw = new RowGroupWriter(options, schema, tempDir)) {
            assertThatThrownBy(() -> rgw.flushTo(sink))
                    .isInstanceOf(ParquetWriteException.class)
                    .hasMessageContaining("empty row group");
        }
    }

    @Test
    void dictionaryPageOffsetsAreAbsoluteAfterConsolidation() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("color_id"), requiredInt32("brightness"));
        WriteOptions options = options().pageValueLimit(64).build();

        ByteArrayWritableChannel sink = new ByteArrayWritableChannel();
        RowGroupFlushResult flushed;
        try (RowGroupWriter rgw = new RowGroupWriter(options, schema, tempDir)) {
            for (int i = 0; i < 500; i++) {
                rgw.append(row(Map.of(
                        ColumnPath.of("color_id"), i % 4,
                        ColumnPath.of("brightness"), i % 8)));
            }
            flushed = rgw.flushTo(sink);
        }

        for (ColumnChunk chunk : flushed.rowGroup().columns()) {
            ColumnMetaData meta = chunk.metaData().orElseThrow();
            assertThat(meta.dictionaryPageOffset())
                    .as("column %s should have written a dictionary page", meta.pathInSchema())
                    .isPresent();
            long dictOffset = meta.dictionaryPageOffset().orElseThrow();
            assertThat(dictOffset)
                    .as("dictionary page must precede the first data page")
                    .isLessThan(meta.dataPageOffset());
        }

        byte[] bytes = sink.toByteArray();
        long firstDictOffset = flushed.rowGroup()
                .columns()
                .get(0)
                .metaData()
                .orElseThrow()
                .dictionaryPageOffset()
                .orElseThrow();
        assertThat(firstDictOffset).isZero();
        PageHeader firstHeader = ParquetFormat.readPageHeader(new ByteArrayInputStream(bytes));
        assertThat(firstHeader.type()).isEqualTo(PageType.DICTIONARY_PAGE);
    }

    @Test
    void offsetIndexEntriesAreAbsoluteAndNonOverlapping() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("a"), requiredInt32("b"));
        WriteOptions options = options()
                .pageValueLimit(8)
                .encodingPolicy("a", EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy("b", EncodingPolicy.FORCE_PLAIN)
                .build();

        ByteArrayWritableChannel sink = new ByteArrayWritableChannel();
        RowGroupFlushResult flushed;
        try (RowGroupWriter rgw = new RowGroupWriter(options, schema, tempDir)) {
            for (int i = 0; i < 64; i++) {
                rgw.append(row(Map.of(ColumnPath.of("a"), i, ColumnPath.of("b"), i * 2)));
            }
            flushed = rgw.flushTo(sink);
        }

        List<RowGroupFlushResult.ColumnArtifacts> artifacts = flushed.columnArtifacts();
        assertThat(artifacts).hasSize(2);
        long previousChunkEnd = 0L;
        for (int idx = 0; idx < artifacts.size(); idx++) {
            RowGroupFlushResult.ColumnArtifacts artifact = artifacts.get(idx);
            ColumnMetaData meta =
                    flushed.rowGroup().columns().get(idx).metaData().orElseThrow();
            List<PageLocation> locs = artifact.offsetIndex().pageLocations();
            assertThat(locs).hasSizeGreaterThan(1);
            assertThat(locs.get(0).offset())
                    .as("first page offset for %s must equal the column's data-page offset", meta.pathInSchema())
                    .isEqualTo(meta.dataPageOffset());
            long previousPageEnd = locs.get(0).offset();
            for (PageLocation loc : locs) {
                assertThat(loc.offset()).isGreaterThanOrEqualTo(previousPageEnd);
                assertThat(loc.offset()).isGreaterThanOrEqualTo(previousChunkEnd);
                previousPageEnd = loc.offset() + loc.compressedPageSize();
            }
            previousChunkEnd = meta.dataPageOffset() + meta.totalCompressedSize();
        }
    }

    @Test
    void tempFilesAreCleanedUpAfterFlush() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("a"), requiredInt32("b"));
        WriteOptions options = options().build();
        ByteArrayWritableChannel sink = new ByteArrayWritableChannel();
        try (RowGroupWriter rgw = new RowGroupWriter(options, schema, tempDir)) {
            for (int i = 0; i < 10; i++) {
                rgw.append(row(Map.of(ColumnPath.of("a"), i, ColumnPath.of("b"), i + 1)));
            }
            rgw.flushTo(sink);
        }
        assertThat(listTempFiles()).isEmpty();
    }

    @Test
    void closeWithoutFlushCleansUpTempFiles() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("a"));
        WriteOptions options = options().build();
        RowGroupWriter rgw = new RowGroupWriter(options, schema, tempDir);
        rgw.append(row(Map.of(ColumnPath.of("a"), 1)));
        rgw.close();
        assertThat(listTempFiles())
                .as("close without flushTo must delete every per-column temp file it created")
                .isEmpty();
    }

    @Test
    void optionalColumnRecordsNullsForAbsentValues() throws Exception {
        SchemaNode.Primitive optional = new SchemaNode.Primitive(
                "nullable", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        ParquetSchema schema = flatSchema(optional);
        WriteOptions options = options()
                .pageValueLimit(16)
                .encodingPolicy("nullable", EncodingPolicy.FORCE_PLAIN)
                .build();

        ByteArrayWritableChannel sink = new ByteArrayWritableChannel();
        RowGroupFlushResult flushed;
        try (RowGroupWriter rgw = new RowGroupWriter(options, schema, tempDir)) {
            for (int i = 0; i < 10; i++) {
                Map<ColumnPath, Object> values = new HashMap<>();
                if (i % 2 == 0) {
                    values.put(ColumnPath.of("nullable"), i);
                }
                rgw.append(row(values));
            }
            flushed = rgw.flushTo(sink);
        }

        ColumnMetaData meta = flushed.rowGroup().columns().get(0).metaData().orElseThrow();
        assertThat(meta.numValues()).isEqualTo(10L);
        assertThat(meta.statistics().orElseThrow().nullCount().orElseThrow()).isEqualTo(5L);
    }

    // --- helpers ---

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), /* fieldId */ -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static WriteOptions.Builder options() {
        return WriteOptions.builder();
    }

    private static WriteRow row(Map<ColumnPath, Object> values) {
        Map<ColumnPath, Object> copy = Map.copyOf(values);
        return copy::get;
    }

    private static MemorySegment wrap(String s) {
        return MemorySegment.ofArray(s.getBytes(StandardCharsets.UTF_8)).asReadOnly();
    }

    private List<Path> listTempFiles() throws Exception {
        List<Path> result = new ArrayList<>();
        try (var stream = Files.list(tempDir)) {
            stream.filter(p -> p.getFileName().toString().startsWith("rgw-")).forEach(result::add);
        }
        return result;
    }
}
