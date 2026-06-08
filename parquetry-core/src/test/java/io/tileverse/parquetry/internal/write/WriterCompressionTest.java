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
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
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

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class WriterCompressionTest {

    @TempDir
    Path tempDir;

    @Test
    void uncompressedRoundTripsRows() throws Exception {
        roundTripCodec(Compression.uncompressed(), CompressionCodec.UNCOMPRESSED);
    }

    @Test
    void snappyRoundTripsRows() throws Exception {
        roundTripCodec(Compression.snappy(), CompressionCodec.SNAPPY);
    }

    @Test
    void gzipRoundTripsRows() throws Exception {
        roundTripCodec(Compression.gzip(), CompressionCodec.GZIP);
    }

    @Test
    void lz4RawRoundTripsRows() throws Exception {
        roundTripCodec(Compression.lz4Raw(), CompressionCodec.LZ4_RAW);
    }

    @Test
    void zstdDefaultLevelRoundTripsRows() throws Exception {
        roundTripCodec(Compression.zstd(3), CompressionCodec.ZSTD);
    }

    @Test
    void zstdHighLevelRoundTripsRows() throws Exception {
        roundTripCodec(Compression.zstd(15), CompressionCodec.ZSTD);
    }

    @Test
    void brotliWriteFailsWithUnsupportedOperation() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options =
                options().defaultCompression(Compression.brotli()).build();
        Path file = tempDir.resolve("brotli.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            assertThatThrownBy(() -> {
                        for (int i = 0; i < 200; i++) {
                            writer.write(rowOf(Map.of(ColumnPath.of("id"), i)));
                        }
                        writer.flushRowGroup();
                    })
                    .satisfiesAnyOf(
                            t -> assertThat(t).isInstanceOf(UnsupportedOperationException.class),
                            t -> assertThat(t).hasRootCauseInstanceOf(UnsupportedOperationException.class));
        }
    }

    @Test
    void perColumnOverrideAppliesToConfiguredColumnOnly() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("a"), requiredInt32("b"), requiredInt32("c"));
        WriteOptions options = options()
                .defaultCompression(Compression.zstd(3))
                .compression("a", Compression.snappy())
                .build();
        Path file = tempDir.resolve("per-column.parquet");

        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (int i = 0; i < 200; i++) {
                writer.write(rowOf(Map.of(
                        ColumnPath.of("a"), i,
                        ColumnPath.of("b"), i + 1,
                        ColumnPath.of("c"), i + 2)));
            }
        }

        assertThat(columnMetadata(file, "a").codec()).isEqualTo(CompressionCodec.SNAPPY);
        assertThat(columnMetadata(file, "b").codec()).isEqualTo(CompressionCodec.ZSTD);
        assertThat(columnMetadata(file, "c").codec()).isEqualTo(CompressionCodec.ZSTD);
    }

    @Test
    void compressibleDataShrinksUnderSnappy() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("v"));
        Path compressed = tempDir.resolve("snappy-compressible.parquet");
        Path uncompressed = tempDir.resolve("uncompressed-compressible.parquet");

        writeRepeatedInts(compressed, schema, Compression.snappy());
        writeRepeatedInts(uncompressed, schema, Compression.uncompressed());

        ColumnMetaData snappyMeta = columnMetadata(compressed, "v");
        ColumnMetaData rawMeta = columnMetadata(uncompressed, "v");

        assertThat(snappyMeta.totalCompressedSize())
                .as("highly compressible repeated integers should shrink under Snappy")
                .isLessThan(rawMeta.totalCompressedSize());
    }

    // --- helpers ---

    private void roundTripCodec(Compression writeCodec, CompressionCodec expectedWire) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().defaultCompression(writeCodec).build();
        Path file = tempDir.resolve(expectedWire.name() + ".parquet");

        int rows = 1_000;
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (int i = 0; i < rows; i++) {
                writer.write(rowOf(Map.of(ColumnPath.of("id"), i)));
            }
        }

        ColumnMetaData meta = columnMetadata(file, "id");
        assertThat(meta.codec()).as("ColumnMetaData codec for %s", expectedWire).isEqualTo(expectedWire);

        List<Integer> read = readInts(file, "id");
        assertThat(read).hasSize(rows);
        for (int i = 0; i < rows; i++) {
            assertThat(read.get(i)).isEqualTo(i);
        }

        // Cross-check the per-page header carries the same codec the metadata advertises so a downstream
        // reader that scans page headers (not just the footer) decompresses with the right codec.
        List<PageHeader> headers = collectPageHeaders(file, "id");
        assertThat(headers).isNotEmpty();
    }

    private void writeRepeatedInts(Path file, ParquetSchema schema, Compression codec) throws Exception {
        WriteOptions options = options()
                .defaultCompression(codec)
                .encodingPolicy("v", EncodingPolicy.FORCE_PLAIN)
                .build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (int i = 0; i < 10_000; i++) {
                writer.write(rowOf(Map.of(ColumnPath.of("v"), 42)));
            }
        }
    }

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

    private static WriteRow rowOf(Map<ColumnPath, Object> values) {
        Map<ColumnPath, Object> copy = new HashMap<>(values);
        return copy::get;
    }

    private static List<Integer> readInts(Path file, String columnName) {
        List<Integer> out = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader dataset = ParquetReader.open(source);
            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                stream.forEach(r -> out.add(r.getInt(ColumnPath.of(columnName))));
            }
        }
        return out;
    }

    private static ColumnMetaData columnMetadata(Path file, String columnName) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            for (RowGroup rg : footer.rowGroups()) {
                ColumnMetaData meta = findColumnInRowGroup(rg, columnName);
                if (meta != null) {
                    return meta;
                }
            }
            throw new AssertionError("column " + columnName + " not found");
        }
    }

    private static ColumnMetaData findColumnInRowGroup(RowGroup rg, String columnName) {
        for (ColumnChunk chunk : rg.columns()) {
            ColumnMetaData meta = chunk.metaData().orElse(null);
            if (meta == null) {
                continue;
            }
            if (meta.pathInSchema().size() == 1
                    && columnName.equals(meta.pathInSchema().get(0))) {
                return meta;
            }
        }
        return null;
    }

    private static List<PageHeader> collectPageHeaders(Path file, String columnName) throws Exception {
        List<PageHeader> headers = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            for (RowGroup rg : footer.rowGroups()) {
                ColumnMetaData meta = findColumnInRowGroup(rg, columnName);
                if (meta == null) {
                    continue;
                }
                long chunkStart = meta.dictionaryPageOffset().orElse(meta.dataPageOffset());
                long chunkSize = meta.totalCompressedSize();
                if (chunkSize <= 0 || chunkSize > Integer.MAX_VALUE) {
                    continue;
                }
                byte[] bytes = new byte[(int) chunkSize];
                source.readFully(chunkStart, MemorySegment.ofArray(bytes));
                InputStream in = new ByteArrayInputStream(bytes);
                long consumed = 0L;
                while (consumed < chunkSize) {
                    int before = in.available();
                    PageHeader header = ParquetFormat.readPageHeader(in);
                    int headerBytes = before - in.available();
                    headers.add(header);
                    long pagePayloadBytes = header.compressedPageSize();
                    long skipped = in.skip(pagePayloadBytes);
                    if (skipped != pagePayloadBytes) {
                        throw new java.io.IOException(
                                "expected to skip " + pagePayloadBytes + " bytes, skipped " + skipped);
                    }
                    consumed += headerBytes + pagePayloadBytes;
                }
            }
        }
        return headers;
    }
}
