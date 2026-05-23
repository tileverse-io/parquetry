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

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.BloomFilterConfig;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ColumnChunkWriterTest {

    @TempDir
    Path tempDir;

    private Path tempFile;

    @BeforeEach
    void allocateTempFile() throws Exception {
        tempFile = Files.createTempFile(tempDir, "ccw", ".tmp");
    }

    @AfterEach
    void deleteTempFile() throws Exception {
        Files.deleteIfExists(tempFile);
    }

    @Test
    void singlePageRoundTripV2() throws Exception {
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(16)
                .pageByteLimit(1 << 20)
                .encodingPolicy("col", EncodingPolicy.FORCE_PLAIN)
                .build();
        SchemaNode.Primitive leaf = requiredInt32("col");

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            for (int i = 0; i < 16; i++) {
                writer.appendInt(i, 0, 0);
            }
            result = writer.finishChunk();
        }

        assertThat(result.numValues()).isEqualTo(16);
        assertThat(result.encodings()).containsExactly(Encoding.PLAIN);
        assertThat(result.dictionaryPageOffset()).isEqualTo(-1L);
        assertThat(result.firstDataPageOffset()).isZero();
        assertThat(result.columnIndex()).isNotNull();
        assertThat(result.offsetIndex()).isNotNull();
        assertThat(result.offsetIndex().pageLocations()).hasSize(1);
        assertThat(result.columnIndex().nullPages()).containsExactly(false);

        byte[] wire = Files.readAllBytes(tempFile);
        assertThat(wire).hasSize((int) result.compressedBytes());

        ByteArrayInputStream in = new ByteArrayInputStream(wire);
        PageHeader header = ParquetFormat.readPageHeader(in);
        assertThat(header.type()).isEqualTo(PageType.DATA_PAGE_V2);
        DataPageHeaderV2 v2 = header.dataPageHeaderV2().orElseThrow();
        assertThat(v2.numValues()).isEqualTo(16);
        assertThat(v2.numNulls()).isZero();
        assertThat(v2.encoding()).isEqualTo(Encoding.PLAIN);

        int[] decoded = decodeIntV2(in, header, v2, Compression.zstd(3));
        assertThat(decoded).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

        io.tileverse.parquetry.format.Statistics chunkStats = result.chunkStatistics();
        assertThat(chunkStats.nullCount().orElseThrow()).isZero();
        assertThat(decodeInt(chunkStats.minValue())).isZero();
        assertThat(decodeInt(chunkStats.maxValue())).isEqualTo(15);
    }

    @Test
    void multiPageV2HasOnePageHeaderPerThreshold() throws Exception {
        int pageLimit = 4;
        int totalValues = 40;
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(pageLimit)
                .pageByteLimit(1 << 20)
                .encodingPolicy("col", EncodingPolicy.FORCE_PLAIN)
                .build();
        SchemaNode.Primitive leaf = requiredInt32("col");

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            for (int i = 0; i < totalValues; i++) {
                writer.appendInt(i, 0, 0);
            }
            result = writer.finishChunk();
        }

        int expectedPages = totalValues / pageLimit;
        assertThat(result.offsetIndex().pageLocations()).hasSize(expectedPages);
        assertThat(result.columnIndex().nullPages()).hasSize(expectedPages);

        byte[] wire = Files.readAllBytes(tempFile);
        List<Integer> decodedValues = new ArrayList<>();
        long rowAccumulator = 0L;
        long byteAccumulator = 0L;
        for (int p = 0; p < expectedPages; p++) {
            long expectedOffset = result.offsetIndex().pageLocations().get(p).offset();
            int compressedPageSize = result.offsetIndex().pageLocations().get(p).compressedPageSize();
            long firstRow = result.offsetIndex().pageLocations().get(p).firstRowIndex();

            assertThat(expectedOffset).isEqualTo(byteAccumulator);
            assertThat(firstRow).isEqualTo(rowAccumulator);

            byte[] pageWire = new byte[compressedPageSize];
            System.arraycopy(wire, (int) expectedOffset, pageWire, 0, compressedPageSize);
            ByteArrayInputStream pageIn = new ByteArrayInputStream(pageWire);
            PageHeader header = ParquetFormat.readPageHeader(pageIn);
            DataPageHeaderV2 v2 = header.dataPageHeaderV2().orElseThrow();
            assertThat(v2.numValues()).isEqualTo(pageLimit);
            int[] values = decodeIntV2(pageIn, header, v2, Compression.zstd(3));
            for (int v : values) {
                decodedValues.add(v);
            }

            rowAccumulator += pageLimit;
            byteAccumulator += compressedPageSize;
        }
        assertThat(decodedValues).hasSize(totalValues);
        for (int i = 0; i < totalValues; i++) {
            assertThat(decodedValues.get(i)).isEqualTo(i);
        }
    }

    @Test
    void v1ModeOmitsColumnAndOffsetIndex() throws Exception {
        WriteOptions options = WriteOptions.builder()
                .parquetVersion(ParquetVersion.V1_1)
                .pageValueLimit(8)
                .encodingPolicy("col", EncodingPolicy.FORCE_PLAIN)
                .build();
        SchemaNode.Primitive leaf = requiredInt32("col");

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            for (int i = 0; i < 8; i++) {
                writer.appendInt(i * 2, 0, 0);
            }
            result = writer.finishChunk();
        }

        assertThat(result.columnIndex()).isNull();
        assertThat(result.offsetIndex()).isNull();
        assertThat(result.encodings()).containsExactly(Encoding.PLAIN);

        byte[] wire = Files.readAllBytes(tempFile);
        ByteArrayInputStream in = new ByteArrayInputStream(wire);
        PageHeader header = ParquetFormat.readPageHeader(in);
        assertThat(header.type()).isEqualTo(PageType.DATA_PAGE);
        DataPageHeader v1 = header.dataPageHeader().orElseThrow();
        assertThat(v1.numValues()).isEqualTo(8);
        assertThat(v1.encoding()).isEqualTo(Encoding.PLAIN);
    }

    @Test
    void appendNullTracksDefinitionLevelsAndNullCount() throws Exception {
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(16)
                .encodingPolicy("col", EncodingPolicy.FORCE_PLAIN)
                .build();
        SchemaNode.Primitive leaf = optionalInt32("col");

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            for (int i = 0; i < 8; i++) {
                if (i % 2 == 0) {
                    writer.appendInt(i, 0, 1);
                } else {
                    writer.appendNull(0, 0);
                }
            }
            result = writer.finishChunk();
        }

        assertThat(result.numValues()).isEqualTo(8);
        assertThat(result.chunkStatistics().nullCount().orElseThrow()).isEqualTo(4L);
        assertThat(result.columnIndex().nullPages()).containsExactly(false);
        assertThat(result.columnIndex().nullCounts().orElseThrow()).containsExactly(4L);
    }

    @Test
    void perColumnCompressionOverrideUsesSnappy() throws Exception {
        WriteOptions options = WriteOptions.builder()
                .defaultCompression(Compression.zstd(3))
                .compression("col", Compression.snappy())
                .pageValueLimit(8)
                .encodingPolicy("col", EncodingPolicy.FORCE_PLAIN)
                .build();
        SchemaNode.Primitive leaf = requiredInt32("col");

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            for (int i = 0; i < 8; i++) {
                writer.appendInt(i, 0, 0);
            }
            result = writer.finishChunk();
        }

        byte[] wire = Files.readAllBytes(tempFile);
        ByteArrayInputStream in = new ByteArrayInputStream(wire);
        PageHeader header = ParquetFormat.readPageHeader(in);
        DataPageHeaderV2 v2 = header.dataPageHeaderV2().orElseThrow();
        int[] decoded = decodeIntV2(in, header, v2, Compression.snappy());
        assertThat(decoded).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
        assertThat(result.compressedBytes()).isEqualTo(wire.length);
    }

    @Test
    void bloomFilterCarriesBitsetWhenColumnIndexed() throws Exception {
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(16)
                .bloomFilter("col", new BloomFilterConfig(0.01, 100L))
                .build();
        SchemaNode.Primitive leaf = requiredInt32("col");

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            for (int i = 0; i < 16; i++) {
                writer.appendInt(i * 17, 0, 0);
            }
            result = writer.finishChunk();
        }

        MemorySegment bloomBytes = result.bloomFilterBytes();
        assertThat(bloomBytes).isNotEqualTo(MemorySegment.NULL);
        assertThat(bloomBytes.byteSize() % SplitBlockBloomFilter.BYTES_PER_BLOCK)
                .isZero();

        SplitBlockBloomFilter sbbf = new SplitBlockBloomFilter(bloomBytes);
        for (int i = 0; i < 16; i++) {
            assertThat(sbbf.mightContainInt(i * 17)).isTrue();
        }
    }

    @Test
    void indexedColumnEnablesBloomWithDefaults() throws Exception {
        WriteOptions options = WriteOptions.builder().indexedColumns("col").build();
        SchemaNode.Primitive leaf = requiredInt32("col");

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            writer.appendInt(42, 0, 0);
            result = writer.finishChunk();
        }

        assertThat(result.bloomFilterBytes()).isNotEqualTo(MemorySegment.NULL);
        SplitBlockBloomFilter sbbf = new SplitBlockBloomFilter(result.bloomFilterBytes());
        assertThat(sbbf.mightContainInt(42)).isTrue();
    }

    @Test
    void mismatchedKindThrows() throws Exception {
        WriteOptions options = WriteOptions.defaults();
        SchemaNode.Primitive leaf = requiredInt32("col");
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            assertThatThrownBy(() -> writer.appendLong(1L, 0, 0))
                    .isInstanceOf(ParquetWriteException.class)
                    .hasMessageContaining("Type mismatch");
            writer.finishChunk();
        }
    }

    @Test
    void closeWithoutFinishThrows() throws Exception {
        WriteOptions options = WriteOptions.defaults();
        SchemaNode.Primitive leaf = requiredInt32("col");
        ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile);
        writer.appendInt(1, 0, 0);
        assertThatThrownBy(writer::close)
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("without finishChunk");
    }

    @Test
    void appendNullOnRequiredColumnThrows() throws Exception {
        WriteOptions options = WriteOptions.defaults();
        SchemaNode.Primitive leaf = requiredInt32("col");
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            assertThatThrownBy(() -> writer.appendNull(0, 0))
                    .isInstanceOf(ParquetWriteException.class)
                    .hasMessageContaining("REQUIRED");
            writer.finishChunk();
        }
    }

    @Test
    void binaryColumnRoundTrips() throws Exception {
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(8)
                .encodingPolicy("name", EncodingPolicy.FORCE_PLAIN)
                .build();
        SchemaNode.Primitive leaf = requiredBinary("name");

        byte[][] inputs = {bytes("alpha"), bytes("beta"), bytes("gamma")};

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            for (byte[] v : inputs) {
                writer.appendBinary(MemorySegment.ofArray(v).asReadOnly(), 0, 0);
            }
            result = writer.finishChunk();
        }

        assertThat(result.numValues()).isEqualTo(inputs.length);
        byte[] wire = Files.readAllBytes(tempFile);
        ByteArrayInputStream in = new ByteArrayInputStream(wire);
        PageHeader header = ParquetFormat.readPageHeader(in);
        DataPageHeaderV2 v2 = header.dataPageHeaderV2().orElseThrow();
        byte[] tail = remaining(in);
        Compression codec = Compression.zstd(3);
        byte[] decompressed = decompress(codec, tail, header.uncompressedPageSize());
        byte[][] decoded = decodePlainBinary(decompressed, v2.numValues());
        assertThat(decoded).hasSameDimensionsAs(inputs);
        for (int i = 0; i < inputs.length; i++) {
            assertThat(decoded[i]).containsExactly(inputs[i]);
        }
    }

    @Test
    void dictionaryEncodesSmallRangeInt32() throws Exception {
        int pageLimit = 256;
        int totalValues = 3000;
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(pageLimit)
                .pageByteLimit(1 << 20)
                .build();
        SchemaNode.Primitive leaf = requiredInt32("col");

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            for (int i = 0; i < totalValues; i++) {
                writer.appendInt((i % 3) + 1, 0, 0);
            }
            result = writer.finishChunk();
        }

        assertThat(result.numValues()).isEqualTo(totalValues);
        assertThat(result.dictionaryPageOffset()).isZero();
        assertThat(result.encodings()).contains(Encoding.PLAIN, Encoding.RLE_DICTIONARY);

        byte[] wire = Files.readAllBytes(tempFile);
        ByteArrayInputStream in = new ByteArrayInputStream(wire);
        PageHeader dictHeader = ParquetFormat.readPageHeader(in);
        assertThat(dictHeader.type()).isEqualTo(PageType.DICTIONARY_PAGE);
        io.tileverse.parquetry.format.DictionaryPageHeader dict =
                dictHeader.dictionaryPageHeader().orElseThrow();
        assertThat(dict.numValues()).isEqualTo(3);
        assertThat(dict.encoding()).isEqualTo(Encoding.PLAIN);

        byte[] dictBytes = new byte[dictHeader.compressedPageSize()];
        int read = in.read(dictBytes);
        assertThat(read).isEqualTo(dictBytes.length);
        Compression codec = Compression.zstd(3);
        byte[] decompressedDict = decompress(codec, dictBytes, dictHeader.uncompressedPageSize());
        int[] dictValues = decodeInt32sLittleEndian(decompressedDict, dict.numValues());
        assertThat(dictValues).containsExactlyInAnyOrder(1, 2, 3);

        int decodedSoFar = 0;
        while (in.available() > 0) {
            PageHeader dataHeader = ParquetFormat.readPageHeader(in);
            assertThat(dataHeader.type()).isEqualTo(PageType.DATA_PAGE_V2);
            DataPageHeaderV2 v2 = dataHeader.dataPageHeaderV2().orElseThrow();
            assertThat(v2.encoding()).isEqualTo(Encoding.RLE_DICTIONARY);

            byte[] pageBytes = new byte[dataHeader.compressedPageSize()];
            int got = in.read(pageBytes);
            assertThat(got).isEqualTo(pageBytes.length);
            byte[] decompressedData = decompress(codec, pageBytes, dataHeader.uncompressedPageSize());
            int[] indices = decodeRleDictionaryIndices(decompressedData, v2.numValues());
            for (int idx : indices) {
                int expected = (decodedSoFar % 3) + 1;
                assertThat(dictValues[idx]).isEqualTo(expected);
                decodedSoFar++;
            }
        }
        assertThat(decodedSoFar).isEqualTo(totalValues);
    }

    @Test
    void dictionaryFallsBackToPlainOnOverflow() throws Exception {
        int distinct = 5_000;
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(512)
                .pageByteLimit(1 << 20)
                .dictionaryByteLimit(256)
                .build();
        SchemaNode.Primitive leaf = requiredInt64("col");

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            for (int i = 0; i < distinct; i++) {
                writer.appendLong(1_000_000L + i, 0, 0);
            }
            result = writer.finishChunk();
        }

        assertThat(result.dictionaryPageOffset()).isEqualTo(-1L);
        assertThat(result.encodings()).doesNotContain(Encoding.RLE_DICTIONARY, Encoding.PLAIN_DICTIONARY);
        assertThat(result.encodings()).contains(Encoding.PLAIN);

        byte[] wire = Files.readAllBytes(tempFile);
        ByteArrayInputStream in = new ByteArrayInputStream(wire);
        PageHeader first = ParquetFormat.readPageHeader(in);
        assertThat(first.type()).isEqualTo(PageType.DATA_PAGE_V2);
    }

    @Test
    void dictionaryInV1ModeUsesPlainDictionaryMarker() throws Exception {
        int totalValues = 600;
        WriteOptions options = WriteOptions.builder()
                .parquetVersion(ParquetVersion.V1_1)
                .pageValueLimit(128)
                .build();
        SchemaNode.Primitive leaf = requiredInt32("col");

        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            for (int i = 0; i < totalValues; i++) {
                writer.appendInt((i % 3) + 1, 0, 0);
            }
            writer.finishChunk();
        }

        byte[] wire = Files.readAllBytes(tempFile);
        ByteArrayInputStream in = new ByteArrayInputStream(wire);
        PageHeader dictHeader = ParquetFormat.readPageHeader(in);
        assertThat(dictHeader.type()).isEqualTo(PageType.DICTIONARY_PAGE);
        assertThat(dictHeader.dictionaryPageHeader().orElseThrow().encoding()).isEqualTo(Encoding.PLAIN_DICTIONARY);
        int dictPayloadSize = dictHeader.compressedPageSize();
        long skipped = in.skip(dictPayloadSize);
        assertThat(skipped).isEqualTo(dictPayloadSize);

        PageHeader dataHeader = ParquetFormat.readPageHeader(in);
        assertThat(dataHeader.type()).isEqualTo(PageType.DATA_PAGE);
        DataPageHeader v1 = dataHeader.dataPageHeader().orElseThrow();
        assertThat(v1.encoding()).isEqualTo(Encoding.PLAIN_DICTIONARY);
    }

    @Test
    void pageByteLimitTriggersFlushBeforeValueLimit() throws Exception {
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(1 << 20)
                .pageByteLimit(48)
                .encodingPolicy("col", EncodingPolicy.FORCE_PLAIN)
                .build();
        SchemaNode.Primitive leaf = requiredInt32("col");

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            for (int i = 0; i < 32; i++) {
                writer.appendInt(i, 0, 0);
            }
            result = writer.finishChunk();
        }

        assertThat(result.offsetIndex().pageLocations()).hasSizeGreaterThan(1);
        assertThat(result.numValues()).isEqualTo(32);
    }

    // --- helpers ---

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.REQUIRED,
                PrimitiveKind.INT32,
                OptionalInt.empty(),
                Optional.empty(),
                /* fieldId */ -1);
    }

    private static SchemaNode.Primitive optionalInt32(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.OPTIONAL,
                PrimitiveKind.INT32,
                OptionalInt.empty(),
                Optional.empty(),
                /* fieldId */ -1);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.empty(),
                /* fieldId */ -1);
    }

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.REQUIRED,
                PrimitiveKind.INT64,
                OptionalInt.empty(),
                Optional.empty(),
                /* fieldId */ -1);
    }

    private static int[] decodeRleDictionaryIndices(byte[] pageBytes, int count) {
        int bitWidth = pageBytes[0] & 0xff;
        ByteBuffer payload = ByteBuffer.wrap(pageBytes, 1, pageBytes.length - 1).order(LITTLE_ENDIAN);
        io.tileverse.parquetry.data.read.page.LevelDecoder decoder =
                new io.tileverse.parquetry.data.read.page.LevelDecoder(bitWidth);
        decoder.load(payload);
        int[] indices = new int[count];
        decoder.decode(count, indices, 0);
        return indices;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static int[] decodeIntV2(InputStream in, PageHeader header, DataPageHeaderV2 v2, Compression compression)
            throws Exception {
        byte[] tail = remaining((ByteArrayInputStream) in);
        int levelBytes = v2.repetitionLevelsByteLength() + v2.definitionLevelsByteLength();
        int compressedValueBytes = header.compressedPageSize() - levelBytes;
        int uncompressedValueBytes = header.uncompressedPageSize() - levelBytes;

        byte[] valueBytes = new byte[compressedValueBytes];
        System.arraycopy(tail, levelBytes, valueBytes, 0, compressedValueBytes);

        byte[] decompressed;
        if (compression instanceof Compression.Uncompressed) {
            decompressed = valueBytes;
        } else {
            decompressed = decompress(compression, valueBytes, uncompressedValueBytes);
        }
        return decodeInt32sLittleEndian(decompressed, v2.numValues() - v2.numNulls());
    }

    private static byte[] decompress(Compression codec, byte[] compressed, int uncompressedSize) throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment src = arena.allocate(compressed.length);
            MemorySegment.copy(compressed, 0, src, ValueLayout.JAVA_BYTE, 0L, compressed.length);
            MemorySegment dst = arena.allocate(uncompressedSize);
            codec.decompress(src, dst);
            byte[] out = new byte[uncompressedSize];
            MemorySegment.copy(dst, ValueLayout.JAVA_BYTE, 0L, out, 0, uncompressedSize);
            return out;
        }
    }

    private static byte[] remaining(ByteArrayInputStream in) {
        byte[] out = new byte[in.available()];
        int read = in.read(out, 0, out.length);
        assertThat(read).isEqualTo(out.length);
        return out;
    }

    private static int[] decodeInt32sLittleEndian(byte[] bytes, int count) {
        ByteBuffer view = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN);
        int[] out = new int[count];
        for (int i = 0; i < count; i++) {
            out[i] = view.getInt();
        }
        return out;
    }

    private static byte[][] decodePlainBinary(byte[] bytes, int count) {
        ByteBuffer view = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN);
        byte[][] out = new byte[count][];
        for (int i = 0; i < count; i++) {
            int len = view.getInt();
            byte[] value = new byte[len];
            view.get(value);
            out[i] = value;
        }
        return out;
    }

    private static int decodeInt(MemorySegment segment) {
        return segment.get(INT32, 0L);
    }
}
