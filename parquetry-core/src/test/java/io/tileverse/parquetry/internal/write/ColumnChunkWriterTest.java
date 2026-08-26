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
package io.tileverse.parquetry.internal.write;

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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.BloomFilterConfig;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
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

    @Test
    void dictionaryBinaryColumnRoundTripsWithValueBufferSkipped() throws Exception {
        // Low cardinality: dictionary stays active for the whole chunk.
        List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            values.add(("city-" + (i % 8)).getBytes(StandardCharsets.UTF_8));
        }
        byte[][] readBack = writeThenReadBinaryColumn(values);
        assertThat(readBack).hasNumberOfRows(values.size());
        for (int i = 0; i < values.size(); i++) {
            assertThat(readBack[i]).containsExactly(values.get(i));
        }
    }

    @Test
    void overflowedDictionaryBinaryColumnRoundTripsWithValueBufferSkipped() throws Exception {
        // High cardinality: dictionary overflows to PLAIN mid-chunk; the fallback must still emit every value.
        List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            values.add(("unique-value-" + i).getBytes(StandardCharsets.UTF_8));
        }
        byte[][] readBack = writeThenReadBinaryColumn(values);
        assertThat(readBack).hasNumberOfRows(values.size());
        for (int i = 0; i < values.size(); i++) {
            assertThat(readBack[i]).containsExactly(values.get(i));
        }
    }

    @Test
    void geometryColumnDefaultsToPlainEncoding() throws Exception {
        SchemaNode.Primitive leaf = requiredGeometry("geometry");
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(256)
                .pageByteLimit(1 << 20)
                .build();

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            appendLowCardinalityPoints(writer, 3000);
            result = writer.finishChunk();
        }

        assertThat(result.dictionaryPageOffset()).isEqualTo(-1L);
        assertThat(result.encodings()).contains(Encoding.PLAIN);
        assertThat(result.encodings()).doesNotContain(Encoding.RLE_DICTIONARY, Encoding.PLAIN_DICTIONARY);
    }

    @Test
    void explicitAutoRestoresDictionaryOnGeometryColumn() throws Exception {
        SchemaNode.Primitive leaf = requiredGeometry("geometry");
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(256)
                .pageByteLimit(1 << 20)
                .encodingPolicy("geometry", EncodingPolicy.AUTO)
                .build();

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            appendLowCardinalityPoints(writer, 3000);
            result = writer.finishChunk();
        }

        assertThat(result.dictionaryPageOffset()).isZero();
        assertThat(result.encodings()).contains(Encoding.RLE_DICTIONARY);
    }

    @Test
    void nonGeometryBinaryColumnStillDefaultsToDictionary() throws Exception {
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                "name", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(256)
                .pageByteLimit(1 << 20)
                .build();

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            for (int i = 0; i < 3000; i++) {
                byte[] value = ("city-" + (i % 3)).getBytes(StandardCharsets.UTF_8);
                writer.appendBinary(MemorySegment.ofArray(value), 0, 0);
            }
            result = writer.finishChunk();
        }

        assertThat(result.dictionaryPageOffset()).isZero();
        assertThat(result.encodings()).contains(Encoding.RLE_DICTIONARY);
    }

    @Test
    void appendStripedReadsOnlyTheEntryCountPrefix() throws Exception {
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(16)
                .pageByteLimit(1 << 20)
                .encodingPolicy("col", EncodingPolicy.FORCE_PLAIN)
                .build();
        SchemaNode.Primitive leaf = requiredInt32("col");
        IntVector values = IntVector.materialized(new int[] {10, 20}, Validity.allValid(2));
        int[] keptOrdinals = new int[] {0, 1};
        int[] defLevels = new int[] {0, 0, 7, 7};

        ColumnChunkResult result;
        try (ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile)) {
            writer.appendStriped(values, keptOrdinals, null, defLevels, 2, 0);
            result = writer.finishChunk();
        }

        assertThat(result.numValues())
                .as("the tail beyond entryCount is stale capacity, not data")
                .isEqualTo(2);
    }

    // --- helpers ---

    private static SchemaNode.Primitive requiredGeometry(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.Geometry(Optional.empty())),
                -1);
    }

    private static void appendLowCardinalityPoints(ColumnChunkWriter writer, int count) {
        byte[][] points = {wkbPoint(1.0, 2.0), wkbPoint(3.0, 4.0), wkbPoint(5.0, 6.0)};
        for (int i = 0; i < count; i++) {
            writer.appendBinary(MemorySegment.ofArray(points[i % points.length]), 0, 0);
        }
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(1);
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
    }

    /**
     * Writes a single required {@code BYTE_ARRAY} column through the full writer under the default {@code AUTO}
     * encoding policy (dictionary active) and reads every value back through parquetry's own reader. A small dictionary
     * budget forces the high-cardinality case to overflow to {@code PLAIN} mid-chunk while the low-cardinality case
     * stays dictionary-encoded, exercising both dictionary-active code paths.
     */
    private byte[][] writeThenReadBinaryColumn(List<byte[]> values) throws Exception {
        ColumnPath col = ColumnPath.of("value");
        ParquetSchema schema = flatBinarySchema(col);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .dictionaryByteLimit(1024)
                .build();
        Path file = Files.createTempFile(tempDir, "binary-col", ".parquet");

        List<Map<ColumnPath, Object>> rows = new ArrayList<>(values.size());
        for (byte[] value : values) {
            Map<ColumnPath, Object> row = new HashMap<>(1);
            row.put(col, value);
            rows.add(row);
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }

        List<byte[]> readBack = new ArrayList<>(values.size());
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> records =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                records.forEach(record -> readBack.add(record.getBinary(col)));
            }
        }
        return readBack.toArray(new byte[0][]);
    }

    private static ParquetSchema flatBinarySchema(ColumnPath col) {
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                col.name(), Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

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
        io.tileverse.parquetry.internal.read.page.LevelDecoder decoder =
                new io.tileverse.parquetry.internal.read.page.LevelDecoder(bitWidth);
        decoder.load(MemorySegment.ofBuffer(payload));
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
