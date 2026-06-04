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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.BloomFilterConfig;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.bloom.BloomFilterReader;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class BloomFilterWriteTest {

    private static final int LONG_FILTER_NDV = 10_000;
    private static final double DEFAULT_FPP = BloomFilterConfig.defaults().fpp();

    @TempDir
    Path tempDir;

    @Test
    void int64InsertionsReadBackWithoutFalseNegatives() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt64("id"));
        BloomFilterConfig cfg = new BloomFilterConfig(DEFAULT_FPP, LONG_FILTER_NDV);
        WriteOptions options = options().bloomFilter("id", cfg).build();
        Path file = tempDir.resolve("int64-bloom.parquet");

        long[] inserted = new long[LONG_FILTER_NDV];
        for (int i = 0; i < LONG_FILTER_NDV; i++) {
            inserted[i] = 1_000_000_000L + (long) i;
        }

        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (long value : inserted) {
                writer.write(rowOf(Map.of(ColumnPath.of("id"), value)));
            }
        }

        SplitBlockBloomFilter filter = loadBloomFilter(file, "id");
        for (long value : inserted) {
            assertThat(filter.mightContainLong(value)).as("present(%d)", value).isTrue();
        }

        Random rng = new Random(42L);
        int probes = 100;
        int budget = (int) Math.ceil(probes * DEFAULT_FPP * 5.0);
        int falsePositives = 0;
        for (int i = 0; i < probes; i++) {
            long candidate = rng.nextLong();
            if (candidate >= 1_000_000_000L && candidate < 1_000_000_000L + LONG_FILTER_NDV) {
                continue;
            }
            if (filter.mightContainLong(candidate)) {
                falsePositives++;
            }
        }
        assertThat(falsePositives)
                .as("false positives over %d random known-absent longs", probes)
                .isLessThanOrEqualTo(budget);
    }

    @Test
    void byteArrayInsertionsReadBackWithoutFalseNegatives() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("name"));
        BloomFilterConfig cfg = new BloomFilterConfig(0.01, 5_000L);
        WriteOptions options = options().bloomFilter("name", cfg).build();
        Path file = tempDir.resolve("binary-bloom.parquet");

        int rows = 5_000;
        byte[][] payloads = new byte[rows][];
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (int i = 0; i < rows; i++) {
                payloads[i] = ("payload-" + i).getBytes(StandardCharsets.UTF_8);
                writer.write(rowOf(Map.of(
                        ColumnPath.of("name"),
                        java.lang.foreign.MemorySegment.ofArray(payloads[i]).asReadOnly())));
            }
        }

        SplitBlockBloomFilter filter = loadBloomFilter(file, "name");
        for (int i = 0; i < rows; i++) {
            assertThat(filter.mightContainBytes(payloads[i]))
                    .as("present(payload-%d)", i)
                    .isTrue();
        }
    }

    @Test
    void columnWithoutConfiguredBloomHasNoBloomMetadata() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().build();
        Path file = tempDir.resolve("no-bloom.parquet");

        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (int i = 0; i < 10; i++) {
                writer.write(rowOf(Map.of(ColumnPath.of("id"), i)));
            }
        }

        ColumnMetaData meta = columnMetadata(file, "id");
        assertThat(meta.bloomFilterOffset()).isEmpty();
        assertThat(meta.bloomFilterLength()).isEmpty();
    }

    @Test
    void onlyConfiguredColumnGetsBloomMetadata() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("a"), requiredInt32("b"), requiredInt32("c"));
        WriteOptions options =
                options().bloomFilter("a", new BloomFilterConfig(0.01, 1_000L)).build();
        Path file = tempDir.resolve("selective-bloom.parquet");

        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (int i = 0; i < 100; i++) {
                writer.write(rowOf(Map.of(
                        ColumnPath.of("a"), i,
                        ColumnPath.of("b"), i + 1,
                        ColumnPath.of("c"), i + 2)));
            }
        }

        assertThat(columnMetadata(file, "a").bloomFilterOffset()).isPresent();
        assertThat(columnMetadata(file, "b").bloomFilterOffset()).isEmpty();
        assertThat(columnMetadata(file, "c").bloomFilterOffset()).isEmpty();
    }

    @Test
    void bloomCoexistsWithDictionaryEncoding() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("category"));
        BloomFilterConfig cfg = new BloomFilterConfig(0.01, 1_000L);
        WriteOptions options = options().bloomFilter("category", cfg).build();
        Path file = tempDir.resolve("bloom-with-dict.parquet");

        int distinctCategories = 8;
        int rowsPerCategory = 200;
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (int repeat = 0; repeat < rowsPerCategory; repeat++) {
                for (int cat = 0; cat < distinctCategories; cat++) {
                    writer.write(rowOf(Map.of(ColumnPath.of("category"), cat)));
                }
            }
        }

        ColumnMetaData meta = columnMetadata(file, "category");
        assertThat(meta.dictionaryPageOffset())
                .as("low-cardinality column should still emit a dictionary page")
                .isPresent();
        assertThat(meta.bloomFilterOffset())
                .as("bloom filter should be emitted alongside the dictionary page")
                .isPresent();

        SplitBlockBloomFilter filter = loadBloomFilter(file, "category");
        for (int cat = 0; cat < distinctCategories; cat++) {
            assertThat(filter.mightContainInt(cat)).as("present(%d)", cat).isTrue();
        }
    }

    @Test
    void bloomFilterByteSizeTracksConfiguredSizingFormula() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("value"));
        BloomFilterConfig cfg = new BloomFilterConfig(0.01, 1_000L);
        WriteOptions options = options().bloomFilter("value", cfg).build();
        Path file = tempDir.resolve("sized-bloom.parquet");

        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (int i = 0; i < 1_000; i++) {
                writer.write(rowOf(Map.of(ColumnPath.of("value"), i)));
            }
        }

        SplitBlockBloomFilter filter = loadBloomFilter(file, "value");
        int expectedBitsetBytes = optimalBitsetBytes(cfg.fpp(), cfg.ndv());
        assertThat(filter.byteSize()).isEqualTo(expectedBitsetBytes);

        ColumnMetaData meta = columnMetadata(file, "value");
        long advertisedLength = meta.bloomFilterLength().orElseThrow();
        long headerBytes = advertisedLength - expectedBitsetBytes;
        assertThat(headerBytes)
                .as("advertised bloom_filter_length minus bitset equals header byte size")
                .isPositive();
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

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static WriteRow rowOf(Map<ColumnPath, Object> values) {
        Map<ColumnPath, Object> copy = new HashMap<>(values);
        return copy::get;
    }

    private static SplitBlockBloomFilter loadBloomFilter(Path file, String columnName) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            ColumnMetaData meta = findColumn(footer, columnName);
            long offset = meta.bloomFilterOffset()
                    .orElseThrow(() -> new AssertionError("column " + columnName + " has no bloom filter"));
            int length =
                    (int) meta.bloomFilterLength().orElseThrow(() -> new AssertionError("bloom_filter_length missing"));
            return BloomFilterReader.read(source, offset, length);
        }
    }

    private static ColumnMetaData columnMetadata(Path file, String columnName) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            return findColumn(footer, columnName);
        }
    }

    private static ColumnMetaData findColumn(FileMetaData footer, String columnName) {
        for (RowGroup rg : footer.rowGroups()) {
            for (ColumnChunk chunk : rg.columns()) {
                ColumnMetaData meta = chunk.metaData().orElseThrow();
                if (meta.pathInSchema().size() == 1
                        && columnName.equals(meta.pathInSchema().get(0))) {
                    return meta;
                }
            }
        }
        throw new AssertionError("column " + columnName + " not found in footer");
    }

    // Mirrors BloomFilterBuilder's SBBF block-count sizing (numBits = -8 * ndv / ln(1 - fpp^(1/8))),
    // rounded up to a multiple of one 32-byte block, with a floor of one block.
    private static int optimalBitsetBytes(double fpp, long ndv) {
        int k = 8;
        double exponent = Math.log(fpp) / k;
        double denominator = Math.log(1.0 - Math.exp(exponent));
        double numBits = -k * (double) ndv / denominator;
        long numBytes = (long) Math.ceil(numBits / 8.0);
        long blockBytes = SplitBlockBloomFilter.BYTES_PER_BLOCK;
        long rounded = ((numBytes + blockBytes - 1) / blockBytes) * blockBytes;
        return (int) Math.max(rounded, blockBytes);
    }
}
