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
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Reads a dictionary-overflow column chunk back through parquetry's own reader (the default parallel decode path).
 *
 * <p>When the first pages of a chunk dictionary-encode a low-cardinality prefix and a later page overflows the
 * dictionary byte budget and falls back to PLAIN, the early {@code RLE_DICTIONARY} pages index into the chunk
 * dictionary. The dictionary page must be written for them to decode. The cross-reader {@code ParquetJavaCompatIT}
 * guards the on-disk shape against parquet-java; this guards the same file against parquetry's reader, where the
 * regression threw {@code "Dictionary-encoded data page requires a loaded Dictionary"} from {@code BatchColumnReader}.
 */
class DictionaryOverflowReadbackTest {

    private static final int ROWS = 100;
    private static final int DICT_PREFIX = 60;

    @TempDir
    Path tempDir;

    @Test
    void int64OverflowChunkReadsBackThroughParquetry() throws Exception {
        ColumnPath col = ColumnPath.of("id");
        Path file = writeOverflowChunk(col, PrimitiveKind.INT64, i -> (long) (i < DICT_PREFIX ? i % 4 : 1_000 + i));

        List<Long> expected = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            expected.add((long) (i < DICT_PREFIX ? i % 4 : 1_000 + i));
        }
        assertThat(readColumn(file, rec -> rec.getLong(col))).isEqualTo(expected);
    }

    @Test
    void binaryOverflowChunkReadsBackThroughParquetry() throws Exception {
        ColumnPath col = ColumnPath.of("label");
        Path file = writeOverflowChunk(col, PrimitiveKind.BYTE_ARRAY, i -> binary(labelFor(i)));

        List<String> expected = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            expected.add(labelFor(i));
        }
        assertThat(readColumn(file, rec -> rec.getString(col))).isEqualTo(expected);
    }

    private Path writeOverflowChunk(ColumnPath col, PrimitiveKind kind, IntFunction<Object> valueAt) throws Exception {
        ParquetSchema schema = flatSchema(required(col.name(), kind));
        // A tiny dictionary budget plus small pages forces the overflow after a few RLE_DICTIONARY pages, with the
        // default Auto encoding and ZSTD compression - the shape a caller hits without tuning anything.
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(10)
                .dictionaryByteLimit(40)
                .build();
        Path file = tempDir.resolve(kind + "-overflow.parquet");
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            rows.add(singleColumnRow(col, valueAt.apply(i)));
        }
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        assertThat(dictionaryPageWritten(file, col))
                .as("the overflow chunk must keep its dictionary page")
                .isTrue();
        return file;
    }

    private static <T> List<T> readColumn(Path file, java.util.function.Function<ParquetRecord, T> get) {
        List<T> values = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader dataset = ParquetReader.open(source);
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.forEach(rec -> values.add(get.apply(rec)));
            }
        }
        return values;
    }

    private static boolean dictionaryPageWritten(Path file, ColumnPath col) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            for (RowGroup rg : footer.rowGroups()) {
                for (ColumnChunk chunk : rg.columns()) {
                    ColumnMetaData meta = chunk.metaData().orElse(null);
                    if (meta != null
                            && ColumnPath.of(meta.pathInSchema()).equals(col)
                            && meta.dictionaryPageOffset().isPresent()) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static String labelFor(int i) {
        return i < DICT_PREFIX ? "dup-" + (i % 4) : "uniq-" + i;
    }

    private static Object binary(String value) {
        return MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Map<ColumnPath, Object> singleColumnRow(ColumnPath col, Object value) {
        Map<ColumnPath, Object> row = new HashMap<>(1);
        row.put(col, value);
        return row;
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive leaf) {
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive required(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }
}
