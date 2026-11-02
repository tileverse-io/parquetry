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
package io.tileverse.parquetry.data;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * A dictionary is decoded when a page uses it. A corrupt dictionary page fails the reads reaching a page using it, and
 * the reads staying away from such pages return their rows.
 *
 * <p>The file comes from parquetry's writer with a dictionary budget of {@value #DICTIONARY_BYTE_LIMIT} bytes. The
 * label and score columns repeat four values over the first {@value #DICTIONARY_ROWS} rows, and each row after them
 * holds a new value. Their first pages are dictionary-encoded, and the budget sends the pages after them to PLAIN.
 *
 * <p>A corrupt copy overwrites the head of a dictionary page's ZSTD frame, and the codec rejects the page. Corrupting
 * the PLAIN bytes of an uncompressed copy would decode without a failure.
 */
class DictionaryWhenNeededIT {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath LABEL = ColumnPath.of("label");
    private static final ColumnPath SCORE = ColumnPath.of("score");

    private static final int ROWS = 100;
    private static final int PAGE_VALUES = 10;
    private static final int DICTIONARY_ROWS = 60;
    private static final int DICTIONARY_BYTE_LIMIT = 40;

    /** Holds for the rows of the first page. */
    private static final Predicate ON_A_DICTIONARY_PAGE = col(ID).lt((long) PAGE_VALUES);

    /** Holds for the rows of the PLAIN pages. */
    private static final Predicate ON_PLAIN_PAGES = col(ID).gtEq((long) DICTIONARY_ROWS);

    private static final ReadOptions NO_PAGE_INDEX =
            ReadOptions.builder().useColumnIndexFilter(false).build();

    @TempDir
    static Path tempDir;

    private static Path file;

    @BeforeAll
    static void writeTheFile() throws IOException {
        ParquetSchema schema = WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf(ID.name(), PrimitiveKind.INT64),
                WriteFixtures.requiredLeaf(LABEL.name(), PrimitiveKind.BYTE_ARRAY),
                WriteFixtures.requiredLeaf(SCORE.name(), PrimitiveKind.DOUBLE));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(PAGE_VALUES)
                .dictionaryByteLimit(DICTIONARY_BYTE_LIMIT)
                .build();
        file = WriteFixtures.writeRows(tempDir.resolve("mixed.parquet"), schema, options, rowsToWrite());
    }

    private static List<Map<ColumnPath, Object>> rowsToWrite() {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(ROWS);
        for (int row = 0; row < ROWS; row++) {
            Map<ColumnPath, Object> values = new HashMap<>();
            values.put(ID, (long) row);
            values.put(LABEL, MemorySegment.ofArray(labelOf(row).getBytes(StandardCharsets.UTF_8)));
            values.put(SCORE, scoreOf(row));
            rows.add(values);
        }
        return rows;
    }

    private static String labelOf(int row) {
        return row < DICTIONARY_ROWS ? "dup-" + (row % 4) : "uniq-" + row;
    }

    private static double scoreOf(int row) {
        return row < DICTIONARY_ROWS ? row % 4 : 1_000.0 + row;
    }

    @Test
    void corruptDictionaryFailsItsRows() throws IOException {
        Path corrupt = copyWithCorruptDictionary(LABEL);
        List<String> firstLabels = cells(0, PAGE_VALUES, DictionaryWhenNeededIT::labelOf);
        List<String> plainLabels = cells(DICTIONARY_ROWS, ROWS, DictionaryWhenNeededIT::labelOf);

        assertThat(labels(file, ON_A_DICTIONARY_PAGE, ReadOptions.DEFAULTS)).isEqualTo(firstLabels);
        assertThat(labels(corrupt, ON_PLAIN_PAGES, ReadOptions.DEFAULTS)).isEqualTo(plainLabels);
        assertThat(labels(corrupt, ON_PLAIN_PAGES, NO_PAGE_INDEX)).isEqualTo(plainLabels);
        assertThatThrownBy(() -> labels(corrupt, ON_A_DICTIONARY_PAGE, ReadOptions.DEFAULTS))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("dictionary page");
    }

    // The first condition leaves no row on the dictionary-encoded pages of the column read by the second.
    @Test
    void laterStageNeedsNoDictionary() throws IOException {
        Path corrupt = copyWithCorruptDictionary(LABEL);
        String label = labelOf(70);
        Predicate oneLabel = Pred.and(ON_PLAIN_PAGES, col(LABEL).eq(label));

        assertThat(labels(corrupt, oneLabel, NO_PAGE_INDEX)).containsExactly(label);
    }

    @Test
    void fixedWidthDictionaryDecodesToo() throws IOException {
        Path corrupt = copyWithCorruptDictionary(SCORE);
        List<Double> firstScores = cells(0, PAGE_VALUES, DictionaryWhenNeededIT::scoreOf);
        List<Double> plainScores = cells(DICTIONARY_ROWS, ROWS, DictionaryWhenNeededIT::scoreOf);

        assertThat(scores(file, ON_A_DICTIONARY_PAGE, ReadOptions.DEFAULTS)).isEqualTo(firstScores);
        assertThat(scores(corrupt, ON_PLAIN_PAGES, ReadOptions.DEFAULTS)).isEqualTo(plainScores);
        assertThat(scores(corrupt, ON_PLAIN_PAGES, NO_PAGE_INDEX)).isEqualTo(plainScores);
        assertThatThrownBy(() -> scores(corrupt, ON_A_DICTIONARY_PAGE, ReadOptions.DEFAULTS))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("dictionary page");
    }

    private static <T> List<T> cells(int firstRow, int endRow, IntFunction<T> cellOf) {
        return IntStream.range(firstRow, endRow).mapToObj(cellOf).toList();
    }

    private static List<String> labels(Path parquet, Predicate predicate, ReadOptions options) {
        return read(parquet, predicate, options, LABEL, ParquetRecord::getString);
    }

    private static List<Double> scores(Path parquet, Predicate predicate, ReadOptions options) {
        return read(parquet, predicate, options, SCORE, ParquetRecord::getDouble);
    }

    /** The cells of {@code column} in the rows held by {@code predicate}, read beside the id column. */
    private static <T> List<T> read(
            Path parquet,
            Predicate predicate,
            ReadOptions options,
            ColumnPath column,
            BiFunction<ParquetRecord, ColumnPath, T> cellOf) {
        Projection projection = Projection.ofPhysical(List.of(ID, column));
        try (ByteRangeSource source = ByteRangeSource.ofFile(parquet)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(predicate, projection, options)) {
                return rows.map(row -> cellOf.apply(row, column)).toList();
            }
        }
    }

    /** A copy of the file with the first bytes of the dictionary page payload of {@code column} overwritten. */
    private static Path copyWithCorruptDictionary(ColumnPath column) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        int payload = dictionaryPayloadOffset(bytes, column);
        Arrays.fill(bytes, payload, payload + Long.BYTES, (byte) 0xFF);
        Path copy = Files.createTempFile(tempDir, "corrupt-", ".parquet");
        return Files.write(copy, bytes);
    }

    /** The offset of the first byte after the header of the dictionary page of {@code column}. */
    private static int dictionaryPayloadOffset(byte[] bytes, ColumnPath column) {
        int pageOffset = Math.toIntExact(dictionaryPageOffset(column));
        ByteArrayInputStream page = new ByteArrayInputStream(bytes, pageOffset, bytes.length - pageOffset);
        ParquetFormat.readPageHeader(page);
        return bytes.length - page.available();
    }

    private static long dictionaryPageOffset(ColumnPath column) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            List<ColumnChunk> chunks = footer.rowGroups().get(0).columns();
            for (ColumnChunk chunk : chunks) {
                ColumnMetaData meta = chunk.metaData().orElseThrow();
                if (ColumnPath.of(meta.pathInSchema()).equals(column)) {
                    return meta.dictionaryPageOffset().orElseThrow();
                }
            }
            throw new IllegalArgumentException("The file has no column " + column.dot());
        }
    }
}
