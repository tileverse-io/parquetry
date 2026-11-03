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
import java.io.ByteArrayOutputStream;
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
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.EncodingStats;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.internal.filter.LearnedPages;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.testsupport.CountingByteRangeSource;
import io.tileverse.parquetry.testsupport.FooterRewrite;

/**
 * A dictionary is decoded when a page uses it, and a narrowed fetch reads it when a fetched page uses it. A dictionary
 * fetched as its own prefix decodes ahead when a fetched page uses it, and a read decodes the later dictionaries of a
 * column ahead once it has used one. A corrupt dictionary page fails the reads reaching a page using it, and the reads
 * staying away from such pages return their rows.
 *
 * <p>Each file holds the same {@value #ROWS} rows in pages of {@value #PAGE_VALUES} rows, under a small dictionary
 * budget. The label and score columns repeat four values over the first {@value #DICTIONARY_ROWS} rows, and each row
 * after them holds a new value. Their first pages are dictionary-encoded, and the budget sends the pages after them to
 * PLAIN.
 *
 * <ul>
 *   <li>The main file comes from parquetry's writer. It has a page index and no encoding statistics.
 *   <li>The headers file comes from parquetry's writer in Parquet 1 mode. It has no page index, and a read learns the
 *       encodings of its pages from their headers.
 *   <li>The declared file comes from parquet-java, without the score column. It has a page index and encoding
 *       statistics, and its footer gives the count of dictionary-encoded pages.
 *   <li>The row groups file comes from parquetry's writer. It repeats those rows in {@value #ROW_GROUPS} row groups,
 *       with a new id in each row. A read of two or more of its row groups can decode a dictionary ahead.
 * </ul>
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
    private static final int DICTIONARY_PAGES = DICTIONARY_ROWS / PAGE_VALUES;
    private static final int PAGES = ROWS / PAGE_VALUES;
    private static final int DICTIONARY_BYTE_LIMIT = 40;
    private static final int ROW_GROUPS = 8;
    private static final int ALL_GROUPS_ROWS = ROW_GROUPS * ROWS;

    /** The smallest dictionary budget accepted by parquet-java. */
    private static final int PARQUET_JAVA_DICTIONARY_BYTE_LIMIT = 64;

    private static final String DECLARED_SCHEMA = """
            {"type":"record","name":"Row","fields":[
              {"name":"id","type":"long"},
              {"name":"label","type":"string"}
            ]}""";

    /** Holds for the rows of the first page. */
    private static final Predicate ON_A_DICTIONARY_PAGE = col(ID).lt((long) PAGE_VALUES);

    /** Holds for the rows of the PLAIN pages. */
    private static final Predicate ON_PLAIN_PAGES = col(ID).gtEq((long) DICTIONARY_ROWS);

    private static final ReadOptions NO_PAGE_INDEX =
            ReadOptions.builder().useColumnIndexFilter(false).build();

    /** Fetches each row group after a reader finished the row group before it. */
    private static final ParquetRuntime SERIAL =
            ParquetRuntime.defaultRuntime().withPrefetchDepth(0).withMaxDecodeAhead(0);

    @TempDir
    static Path tempDir;

    private static Path file;
    private static Path headersFile;
    private static Path declaredFile;
    private static Path rowGroupsFile;

    @BeforeAll
    static void writeTheFiles() throws IOException {
        file = writeWithParquetry("mixed.parquet", ParquetVersion.V2_0);
        headersFile = writeWithParquetry("headers.parquet", ParquetVersion.V1_1);
        declaredFile = writeWithParquetJava("declared.parquet");
        rowGroupsFile = writeRowGroups("row-groups.parquet");
    }

    @BeforeEach
    void clearTheCaches() {
        ParquetCaches.clearAll();
    }

    private static Path writeWithParquetry(String fileName, ParquetVersion version) throws IOException {
        ParquetSchema schema = WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf(ID.name(), PrimitiveKind.INT64),
                WriteFixtures.requiredLeaf(LABEL.name(), PrimitiveKind.BYTE_ARRAY),
                WriteFixtures.requiredLeaf(SCORE.name(), PrimitiveKind.DOUBLE));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .parquetVersion(version)
                .pageValueLimit(PAGE_VALUES)
                .dictionaryByteLimit(DICTIONARY_BYTE_LIMIT)
                .build();
        return WriteFixtures.writeRows(tempDir.resolve(fileName), schema, options, rowsToWrite());
    }

    private static Path writeRowGroups(String fileName) throws IOException {
        ParquetSchema schema = WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf(ID.name(), PrimitiveKind.INT64),
                WriteFixtures.requiredLeaf(LABEL.name(), PrimitiveKind.BYTE_ARRAY),
                WriteFixtures.requiredLeaf(SCORE.name(), PrimitiveKind.DOUBLE));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS))
                .pageValueLimit(PAGE_VALUES)
                .dictionaryByteLimit(DICTIONARY_BYTE_LIMIT)
                .build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(ALL_GROUPS_ROWS);
        for (int group = 0; group < ROW_GROUPS; group++) {
            for (Map<ColumnPath, Object> row : rowsToWrite()) {
                Map<ColumnPath, Object> values = new HashMap<>(row);
                values.put(ID, (long) rows.size());
                rows.add(values);
            }
        }
        return WriteFixtures.writeRows(tempDir.resolve(fileName), schema, options, rows);
    }

    private static Path writeWithParquetJava(String fileName) throws IOException {
        Path target = tempDir.resolve(fileName);
        Schema schema = new Schema.Parser().parse(DECLARED_SCHEMA);
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(target))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withPageRowCountLimit(PAGE_VALUES)
                // The writer checks its first page at row 100 by default, and that page would hold each row.
                .withMinRowCountForPageSizeCheck(PAGE_VALUES)
                .withDictionaryPageSize(PARQUET_JAVA_DICTIONARY_BYTE_LIMIT)
                .build()) {
            for (int row = 0; row < ROWS; row++) {
                GenericData.Record declared = new GenericData.Record(schema);
                declared.put(ID.name(), (long) row);
                declared.put(LABEL.name(), labelOf(row));
                writer.write(declared);
            }
        }
        return target;
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

    // The page index narrows the reads by default, and a fetch of a page using the corrupt prefix decodes it ahead.
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

    @Test
    void secondTouchLeavesItOut() {
        try (CountingByteRangeSource source = named(headersFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            touch(reader, source, ON_PLAIN_PAGES);
            Touch second = touch(reader, source, ON_PLAIN_PAGES);

            long pages = learnedPageBytes(reader, source, ID, DICTIONARY_ROWS, ROWS)
                    + learnedPageBytes(reader, source, LABEL, DICTIONARY_ROWS, ROWS);
            assertThat(second.labels()).isEqualTo(plainLabels());
            assertThat(second.bytes()).isEqualTo(pages);
        }
    }

    @Test
    void learnedDictionaryPageKeepsIt() {
        try (CountingByteRangeSource source = named(headersFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            touch(reader, source, ON_A_DICTIONARY_PAGE);
            Touch second = touch(reader, source, ON_A_DICTIONARY_PAGE);

            long pages = learnedPageBytes(reader, source, ID, 0, PAGE_VALUES)
                    + learnedPageBytes(reader, source, LABEL, 0, PAGE_VALUES);
            assertThat(second.labels()).isEqualTo(firstLabels());
            assertThat(second.bytes()).isEqualTo(pages + dictionaryBytes(headersFile, LABEL));
        }
    }

    // The declared path needs a real file with a page index, encoding statistics and a fallback to PLAIN in a chunk.
    @Test
    void parquetJavaWritesTheShape() {
        ColumnChunk label = chunkOf(declaredFile, LABEL);

        assertThat(label.offsetIndexOffset()).isPresent();
        assertThat(label.metaData().orElseThrow().encodingStats())
                .containsExactlyInAnyOrderElementsOf(statsCounting(DICTIONARY_PAGES));
    }

    @Test
    void declaredIndexLeavesItUnread() throws IOException {
        Path noCount = rewritten(declaredFile, List.of());

        Touch leftOut = firstTouch(declaredFile, ON_PLAIN_PAGES);
        Touch fetched = firstTouch(noCount, ON_PLAIN_PAGES);

        assertThat(leftOut.labels()).isEqualTo(plainLabels());
        assertThat(fetched.labels()).isEqualTo(plainLabels());
        assertThat(fetched.bytes() - leftOut.bytes()).isEqualTo(dictionaryBytes(declaredFile, LABEL));
        assertThat(leftOut.requests()).isLessThanOrEqualTo(fetched.requests());
    }

    @Test
    void declaredDictionaryPageReadsIt() throws IOException {
        Path noCount = rewritten(declaredFile, List.of());

        Touch counted = firstTouch(declaredFile, ON_A_DICTIONARY_PAGE);
        Touch fetched = firstTouch(noCount, ON_A_DICTIONARY_PAGE);

        assertThat(counted.labels()).isEqualTo(firstLabels());
        assertThat(counted.bytes()).isEqualTo(fetched.bytes());
        assertThat(counted.requests()).isEqualTo(fetched.requests());
    }

    // The footer counts no dictionary-encoded page. In the true file, page 0 and the prefix share one request.
    @Test
    void aWrongCountStillReadsEachRow() throws IOException {
        Path wrong = rewritten(declaredFile, statsCounting(0));

        Touch truth = firstTouch(declaredFile, ON_A_DICTIONARY_PAGE);
        Touch claimed = firstTouch(wrong, ON_A_DICTIONARY_PAGE);

        assertThat(claimed.labels()).isEqualTo(firstLabels());
        assertThat(claimed.labels()).isEqualTo(truth.labels());
        assertThat(claimed.bytes()).isEqualTo(truth.bytes());
        assertThat(claimed.requests()).isEqualTo(truth.requests() + 1);
    }

    // No surviving page abuts the prefix, and the true file reads it in a request of its own.
    @Test
    void aWrongCountPastTheFirstPage() throws IOException {
        Path wrong = rewritten(declaredFile, statsCounting(2));
        Predicate fifthPage = Pred.and(col(ID).gtEq(40L), col(ID).lt(50L));

        Touch truth = firstTouch(declaredFile, fifthPage);
        Touch claimed = firstTouch(wrong, fifthPage);

        assertThat(claimed.labels()).isEqualTo(cells(40, 50, DictionaryWhenNeededIT::labelOf));
        assertThat(claimed.labels()).isEqualTo(truth.labels());
        assertThat(claimed.bytes()).isEqualTo(truth.bytes());
        assertThat(claimed.requests()).isEqualTo(truth.requests());
    }

    // The footer counts the pages right, and the pages past the count use the dictionary.
    @Test
    void plainPagesFirstKeepEachRow() throws IOException {
        Path plainFirst = copyWithPlainPagesFirst();

        Touch lastPages = firstTouch(plainFirst, ON_PLAIN_PAGES);

        assertThat(lastPages.labels()).isEqualTo(cells(DICTIONARY_ROWS, ROWS, DictionaryWhenNeededIT::rotatedLabelOf));
    }

    // Page 2 is PLAIN and page 7 uses the dictionary. The check must walk past the first run to find it.
    @Test
    void aLaterRunUsesTheDictionary() throws IOException {
        Path wrong = rewritten(copyWithPlainPagesFirst(), statsCounting(2));
        Predicate twoPages = Pred.or(col(ID).eq(25L), col(ID).eq(75L));

        Touch separatePages = firstTouch(wrong, twoPages);

        assertThat(separatePages.labels()).containsExactly(rotatedLabelOf(25), rotatedLabelOf(75));
    }

    @Test
    void noCountReadsAsBefore() throws IOException {
        Path counted = rewritten(file, statsCounting(DICTIONARY_PAGES));

        Touch uncounted = firstTouch(file, ON_PLAIN_PAGES);
        Touch leftOut = firstTouch(counted, ON_PLAIN_PAGES);

        assertThat(uncounted.labels()).isEqualTo(plainLabels());
        assertThat(uncounted.labels()).isEqualTo(leftOut.labels());
        assertThat(uncounted.bytes() - leftOut.bytes()).isEqualTo(dictionaryBytes(file, LABEL));
    }

    static Stream<Predicate> filtersOnTheLabel() {
        Predicate oneLabel = col(LABEL).eq(labelOf(75));
        return Stream.of(oneLabel, Pred.and(ON_PLAIN_PAGES, oneLabel));
    }

    // The label column is read to filter, alone and in a later stage, and its dictionary stays out of the read.
    @ParameterizedTest
    @MethodSource("filtersOnTheLabel")
    void filterOnALeftOutColumn(Predicate predicate) throws IOException {
        Path noCount = rewritten(declaredFile, List.of());

        Touch leftOut = firstTouch(declaredFile, predicate);
        Touch fetched = firstTouch(noCount, predicate);

        assertThat(leftOut.labels()).containsExactly(labelOf(75));
        assertThat(fetched.labels()).isEqualTo(leftOut.labels());
        assertThat(fetched.bytes() - leftOut.bytes()).isEqualTo(dictionaryBytes(declaredFile, LABEL));
    }

    static Stream<Arguments> readsOfTheRowGroups() {
        // each vector is a predicate and the rows held by it
        IntPredicate oneRepeatedLabel = row -> row % ROWS < DICTIONARY_ROWS && row % 4 == 1;
        return Stream.of(
                Arguments.of(col(ID).gtEq(0L), (IntPredicate) _ -> true),
                Arguments.of(col(LABEL).eq(labelOf(1)), oneRepeatedLabel),
                Arguments.of(
                        Pred.and(col(ID).gtEq(50L), col(LABEL).eq(labelOf(1))),
                        oneRepeatedLabel.and(row -> row >= 50)));
    }

    // The serial read reaches the fetch of each later row group after a reader used the dictionaries of the columns.
    @ParameterizedTest
    @MethodSource("readsOfTheRowGroups")
    void decodeAheadKeepsEachRow(Predicate predicate, IntPredicate holds) {
        List<String> written = IntStream.range(0, ALL_GROUPS_ROWS)
                .filter(holds)
                .mapToObj(row -> textOf(row, labelOf(row % ROWS), scoreOf(row % ROWS)))
                .toList();

        List<String> serial = rowsOfTheGroups(predicate, SERIAL);
        List<String> parallel = rowsOfTheGroups(predicate, ParquetRuntime.defaultRuntime());

        assertThat(serial).isEqualTo(written);
        assertThat(parallel).isEqualTo(serial);
    }

    // The first row group uses the dictionary of the label, and the fetch of the second decodes its corrupt page ahead.
    @Test
    void corruptPageDecodedAhead() throws IOException {
        Path corrupt = copyWithCorruptDictionary(rowGroupsFile, 1, LABEL);
        Predicate secondGroupPlainPages = Pred.and(col(ID).gtEq((long) ROWS + DICTIONARY_ROWS), col(ID).lt(2L * ROWS));
        Predicate secondGroupFirstPage = Pred.and(col(ID).gtEq((long) ROWS), col(ID).lt((long) ROWS + PAGE_VALUES));
        Predicate neverReaching = Pred.or(ON_A_DICTIONARY_PAGE, secondGroupPlainPages);
        Predicate reaching = Pred.or(ON_A_DICTIONARY_PAGE, secondGroupFirstPage);
        List<String> rowsLeft =
                Stream.concat(firstLabels().stream(), plainLabels().stream()).toList();

        assertThat(serialLabels(corrupt, neverReaching, ReadOptions.DEFAULTS)).isEqualTo(rowsLeft);
        assertThat(serialLabels(corrupt, neverReaching, NO_PAGE_INDEX)).isEqualTo(rowsLeft);
        assertThatThrownBy(() -> serialLabels(corrupt, reaching, ReadOptions.DEFAULTS))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("dictionary page");
    }

    /**
     * A copy of the declared file with the PLAIN pages of the label column ahead of its dictionary-encoded pages. No
     * writer checked so far orders them this way, and the format allows it. The id column stays as written, and the
     * label of a row becomes {@link #rotatedLabelOf}. The new offset index of the column fits the bytes of the old one,
     * because the offsets of this small file keep the length of their encoding.
     */
    private static Path copyWithPlainPagesFirst() throws IOException {
        byte[] bytes = Files.readAllBytes(declaredFile);
        ColumnChunk label = chunkOf(declaredFile, LABEL);
        int indexOffset = Math.toIntExact(label.offsetIndexOffset().orElseThrow());
        int indexLength = label.offsetIndexLength().orElseThrow();
        MemorySegment section = MemorySegment.ofArray(bytes).asSlice(indexOffset, indexLength);
        OffsetIndex index = ParquetFormat.readOffsetIndex(section);
        List<PageLocation> pages = index.pageLocations();
        int firstPage = Math.toIntExact(pages.getFirst().offset());

        ByteArrayOutputStream rotated = new ByteArrayOutputStream();
        List<PageLocation> placed = new ArrayList<>(PAGES);
        for (int ordinal = 0; ordinal < PAGES; ordinal++) {
            PageLocation page = pages.get((ordinal + DICTIONARY_PAGES) % PAGES);
            long firstRow = (long) ordinal * PAGE_VALUES;
            placed.add(new PageLocation(firstPage + rotated.size(), page.compressedPageSize(), firstRow));
            rotated.write(bytes, Math.toIntExact(page.offset()), page.compressedPageSize());
        }
        ByteArrayOutputStream rewritten = new ByteArrayOutputStream();
        ParquetFormat.writeOffsetIndex(rewritten, new OffsetIndex(placed, index.unencodedByteArrayDataBytes()));
        assertThat(rewritten.size()).as("the bytes of the offset index").isEqualTo(indexLength);

        System.arraycopy(rotated.toByteArray(), 0, bytes, firstPage, rotated.size());
        System.arraycopy(rewritten.toByteArray(), 0, bytes, indexOffset, indexLength);
        return Files.write(Files.createTempFile(tempDir, "plain-first-", ".parquet"), bytes);
    }

    /** The label of row {@code row} of the copy with its PLAIN pages first. */
    private static String rotatedLabelOf(int row) {
        return labelOf((row + DICTIONARY_ROWS) % ROWS);
    }

    private static List<String> firstLabels() {
        return cells(0, PAGE_VALUES, DictionaryWhenNeededIT::labelOf);
    }

    private static List<String> plainLabels() {
        return cells(DICTIONARY_ROWS, ROWS, DictionaryWhenNeededIT::labelOf);
    }

    /** What one read returned, and what it read from the file. */
    private record Touch(long bytes, long requests, List<String> labels) {}

    /** The first read of {@code predicate} over {@code parquet}, opened anew. */
    private static Touch firstTouch(Path parquet, Predicate predicate) {
        try (CountingByteRangeSource source = named(parquet)) {
            return touch(ParquetFileReader.open(source), source, predicate);
        }
    }

    private static Touch touch(ParquetFileReader reader, CountingByteRangeSource source, Predicate predicate) {
        source.reset();
        List<String> labels = read(reader, predicate, ReadOptions.DEFAULTS, LABEL, ParquetRecord::getString);
        return new Touch(source.bytes(), source.requests(), labels);
    }

    private static CountingByteRangeSource named(Path parquet) {
        return CountingByteRangeSource.named(ByteRangeSource.ofFile(parquet));
    }

    /**
     * The bytes of the learned pages of {@code column} starting in the rows from {@code firstRow} up to, and not
     * including, {@code endRow}.
     */
    private static long learnedPageBytes(
            ParquetFileReader reader, ByteRangeSource source, ColumnPath column, long firstRow, long endRow) {
        String sourceId = source.sourceIdentifier().orElseThrow();
        int ordinal = reader.footer().leafIndex().ordinalOf(column);
        LearnedPages pages = FooterMetadataCache.learnedPages(sourceId, source.size(), 0, ordinal)
                .orElseThrow();
        long bytes = 0;
        for (int page = 0; page < pages.pageCount(); page++) {
            if (pages.firstRow(page) >= firstRow && pages.firstRow(page) < endRow) {
                bytes += pages.size(page);
            }
        }
        return bytes;
    }

    /** The bytes between the dictionary page of {@code column} and its first data page. */
    private static long dictionaryBytes(Path parquet, ColumnPath column) {
        ColumnMetaData meta = chunkOf(parquet, column).metaData().orElseThrow();
        return meta.dataPageOffset() - meta.dictionaryPageOffset().orElseThrow();
    }

    /** A copy of {@code parquet} with {@code stats} as the encoding statistics of the label column. */
    private static Path rewritten(Path parquet, List<EncodingStats> stats) throws IOException {
        Path copy = Files.createTempFile(tempDir, "rewritten-", ".parquet");
        return FooterRewrite.rewrite(parquet, copy, FooterRewrite.encodingStats(LABEL, 0, stats));
    }

    /** The encoding statistics of parquet-java for the label column, with a count of {@code dictionaryPages}. */
    private static List<EncodingStats> statsCounting(int dictionaryPages) {
        return List.of(
                new EncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN_DICTIONARY, 1),
                new EncodingStats(PageType.DATA_PAGE, Encoding.PLAIN_DICTIONARY, dictionaryPages),
                new EncodingStats(PageType.DATA_PAGE, Encoding.PLAIN, PAGES - dictionaryPages));
    }

    private static List<String> rowsOfTheGroups(Predicate predicate, ParquetRuntime runtime) {
        Projection projection = Projection.ofPhysical(List.of(ID, LABEL, SCORE));
        try (ByteRangeSource source = ByteRangeSource.ofFile(rowGroupsFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source, runtime, Optional.empty());
            try (Stream<ParquetRecord> rows = reader.read(predicate, projection, ReadOptions.DEFAULTS)) {
                return rows.map(row -> textOf(row.getLong(ID), row.getString(LABEL), row.getDouble(SCORE)))
                        .toList();
            }
        }
    }

    private static String textOf(long id, String label, double score) {
        return id + " " + label + " " + score;
    }

    private static List<String> serialLabels(Path parquet, Predicate predicate, ReadOptions options) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(parquet)) {
            ParquetFileReader reader = ParquetFileReader.open(source, SERIAL, Optional.empty());
            return read(reader, predicate, options, LABEL, ParquetRecord::getString);
        }
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
        try (ByteRangeSource source = ByteRangeSource.ofFile(parquet)) {
            return read(ParquetFileReader.open(source), predicate, options, column, cellOf);
        }
    }

    private static <T> List<T> read(
            ParquetFileReader reader,
            Predicate predicate,
            ReadOptions options,
            ColumnPath column,
            BiFunction<ParquetRecord, ColumnPath, T> cellOf) {
        Projection projection = Projection.ofPhysical(List.of(ID, column));
        try (Stream<ParquetRecord> rows = reader.read(predicate, projection, options)) {
            return rows.map(row -> cellOf.apply(row, column)).toList();
        }
    }

    /** A copy of the file with the first bytes of the dictionary page payload of {@code column} overwritten. */
    private static Path copyWithCorruptDictionary(ColumnPath column) throws IOException {
        return copyWithCorruptDictionary(file, 0, column);
    }

    /**
     * A copy of {@code parquet} with the first bytes of the dictionary page payload of {@code column} in row group
     * {@code rowGroup} overwritten.
     */
    private static Path copyWithCorruptDictionary(Path parquet, int rowGroup, ColumnPath column) throws IOException {
        byte[] bytes = Files.readAllBytes(parquet);
        int payload = dictionaryPayloadOffset(bytes, chunkOf(parquet, rowGroup, column));
        Arrays.fill(bytes, payload, payload + Long.BYTES, (byte) 0xFF);
        Path copy = Files.createTempFile(tempDir, "corrupt-", ".parquet");
        return Files.write(copy, bytes);
    }

    /** The offset of the first byte after the header of the dictionary page of {@code chunk}. */
    private static int dictionaryPayloadOffset(byte[] bytes, ColumnChunk chunk) {
        long dictionaryPage =
                chunk.metaData().orElseThrow().dictionaryPageOffset().orElseThrow();
        int pageOffset = Math.toIntExact(dictionaryPage);
        ByteArrayInputStream page = new ByteArrayInputStream(bytes, pageOffset, bytes.length - pageOffset);
        ParquetFormat.readPageHeader(page);
        return bytes.length - page.available();
    }

    private static ColumnChunk chunkOf(Path parquet, ColumnPath column) {
        return chunkOf(parquet, 0, column);
    }

    private static ColumnChunk chunkOf(Path parquet, int rowGroup, ColumnPath column) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(parquet)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            List<ColumnChunk> chunks = footer.rowGroups().get(rowGroup).columns();
            for (ColumnChunk chunk : chunks) {
                ColumnMetaData meta = chunk.metaData().orElseThrow();
                if (ColumnPath.of(meta.pathInSchema()).equals(column)) {
                    return chunk;
                }
            }
            throw new IllegalArgumentException("The file has no column " + column.dot());
        }
    }
}
