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
package io.tileverse.parquetry.internal.read;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.internal.read.page.DataPageRun;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Batched reads over V1 data pages that end in the middle of a row: a row's list values legally spill into the
 * following page when the writer emits no page index. The reader must credit the whole row to one batch - never
 * truncate the head at the page boundary nor start the next batch's level window mid-row.
 *
 * <p>Fixtures follow {@link BatchColumnReaderTest}: Thrift compact page headers via {@link Util#writePageHeader}
 * (test-scope parquet-format-structures) over hand-encoded rep/def/value sections. The column is the standard 3-level
 * annotated LIST - {@code items (LIST) { repeated list { optional element } }}, {@code maxRep == 1}, {@code maxDef ==
 * 3}.
 */
class PageSplitRowReadTest {

    private static final ColumnPath ITEMS = ColumnPath.of("items");
    private static final ColumnPath ELEMENT = ColumnPath.of("items", "list", "element");
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final int MAX_REP = 1;
    private static final int MAX_DEF = 3;

    // --- ASSEMBLED form: a row split across two pages is reassembled whole ---

    @Test
    void assembledFormReassemblesRowSplitAcrossTwoPages() throws IOException {
        // Rows: [10, 20], [30, 40, 50], [60]. Page 1 ends after 40, mid-row-1.
        byte[] page1 = repeatedInt32Page(new int[] {0, 1, 0, 1}, new int[] {3, 3, 3, 3}, new int[] {10, 20, 30, 40});
        byte[] page2 = repeatedInt32Page(new int[] {1, 0}, new int[] {3, 3}, new int[] {50, 60});
        FetchedColumnChunk chunk = repeatedInt32Chunk(page1, page2);

        List<List<Integer>> rows = drainAssembledRows(List.of(chunk), listOfInt32Schema());

        assertThat(rows).containsExactly(List.of(10, 20), List.of(30, 40, 50), List.of(60));
    }

    @Test
    void splitAfterSingleHeadValue() throws IOException {
        // Rows: [1], [2, 3], [4]. Row 1 starts on the last value of page 1.
        byte[] page1 = repeatedInt32Page(new int[] {0, 0}, new int[] {3, 3}, new int[] {1, 2});
        byte[] page2 = repeatedInt32Page(new int[] {1, 0}, new int[] {3, 3}, new int[] {3, 4});
        FetchedColumnChunk chunk = repeatedInt32Chunk(page1, page2);

        List<List<Integer>> rows = drainAssembledRows(List.of(chunk), listOfInt32Schema());

        assertThat(rows).containsExactly(List.of(1), List.of(2, 3), List.of(4));
    }

    @Test
    void rowSpanningThreePages() throws IOException {
        // Rows: [1, 2], [3, 4, 5, 6, 7, 8], [9]. The middle page holds no row boundary at all.
        byte[] page1 = repeatedInt32Page(new int[] {0, 1, 0, 1}, new int[] {3, 3, 3, 3}, new int[] {1, 2, 3, 4});
        byte[] page2 = repeatedInt32Page(new int[] {1, 1, 1}, new int[] {3, 3, 3}, new int[] {5, 6, 7});
        byte[] page3 = repeatedInt32Page(new int[] {1, 0}, new int[] {3, 3}, new int[] {8, 9});
        FetchedColumnChunk chunk = repeatedInt32Chunk(page1, page2, page3);

        List<List<Integer>> rows = drainAssembledRows(List.of(chunk), listOfInt32Schema());

        assertThat(rows).containsExactly(List.of(1, 2), List.of(3, 4, 5, 6, 7, 8), List.of(9));
    }

    @Test
    void splitRowWithNullAndEmptyListsAround() throws IOException {
        // Rows: [10, null], null list, empty list, [20, 30, 40], [50]. Page 1 ends after 30, mid-row-3.
        // def semantics: 3 = present element, 2 = null element, 1 = empty list, 0 = null list.
        byte[] page1 =
                repeatedInt32Page(new int[] {0, 1, 0, 0, 0, 1}, new int[] {3, 2, 0, 1, 3, 3}, new int[] {10, 20, 30});
        byte[] page2 = repeatedInt32Page(new int[] {1, 0}, new int[] {3, 3}, new int[] {40, 50});
        FetchedColumnChunk chunk = repeatedInt32Chunk(page1, page2);

        List<List<Integer>> rows = drainAssembledRows(List.of(chunk), listOfInt32Schema());

        assertThat(rows)
                .containsExactly(Arrays.asList(10, null), List.of(), List.of(), List.of(20, 30, 40), List.of(50));
    }

    @Test
    void alignedPagesStillProduceFullContent() throws IOException {
        // Rows: [1, 2], [3], [4, 5]. Pages end exactly at row boundaries, the layout row-aligned writers produce.
        byte[] page1 = repeatedInt32Page(new int[] {0, 1, 0}, new int[] {3, 3, 3}, new int[] {1, 2, 3});
        byte[] page2 = repeatedInt32Page(new int[] {0, 1}, new int[] {3, 3}, new int[] {4, 5});
        FetchedColumnChunk chunk = repeatedInt32Chunk(page1, page2);

        List<List<Integer>> rows = drainAssembledRows(List.of(chunk), listOfInt32Schema());

        assertThat(rows).containsExactly(List.of(1, 2), List.of(3), List.of(4, 5));
    }

    @Test
    void batchSizeCapOneWithSplit() throws IOException {
        byte[] page1 = repeatedInt32Page(new int[] {0, 1, 0, 1}, new int[] {3, 3, 3, 3}, new int[] {10, 20, 30, 40});
        byte[] page2 = repeatedInt32Page(new int[] {1, 0}, new int[] {3, 3}, new int[] {50, 60});
        FetchedColumnChunk chunk = repeatedInt32Chunk(page1, page2);

        List<List<Integer>> rows = drainAssembledRows(List.of(chunk), listOfInt32Schema(), OptionalInt.of(1));

        assertThat(rows).containsExactly(List.of(10, 20), List.of(30, 40, 50), List.of(60));
    }

    // --- zero-value data pages: legal, and the one-row read must walk over them ---

    @Test
    void leadingEmptyDataPage() throws IOException {
        byte[] empty = repeatedInt32Page(new int[] {}, new int[] {}, new int[] {});
        byte[] page1 = repeatedInt32Page(new int[] {0, 1, 0, 1}, new int[] {3, 3, 3, 3}, new int[] {10, 20, 30, 40});
        byte[] page2 = repeatedInt32Page(new int[] {1, 0}, new int[] {3, 3}, new int[] {50, 60});
        FetchedColumnChunk chunk = repeatedInt32Chunk(empty, page1, page2);

        List<List<Integer>> rows = drainAssembledRows(List.of(chunk), listOfInt32Schema());

        assertThat(rows).containsExactly(List.of(10, 20), List.of(30, 40, 50), List.of(60));
    }

    @Test
    void emptyDataPageBetweenRows() throws IOException {
        byte[] page1 = repeatedInt32Page(new int[] {0, 1}, new int[] {3, 3}, new int[] {10, 20});
        byte[] empty = repeatedInt32Page(new int[] {}, new int[] {}, new int[] {});
        byte[] page2 = repeatedInt32Page(new int[] {0, 1}, new int[] {3, 3}, new int[] {30, 40});
        FetchedColumnChunk chunk = repeatedInt32Chunk(page1, empty, page2);

        List<List<Integer>> rows = drainAssembledRows(List.of(chunk), listOfInt32Schema());

        assertThat(rows).containsExactly(List.of(10, 20), List.of(30, 40));
    }

    @Test
    void emptyDataPageInFlatColumn() throws IOException {
        // A flat-only projection reaches the one-row read when its current page holds no rows at all.
        FetchedColumnChunk chunk = flatInt32Chunk(ID, new int[] {1, 2}, new int[] {}, new int[] {3});
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        ParquetSchema schema = schemaOf(id);

        List<Integer> values = new ArrayList<>();
        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(),
                List.of(chunk),
                schema,
                schema,
                OptionalInt.empty(),
                Optional.empty(),
                BatchForm.ASSEMBLED)) {
            while (reader.hasMore()) {
                try (ParquetRecordBatch batch = reader.nextBatch()) {
                    IntVector idColumn = (IntVector) batch.columns().get(ID);
                    for (int row = 0; row < batch.rowCount(); row++) {
                        values.add(idColumn.getInt(row));
                    }
                }
            }
        }

        assertThat(values).containsExactly(1, 2, 3);
    }

    // --- repeated and flat columns advance in lockstep across the split ---

    @Test
    void repeatedAndFlatColumnsStayInLockstep() throws IOException {
        byte[] page1 = repeatedInt32Page(new int[] {0, 1, 0, 1}, new int[] {3, 3, 3, 3}, new int[] {10, 20, 30, 40});
        byte[] page2 = repeatedInt32Page(new int[] {1, 0}, new int[] {3, 3}, new int[] {50, 60});
        FetchedColumnChunk itemsChunk = repeatedInt32Chunk(page1, page2);
        FetchedColumnChunk idChunk = flatInt32Chunk(ID, new int[] {1, 2, 3});

        ParquetSchema schema = listAndIdSchema();
        List<Integer> ids = new ArrayList<>();
        List<List<Integer>> itemRows = new ArrayList<>();
        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(),
                List.of(itemsChunk, idChunk),
                schema,
                schema,
                OptionalInt.empty(),
                Optional.empty(),
                BatchForm.ASSEMBLED)) {
            while (reader.hasMore()) {
                try (ParquetRecordBatch batch = reader.nextBatch()) {
                    ListVector items = (ListVector) batch.columns().get(ITEMS);
                    IntVector idColumn = (IntVector) batch.columns().get(ID);
                    for (int row = 0; row < batch.rowCount(); row++) {
                        ids.add(idColumn.getInt(row));
                        itemRows.add(listRow(items, row));
                    }
                }
            }
        }

        assertThat(itemRows).containsExactly(List.of(10, 20), List.of(30, 40, 50), List.of(60));
        assertThat(ids).containsExactly(1, 2, 3);
    }

    // --- LEVELS form: the filtered-read path must accept a split without tripping the boundary invariant ---

    @Test
    void levelsFormAcceptsRowSplitAcrossTwoPages() throws IOException {
        byte[] page1 = repeatedInt32Page(new int[] {0, 1, 0, 1}, new int[] {3, 3, 3, 3}, new int[] {10, 20, 30, 40});
        byte[] page2 = repeatedInt32Page(new int[] {1, 0}, new int[] {3, 3}, new int[] {50, 60});
        FetchedColumnChunk chunk = repeatedInt32Chunk(page1, page2);

        int totalRows = 0;
        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(),
                List.of(chunk),
                listOfInt32Schema(),
                listOfInt32Schema(),
                OptionalInt.empty(),
                Optional.empty(),
                BatchForm.LEVELS)) {
            while (reader.hasMore()) {
                try (ParquetRecordBatch batch = reader.nextBatch()) {
                    totalRows += batch.rowCount();
                }
            }
        }

        assertThat(totalRows).isEqualTo(3);
    }

    // --- a split that also crosses the dictionary-to-PLAIN encoding fallback ---

    @Test
    @SuppressWarnings("java:S125") // the fixture-shape comment reads like code to the rule; it is prose
    void splitRowAcrossDictionaryToPlainFallback() throws IOException {
        // Rows: [aa, bb], [cc, aa, zz], [yy]. Page 1 is dictionary-encoded and ends after the "aa" of row 1;
        // page 2 fell back to PLAIN, the way a writer abandons a dictionary that stopped paying off mid-chunk.
        byte[][] dictValues = {bytes("aa"), bytes("bb"), bytes("cc")};
        byte[] page1 = dictionaryRepeatedByteArrayPage(
                dictValues.length, new int[] {0, 1, 0, 1}, new int[] {3, 3, 3, 3}, new int[] {0, 1, 2, 0});
        byte[] page2 =
                plainRepeatedByteArrayPage(new int[] {1, 0}, new int[] {3, 3}, new byte[][] {bytes("zz"), bytes("yy")});
        FetchedColumnChunk chunk = repeatedByteArrayChunk(dictValues, page1, page2);

        List<List<String>> rows = new ArrayList<>();
        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(),
                List.of(chunk),
                listOfByteArraySchema(),
                listOfByteArraySchema(),
                OptionalInt.empty(),
                Optional.empty(),
                BatchForm.ASSEMBLED)) {
            while (reader.hasMore()) {
                try (ParquetRecordBatch batch = reader.nextBatch()) {
                    ListVector items = (ListVector) batch.columns().get(ITEMS);
                    BinaryVector child = (BinaryVector) items.child();
                    for (int row = 0; row < batch.rowCount(); row++) {
                        List<String> elements = new ArrayList<>();
                        for (int i = items.rowOffsetStart(row); i < items.rowOffsetEnd(row); i++) {
                            elements.add(stringAt(child, i));
                        }
                        rows.add(elements);
                    }
                }
            }
        }

        assertThat(rows).containsExactly(List.of("aa", "bb"), List.of("cc", "aa", "zz"), List.of("yy"));
    }

    // --- drain helpers ---

    private static List<List<Integer>> drainAssembledRows(List<FetchedColumnChunk> chunks, ParquetSchema schema) {
        return drainAssembledRows(chunks, schema, OptionalInt.empty());
    }

    private static List<List<Integer>> drainAssembledRows(
            List<FetchedColumnChunk> chunks, ParquetSchema schema, OptionalInt batchSizeCap) {
        List<List<Integer>> rows = new ArrayList<>();
        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(),
                chunks,
                schema,
                schema,
                batchSizeCap,
                Optional.empty(),
                BatchForm.ASSEMBLED)) {
            while (reader.hasMore()) {
                try (ParquetRecordBatch batch = reader.nextBatch()) {
                    ListVector items = (ListVector) batch.columns().get(ITEMS);
                    for (int row = 0; row < batch.rowCount(); row++) {
                        rows.add(listRow(items, row));
                    }
                }
            }
        }
        return rows;
    }

    private static List<Integer> listRow(ListVector items, int row) {
        IntVector child = (IntVector) items.child();
        List<Integer> elements = new ArrayList<>();
        for (int i = items.rowOffsetStart(row); i < items.rowOffsetEnd(row); i++) {
            elements.add(child.isNull(i) ? null : child.getInt(i));
        }
        return elements;
    }

    private static String stringAt(BinaryVector vector, int index) {
        MemorySegment bytes = vector.get(index);
        return new String(bytes.toArray(JAVA_BYTE), StandardCharsets.UTF_8);
    }

    // --- schema fixtures ---

    private static ParquetSchema listOfInt32Schema() {
        return schemaOf(listGroup(int32Element()));
    }

    private static ParquetSchema listOfByteArraySchema() {
        return schemaOf(listGroup(byteArrayElement()));
    }

    private static ParquetSchema listAndIdSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        return schemaOf(listGroup(int32Element()), id);
    }

    private static SchemaNode.Group listGroup(SchemaNode.Primitive element) {
        SchemaNode.Group middle =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        return new SchemaNode.Group(
                "items", Repetition.OPTIONAL, List.of(middle), Optional.of(new LogicalType.ListType()), -1);
    }

    private static SchemaNode.Primitive int32Element() {
        return new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive byteArrayElement() {
        return new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static ParquetSchema schemaOf(SchemaNode... children) {
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(children), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    // --- page and chunk fixtures ---

    /** One V1 page of the repeated INT32 leaf: rep and def sections (both length-prefixed) then the non-null values. */
    private static byte[] repeatedInt32Page(int[] repLevels, int[] defLevels, int[] nonNullValues) throws IOException {
        byte[] repBytes = rleEncodeBits(repLevels, MAX_REP);
        byte[] defBytes = rleEncodeBits(defLevels, MAX_DEF);
        byte[] valueBytes = encodeInt32sLittleEndian(nonNullValues);
        byte[] payload = buildV1PayloadWithRepAndDefLevels(repBytes, defBytes, valueBytes);
        return encodeV1Page(repLevels.length, payload, org.apache.parquet.format.Encoding.PLAIN);
    }

    /** One dictionary-encoded V1 page of the repeated BYTE_ARRAY leaf: rep/def sections then the RLE index stream. */
    private static byte[] dictionaryRepeatedByteArrayPage(
            int dictionarySize, int[] repLevels, int[] defLevels, int[] indices) throws IOException {
        byte[] repBytes = rleEncodeBits(repLevels, MAX_REP);
        byte[] defBytes = rleEncodeBits(defLevels, MAX_DEF);
        byte[] indexBytes = encodeRleDictionaryIndexPage(indices, dictionarySize);
        byte[] payload = buildV1PayloadWithRepAndDefLevels(repBytes, defBytes, indexBytes);
        return encodeV1Page(repLevels.length, payload, org.apache.parquet.format.Encoding.RLE_DICTIONARY);
    }

    /** One PLAIN V1 page of the repeated BYTE_ARRAY leaf: rep/def sections then length-prefixed value bytes. */
    private static byte[] plainRepeatedByteArrayPage(int[] repLevels, int[] defLevels, byte[][] nonNullValues)
            throws IOException {
        byte[] repBytes = rleEncodeBits(repLevels, MAX_REP);
        byte[] defBytes = rleEncodeBits(defLevels, MAX_DEF);
        byte[] valueBytes = encodePlainByteArrays(nonNullValues);
        byte[] payload = buildV1PayloadWithRepAndDefLevels(repBytes, defBytes, valueBytes);
        return encodeV1Page(repLevels.length, payload, org.apache.parquet.format.Encoding.PLAIN);
    }

    private static FetchedColumnChunk repeatedInt32Chunk(byte[]... pages) {
        byte[] chunkBuffer = concat(pages);
        long totalValues = totalValuesOf(pages);
        MemorySegment segment = MemorySegment.ofArray(chunkBuffer).asReadOnly();
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(List.of("items", "list", "element"))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(totalValues)
                .totalUncompressedSize((long) chunkBuffer.length)
                .totalCompressedSize((long) chunkBuffer.length)
                .dataPageOffset(0L)
                .build();
        return new FetchedColumnChunk(
                ELEMENT, meta, MAX_REP, MAX_DEF, List.of(new DataPageRun(segment, 0)), Optional.empty());
    }

    private static FetchedColumnChunk repeatedByteArrayChunk(byte[][] dictValues, byte[]... pages) {
        byte[] chunkBuffer = concat(pages);
        long totalValues = totalValuesOf(pages);
        Dictionary.BinaryDict dictionary = new Dictionary.BinaryDict(toSegments(dictValues));
        MemorySegment segment = MemorySegment.ofArray(chunkBuffer).asReadOnly();
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.BYTE_ARRAY)
                .encodings(List.of(Encoding.RLE_DICTIONARY, Encoding.PLAIN))
                .pathInSchema(List.of("items", "list", "element"))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(totalValues)
                .totalUncompressedSize((long) chunkBuffer.length)
                .totalCompressedSize((long) chunkBuffer.length)
                .dataPageOffset(0L)
                .build();
        return new FetchedColumnChunk(
                ELEMENT, meta, MAX_REP, MAX_DEF, List.of(new DataPageRun(segment, 0)), Optional.of(dictionary));
    }

    private static FetchedColumnChunk flatInt32Chunk(ColumnPath path, int[]... pages) throws IOException {
        byte[][] pageBytes = new byte[pages.length][];
        long totalValues = 0;
        for (int i = 0; i < pages.length; i++) {
            byte[] valueBytes = encodeInt32sLittleEndian(pages[i]);
            pageBytes[i] = encodeV1Page(pages[i].length, valueBytes, org.apache.parquet.format.Encoding.PLAIN);
            totalValues += pages[i].length;
        }
        byte[] chunkBuffer = concat(pageBytes);
        MemorySegment segment = MemorySegment.ofArray(chunkBuffer).asReadOnly();
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(List.of(path.part(0)))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(totalValues)
                .totalUncompressedSize((long) chunkBuffer.length)
                .totalCompressedSize((long) chunkBuffer.length)
                .dataPageOffset(0L)
                .build();
        return new FetchedColumnChunk(
                path, meta, /*maxRep*/ 0, /*maxDef*/ 0, List.of(new DataPageRun(segment, 0)), Optional.empty());
    }

    /**
     * Sums the {@code numValues} of each already-encoded page by re-reading its Thrift header, keeping the chunk's
     * metadata consistent with the pages without threading counts through every call site.
     */
    private static long totalValuesOf(byte[]... pages) {
        long total = 0;
        for (byte[] page : pages) {
            try {
                PageHeader header = Util.readPageHeader(new ByteArrayInputStream(page));
                total += header.getData_page_header().getNum_values();
            } catch (IOException e) {
                throw new IllegalStateException("fixture page header unreadable", e);
            }
        }
        return total;
    }

    // --- byte-level encoders (the layout BatchColumnReaderTest documents) ---

    private static byte[] encodeV1Page(int numValues, byte[] payload, org.apache.parquet.format.Encoding encoding)
            throws IOException {
        PageHeader header = new PageHeader(PageType.DATA_PAGE, payload.length, payload.length);
        DataPageHeader dataHeader = new DataPageHeader(
                numValues, encoding, org.apache.parquet.format.Encoding.RLE, org.apache.parquet.format.Encoding.RLE);
        header.setData_page_header(dataHeader);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writePageHeader(header, out);
        out.write(payload);
        return out.toByteArray();
    }

    /** V1 repeated-nullable payload: [len][rep bytes][len][def bytes][values], lengths 4-byte LE. */
    private static byte[] buildV1PayloadWithRepAndDefLevels(byte[] repBytes, byte[] defBytes, byte[] valueBytes) {
        ByteBuffer buf = ByteBuffer.allocate(8 + repBytes.length + defBytes.length + valueBytes.length)
                .order(LITTLE_ENDIAN);
        buf.putInt(repBytes.length);
        buf.put(repBytes);
        buf.putInt(defBytes.length);
        buf.put(defBytes);
        buf.put(valueBytes);
        return buf.array();
    }

    /** One single-element RLE run per level value - trivially correct, never the compact form a real writer emits. */
    private static byte[] rleEncodeBits(int[] levels, int maxLevel) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int bitWidth = computeBitWidth(maxLevel);
        int bytesPerValue = (bitWidth + 7) / 8;
        for (int level : levels) {
            writeVarint(out, 2L);
            for (int b = 0; b < bytesPerValue; b++) {
                out.write((level >>> (b * 8)) & 0xff);
            }
        }
        return out.toByteArray();
    }

    private static byte[] encodeRleDictionaryIndexPage(int[] indices, int dictionarySize) {
        int bitWidth = computeBitWidth(dictionarySize - 1);
        if (bitWidth == 0) {
            bitWidth = 1;
        }
        int paddedCount = ((indices.length + 7) / 8) * 8;
        int byteCount = (paddedCount * bitWidth + 7) / 8;
        int numGroups = paddedCount / 8;
        int rleHeader = (numGroups << 1) | 1;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(bitWidth);
        writeVarint(out, rleHeader);
        byte[] packed = new byte[byteCount];
        for (int i = 0; i < indices.length; i++) {
            int bitOffset = i * bitWidth;
            int byteOffset = bitOffset / 8;
            int shift = bitOffset % 8;
            packed[byteOffset] |= (byte) ((indices[i] & ((1 << bitWidth) - 1)) << shift);
            if (shift + bitWidth > 8) {
                packed[byteOffset + 1] |= (byte) ((indices[i] & ((1 << bitWidth) - 1)) >>> (8 - shift));
            }
        }
        out.write(packed, 0, byteCount);
        return out.toByteArray();
    }

    private static byte[] encodeInt32sLittleEndian(int[] values) {
        ByteBuffer buf = ByteBuffer.allocate(values.length * Integer.BYTES).order(LITTLE_ENDIAN);
        for (int v : values) {
            buf.putInt(v);
        }
        return buf.array();
    }

    private static byte[] encodePlainByteArrays(byte[][] values) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] value : values) {
            ByteBuffer lengthPrefix = ByteBuffer.allocate(4).order(LITTLE_ENDIAN);
            lengthPrefix.putInt(value.length);
            out.writeBytes(lengthPrefix.array());
            out.writeBytes(value);
        }
        return out.toByteArray();
    }

    private static int computeBitWidth(int maxLevel) {
        if (maxLevel == 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxLevel);
    }

    private static void writeVarint(ByteArrayOutputStream out, long value) {
        while (true) {
            if ((value & ~0x7fL) == 0) {
                out.write((int) value);
                return;
            }
            out.write(((int) value & 0x7f) | 0x80);
            value >>>= 7;
        }
    }

    private static byte[] concat(byte[]... arrays) {
        int total = 0;
        for (byte[] array : arrays) {
            total += array.length;
        }
        byte[] out = new byte[total];
        int offset = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, out, offset, array.length);
            offset += array.length;
        }
        return out;
    }

    private static List<MemorySegment> toSegments(byte[][] values) {
        return Arrays.stream(values)
                .map(value -> MemorySegment.ofArray(value).asReadOnly())
                .toList();
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
