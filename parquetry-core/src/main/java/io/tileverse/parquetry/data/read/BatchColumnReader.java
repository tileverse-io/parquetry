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
package io.tileverse.parquetry.data.read;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Optional;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.Int96Vector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.read.page.ByteStreamSplitDoubleDecoder;
import io.tileverse.parquetry.data.read.page.ByteStreamSplitFloatDecoder;
import io.tileverse.parquetry.data.read.page.DecodedPage;
import io.tileverse.parquetry.data.read.page.DeltaBinaryPackedInt32Decoder;
import io.tileverse.parquetry.data.read.page.DeltaBinaryPackedInt64Decoder;
import io.tileverse.parquetry.data.read.page.DeltaByteArrayDecoder;
import io.tileverse.parquetry.data.read.page.DeltaLengthByteArrayDecoder;
import io.tileverse.parquetry.data.read.page.Dictionary;
import io.tileverse.parquetry.data.read.page.LevelDecoder;
import io.tileverse.parquetry.data.read.page.PageCursor;
import io.tileverse.parquetry.data.read.page.PageDecoder;
import io.tileverse.parquetry.data.read.page.PageSelection;
import io.tileverse.parquetry.data.read.page.PlainBinaryDecoder;
import io.tileverse.parquetry.data.read.page.PlainBooleanDecoder;
import io.tileverse.parquetry.data.read.page.PlainDoubleDecoder;
import io.tileverse.parquetry.data.read.page.PlainFixedLenBinaryDecoder;
import io.tileverse.parquetry.data.read.page.PlainFloatDecoder;
import io.tileverse.parquetry.data.read.page.PlainInt32Decoder;
import io.tileverse.parquetry.data.read.page.PlainInt64Decoder;
import io.tileverse.parquetry.data.read.page.PlainInt96Decoder;
import io.tileverse.parquetry.data.read.page.RleBooleanDecoder;
import io.tileverse.parquetry.data.read.page.RleDictionaryPageDecoder;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * Per-column reader for the batch API. Each call to {@link #readBatch(int)} returns a {@link ColumnVector} sliced from
 * the current page's pre-decoded values.
 *
 * <p>Page lifecycle: {@link #loadNextPage()} decompresses the next page into a transient {@link Arena#ofConfined()
 * confined Arena}, fully decodes rep-levels / def-levels / values into heap-backed Java arrays (or
 * {@link MemorySegment}[] of heap-backed segments for binary kinds), and closes the Arena before returning. Once a page
 * is loaded, every {@link #readBatch} call is array slicing - no Arena-backed memory escapes the load step, and
 * within-page splitting is naturally correct because page state lives on the heap.
 */
final class BatchColumnReader {

    private static final int[] EMPTY_REP_LEVELS = new int[0];

    private final ColumnPath columnPath;
    private final SchemaNode.Primitive leaf;
    private final LevelMaxima maxLevels;
    private final Compression codec;
    private final long totalValues;
    private final PageCursor pageCursor;
    private final int defBitWidth;
    private final int repBitWidth;
    private final Optional<Dictionary<?>> dictionary;
    private final RowRanges survivingRows; // null when this column is not masked

    // Per-page state. Populated by loadNextPage; cleared on advance.
    private boolean pageLoaded;
    private int pageSize;
    private int pageLogicalRowCount;
    private BitSet pageValidity;
    private int[] pageRepLevels; // null when maxRep == 0
    private int[] pageInts;
    private long[] pageLongs;
    private float[] pageFloats;
    private double[] pageDoubles;
    private boolean[] pageBooleans;
    private MemorySegment[] pageSegments;

    private int valuesConsumedInCurrentPage;
    private int logicalRowsConsumedInCurrentPage;
    private long valuesConsumedTotal;
    private long survivingRowsConsumedTotal;

    BatchColumnReader(@NonNull FetchedColumnChunk chunk, @NonNull SchemaNode.Primitive leaf) {
        this(chunk, leaf, null, null);
    }

    BatchColumnReader(
            @NonNull FetchedColumnChunk chunk,
            @NonNull SchemaNode.Primitive leaf,
            RowRanges survivingRows,
            OffsetIndex offsetIndex) {
        this.columnPath = chunk.path();
        this.leaf = leaf;
        this.maxLevels = new LevelMaxima(chunk.maxRepetitionLevel(), chunk.maxDefinitionLevel());
        this.codec = Compression.forWireCodec(chunk.metadata().codec());
        this.totalValues = chunk.metadata().numValues();
        this.defBitWidth = LevelDecoder.computeBitWidth(maxLevels.maxDefinitionLevel());
        this.repBitWidth = LevelDecoder.computeBitWidth(maxLevels.maxRepetitionLevel());
        this.dictionary = chunk.dictionary();
        if (survivingRows != null && offsetIndex != null) {
            this.survivingRows = survivingRows;
            PageSelection selection = PageSelection.forRanges(offsetIndex.pageLocations(), totalValues, survivingRows);
            this.pageCursor = new PageCursor(chunk.compressedSegment(), columnPath, selection);
        } else {
            this.survivingRows = null;
            this.pageCursor = new PageCursor(chunk.compressedSegment(), columnPath);
        }
    }

    // ---- public iteration surface ----

    /** True while the current page has unconsumed values or the page cursor has more pages. */
    boolean hasMore() {
        if (pageLoaded && valuesConsumedInCurrentPage < pageSize) {
            return true;
        }
        if (survivingRows != null) {
            return survivingRowsConsumedTotal < survivingRows.totalRows();
        }
        return pageCursor.hasRemaining();
    }

    /** Leaf-value count remaining in the current page. Loads the page on first call. */
    int rowsRemainingInCurrentPage() {
        ensurePageLoaded();
        return pageSize - valuesConsumedInCurrentPage;
    }

    /**
     * Logical (top-level) row count remaining in the current page. For flat columns this equals
     * {@link #rowsRemainingInCurrentPage()}; for repeated columns it counts rep=0 markers ahead of the consume cursor.
     */
    int logicalRowsRemainingInCurrentPage() {
        ensurePageLoaded();
        if (pageRepLevels == null) {
            return pageSize - valuesConsumedInCurrentPage;
        }
        return pageLogicalRowCount - logicalRowsConsumedInCurrentPage;
    }

    /** Rep-level stream for the current page; one entry per leaf element. Null for flat columns. */
    int[] currentPageRepLevels() {
        ensurePageLoaded();
        return pageRepLevels;
    }

    /** Position in the current page's value stream where the next {@link #readBatch} call will start. */
    int valuesConsumedInCurrentPage() {
        ensurePageLoaded();
        return valuesConsumedInCurrentPage;
    }

    /**
     * Number of leaf values that cover the next {@code logicalRows} logical rows in the current page. For flat columns
     * this is {@code logicalRows}; for repeated columns it counts entries from the consume cursor until the
     * {@code logicalRows}'th rep=0 marker.
     */
    int valuesForLogicalRows(int logicalRows) {
        ensurePageLoaded();
        if (logicalRows <= 0) {
            return 0;
        }
        if (pageRepLevels == null) {
            return logicalRows;
        }
        return countValuesForLogicalRows(pageRepLevels, valuesConsumedInCurrentPage, logicalRows);
    }

    /**
     * Reads up to {@code maxValues} values from the current page into a new {@link ColumnVector}. The actual size is
     * {@code min(maxValues, pageSize - valuesConsumedInCurrentPage)}. Subsequent calls continue within the same page
     * until it is exhausted, then advance to the next page on demand.
     */
    ColumnVector readBatch(int maxValues) {
        if (maxValues <= 0) {
            throw new IllegalArgumentException("maxValues must be positive; got " + maxValues);
        }
        ensurePageLoaded();
        if (valuesConsumedInCurrentPage >= pageSize) {
            throw new IllegalStateException(
                    "No more values in column " + columnPath.dot() + " (totalValues=" + totalValues + ")");
        }
        int start = valuesConsumedInCurrentPage;
        int n = Math.min(maxValues, pageSize - start);

        ColumnVector vec = sliceVector(start, n);

        valuesConsumedInCurrentPage += n;
        logicalRowsConsumedInCurrentPage += countLogicalRowsInSlice(start, n);
        valuesConsumedTotal += n;
        if (survivingRows != null) {
            survivingRowsConsumedTotal += n;
        }
        if (valuesConsumedInCurrentPage >= pageSize) {
            advancePastCurrentPage();
        }
        return vec;
    }

    // ---- page lifecycle ----

    private void ensurePageLoaded() {
        if (!pageLoaded) {
            loadNextPage();
        }
    }

    private void loadNextPage() {
        Arena pageArena = Arena.ofConfined();
        try {
            DecodedPage page;
            try {
                page = pageCursor.nextDataPage(maxLevels, codec, pageArena);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read next page for column " + columnPath.dot(), e);
            }
            if (page == null) {
                throw new MalformedFileException("Column " + columnPath.dot() + " exhausted page stream after "
                        + valuesConsumedTotal + " values; expected " + totalValues);
            }
            decodeIntoHeap(page);
        } finally {
            pageArena.close();
        }
    }

    private void decodeIntoHeap(DecodedPage page) {
        pageSize = page.valueCount();
        pageValidity = decodeValidity(page, pageSize);
        pageRepLevels = decodeRepLevels(page, pageSize);
        pageLogicalRowCount = (pageRepLevels == null) ? pageSize : countRepZeroMarkers(pageRepLevels);
        clearTypedPayloads();
        decodeValuesByKind(page);
        if (survivingRows != null) {
            compactToSurvivingRows(pageCursor.currentPageFirstRowIndex());
        }
        valuesConsumedInCurrentPage = 0;
        logicalRowsConsumedInCurrentPage = 0;
        pageLoaded = true;
    }

    private void advancePastCurrentPage() {
        pageLoaded = false;
        pageSize = 0;
        pageLogicalRowCount = 0;
        pageValidity = null;
        pageRepLevels = null;
        clearTypedPayloads();
        valuesConsumedInCurrentPage = 0;
        logicalRowsConsumedInCurrentPage = 0;
    }

    private void clearTypedPayloads() {
        pageInts = null;
        pageLongs = null;
        pageFloats = null;
        pageDoubles = null;
        pageBooleans = null;
        pageSegments = null;
    }

    // ---- level decoding ----

    private BitSet decodeValidity(DecodedPage page, int values) {
        BitSet validity = new BitSet(values);
        int maxDef = maxLevels.maxDefinitionLevel();
        if (maxDef == 0) {
            validity.set(0, values);
            return validity;
        }
        LevelDecoder defDecoder = new LevelDecoder(defBitWidth);
        defDecoder.load(page.defLevelBytes());
        defDecoder.decodeEquals(values, maxDef, validity, 0);
        return validity;
    }

    // null marks "flat column, no rep levels exist" - distinct from "repeated column with zero values in the page"
    @SuppressWarnings("java:S1168")
    private int[] decodeRepLevels(DecodedPage page, int values) {
        if (maxLevels.maxRepetitionLevel() == 0) {
            return null;
        }
        if (values == 0) {
            return EMPTY_REP_LEVELS;
        }
        LevelDecoder repDecoder = new LevelDecoder(repBitWidth);
        repDecoder.load(page.repLevelBytes());
        int[] repLevels = new int[values];
        repDecoder.decode(values, repLevels, 0);
        return repLevels;
    }

    private static int countRepZeroMarkers(int[] repLevels) {
        int count = 0;
        for (int rep : repLevels) {
            if (rep == 0) {
                count++;
            }
        }
        return count;
    }

    private static int countValuesForLogicalRows(int[] repLevels, int startIndex, int logicalRows) {
        int rowsCrossed = 0;
        int i = startIndex;
        while (i < repLevels.length) {
            if (repLevels[i] == 0) {
                if (rowsCrossed == logicalRows) {
                    return i - startIndex;
                }
                rowsCrossed++;
            }
            i++;
        }
        return repLevels.length - startIndex;
    }

    private int countLogicalRowsInSlice(int start, int count) {
        if (pageRepLevels == null) {
            return count;
        }
        int rows = 0;
        int end = start + count;
        for (int i = start; i < end; i++) {
            if (pageRepLevels[i] == 0) {
                rows++;
            }
        }
        return rows;
    }

    // ---- per-kind value decoding ----

    private void decodeValuesByKind(DecodedPage page) {
        int nonNullCount = pageValidity.cardinality();
        Encoding encoding = page.valuesEncoding();
        MemorySegment valueBuf = page.valueBytes();
        Dictionary<?> dict = dictionary.orElse(null);
        switch (leaf.kind()) {
            case INT32 -> pageInts = decodeInts(valueBuf, encoding, nonNullCount, dict);
            case INT64 -> pageLongs = decodeLongs(valueBuf, encoding, nonNullCount, dict);
            case FLOAT -> pageFloats = decodeFloats(valueBuf, encoding, nonNullCount, dict);
            case DOUBLE -> pageDoubles = decodeDoubles(valueBuf, encoding, nonNullCount, dict);
            case BOOLEAN -> pageBooleans = decodeBooleans(valueBuf, encoding, nonNullCount);
            case BYTE_ARRAY -> pageSegments = decodeBinary(valueBuf, encoding, nonNullCount, dict);
            case FIXED_LEN_BYTE_ARRAY -> pageSegments = decodeFixedLenBinary(valueBuf, encoding, nonNullCount, dict);
            case INT96 -> pageSegments = decodeInt96(valueBuf, encoding, nonNullCount, dict);
        }
    }

    private int[] decodeInts(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        int[] out = new int[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = intDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeInts(pageSize, out, 0);
            return out;
        }
        int[] dense = new int[nonNullCount];
        decoder.decodeInts(nonNullCount, dense, 0);
        spreadInts(dense, out);
        return out;
    }

    private long[] decodeLongs(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        long[] out = new long[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = longDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeLongs(pageSize, out, 0);
            return out;
        }
        long[] dense = new long[nonNullCount];
        decoder.decodeLongs(nonNullCount, dense, 0);
        spreadLongs(dense, out);
        return out;
    }

    private float[] decodeFloats(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        float[] out = new float[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = floatDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeFloats(pageSize, out, 0);
            return out;
        }
        float[] dense = new float[nonNullCount];
        decoder.decodeFloats(nonNullCount, dense, 0);
        spreadFloats(dense, out);
        return out;
    }

    private double[] decodeDoubles(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        double[] out = new double[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = doubleDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeDoubles(pageSize, out, 0);
            return out;
        }
        double[] dense = new double[nonNullCount];
        decoder.decodeDoubles(nonNullCount, dense, 0);
        spreadDoubles(dense, out);
        return out;
    }

    private boolean[] decodeBooleans(MemorySegment buf, Encoding encoding, int nonNullCount) {
        boolean[] out = new boolean[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = booleanDecoderFor(encoding);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeBooleans(pageSize, out, 0);
            return out;
        }
        boolean[] dense = new boolean[nonNullCount];
        decoder.decodeBooleans(nonNullCount, dense, 0);
        spreadBooleans(dense, out);
        return out;
    }

    private MemorySegment[] decodeBinary(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        MemorySegment[] out = new MemorySegment[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = binaryDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeBinary(pageSize, out, 0);
        } else {
            MemorySegment[] dense = new MemorySegment[nonNullCount];
            decoder.decodeBinary(nonNullCount, dense, 0);
            spreadSegments(dense, out);
        }
        if (!isDictionaryEncoded(encoding)) {
            copySegmentsToHeap(out);
        }
        return out;
    }

    private MemorySegment[] decodeFixedLenBinary(
            MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        int byteWidth = requiredByteWidth();
        MemorySegment[] out = new MemorySegment[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = fixedLenBinaryDecoderFor(encoding, byteWidth, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeBinary(pageSize, out, 0);
        } else {
            MemorySegment[] dense = new MemorySegment[nonNullCount];
            decoder.decodeBinary(nonNullCount, dense, 0);
            spreadSegments(dense, out);
        }
        if (!isDictionaryEncoded(encoding)) {
            copySegmentsToHeap(out);
        }
        return out;
    }

    private MemorySegment[] decodeInt96(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        MemorySegment[] out = new MemorySegment[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = int96DecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeBinary(pageSize, out, 0);
        } else {
            MemorySegment[] dense = new MemorySegment[nonNullCount];
            decoder.decodeBinary(nonNullCount, dense, 0);
            spreadSegments(dense, out);
        }
        if (!isDictionaryEncoded(encoding)) {
            copySegmentsToHeap(out);
        }
        return out;
    }

    private int requiredByteWidth() {
        return leaf.typeLength()
                .orElseThrow(() -> new IllegalStateException(
                        "FIXED_LEN_BYTE_ARRAY column " + columnPath.dot() + " is missing typeLength in schema"));
    }

    /**
     * Dictionary-encoded binary values are references into the column's {@link Dictionary}, whose values are
     * heap-owned, immutable, GC-managed segments (not page-Arena or pool memory). They outlive the page Arena and
     * survive the chunk's close, so they need no per-row heap copy. PLAIN/DELTA values are zero-copy views into the
     * page Arena and must be copied out before that Arena closes.
     */
    private static boolean isDictionaryEncoded(Encoding encoding) {
        return encoding == Encoding.RLE_DICTIONARY || encoding == Encoding.PLAIN_DICTIONARY;
    }

    /**
     * Replaces every non-null entry in {@code segments} with a heap-backed copy. Lets the caller close the page Arena
     * without invalidating the segments.
     */
    private static void copySegmentsToHeap(MemorySegment[] segments) {
        for (int i = 0; i < segments.length; i++) {
            MemorySegment src = segments[i];
            if (src == null) {
                continue;
            }
            byte[] bytes = src.toArray(JAVA_BYTE);
            segments[i] = MemorySegment.ofArray(bytes).asReadOnly();
        }
    }

    // ---- spread (dense -> validity-positioned) ----

    private void spreadInts(int[] dense, int[] dst) {
        int j = 0;
        for (int i = pageValidity.nextSetBit(0); i >= 0; i = pageValidity.nextSetBit(i + 1)) {
            dst[i] = dense[j++];
        }
    }

    private void spreadLongs(long[] dense, long[] dst) {
        int j = 0;
        for (int i = pageValidity.nextSetBit(0); i >= 0; i = pageValidity.nextSetBit(i + 1)) {
            dst[i] = dense[j++];
        }
    }

    private void spreadFloats(float[] dense, float[] dst) {
        int j = 0;
        for (int i = pageValidity.nextSetBit(0); i >= 0; i = pageValidity.nextSetBit(i + 1)) {
            dst[i] = dense[j++];
        }
    }

    private void spreadDoubles(double[] dense, double[] dst) {
        int j = 0;
        for (int i = pageValidity.nextSetBit(0); i >= 0; i = pageValidity.nextSetBit(i + 1)) {
            dst[i] = dense[j++];
        }
    }

    private void spreadBooleans(boolean[] dense, boolean[] dst) {
        int j = 0;
        for (int i = pageValidity.nextSetBit(0); i >= 0; i = pageValidity.nextSetBit(i + 1)) {
            dst[i] = dense[j++];
        }
    }

    private void spreadSegments(MemorySegment[] dense, MemorySegment[] dst) {
        int j = 0;
        for (int i = pageValidity.nextSetBit(0); i >= 0; i = pageValidity.nextSetBit(i + 1)) {
            dst[i] = dense[j++];
        }
    }

    // ---- decoder dispatch ----

    private PageDecoder<?> intDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainInt32Decoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedInt32Decoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "INT32");
            default -> throw unsupported(encoding, "INT32");
        };
    }

    private PageDecoder<?> longDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainInt64Decoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedInt64Decoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "INT64");
            default -> throw unsupported(encoding, "INT64");
        };
    }

    private PageDecoder<?> floatDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainFloatDecoder();
            case BYTE_STREAM_SPLIT -> new ByteStreamSplitFloatDecoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "FLOAT");
            default -> throw unsupported(encoding, "FLOAT");
        };
    }

    private PageDecoder<?> doubleDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainDoubleDecoder();
            case BYTE_STREAM_SPLIT -> new ByteStreamSplitDoubleDecoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "DOUBLE");
            default -> throw unsupported(encoding, "DOUBLE");
        };
    }

    private static PageDecoder<?> booleanDecoderFor(Encoding encoding) {
        return switch (encoding) {
            case PLAIN -> new PlainBooleanDecoder();
            case RLE -> new RleBooleanDecoder();
            default -> throw unsupported(encoding, "BOOLEAN");
        };
    }

    private PageDecoder<?> binaryDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainBinaryDecoder();
            case DELTA_BYTE_ARRAY -> new DeltaByteArrayDecoder();
            case DELTA_LENGTH_BYTE_ARRAY -> new DeltaLengthByteArrayDecoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "BYTE_ARRAY");
            default -> throw unsupported(encoding, "BYTE_ARRAY");
        };
    }

    private PageDecoder<?> fixedLenBinaryDecoderFor(Encoding encoding, int byteWidth, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainFixedLenBinaryDecoder(byteWidth);
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "FIXED_LEN_BYTE_ARRAY");
            default -> throw unsupported(encoding, "FIXED_LEN_BYTE_ARRAY");
        };
    }

    private PageDecoder<?> int96DecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainInt96Decoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "INT96");
            default -> throw unsupported(encoding, "INT96");
        };
    }

    private static PageDecoder<?> requireDictionaryDecoder(Dictionary<?> dict, String kindLabel) {
        if (dict == null) {
            throw new IllegalStateException(
                    "Dictionary-encoded data page requires a loaded Dictionary; none supplied for " + kindLabel);
        }
        return new RleDictionaryPageDecoder<>(dict);
    }

    private static UnsupportedOperationException unsupported(Encoding encoding, String kindLabel) {
        return new UnsupportedOperationException(
                "BatchColumnReader has no decoder wired for encoding " + encoding + " on " + kindLabel);
    }

    // ---- row compaction for masked reads ----

    /**
     * Drops the rows of the just-decoded page that fall outside the surviving ranges, leaving only the surviving rows
     * (in order) in the per-page arrays. Flat columns only: one value per row, no repetition levels.
     */
    private void compactToSurvivingRows(long pageFirstRow) {
        int[] keep = survivingLocalPositions(pageFirstRow, pageSize);
        if (keep.length == pageSize) {
            return;
        }
        pageValidity = gatherValidity(pageValidity, keep);
        gatherTypedPayloads(keep);
        pageSize = keep.length;
        pageLogicalRowCount = keep.length;
    }

    /** Local positions {@code p} of the page whose row group index {@code pageFirstRow + p} is in the surviving set. */
    private int[] survivingLocalPositions(long pageFirstRow, int pageRows) {
        long pageLast = pageFirstRow + pageRows - 1;
        int[] keep = new int[pageRows];
        int count = 0;
        for (RowRanges.Range range : survivingRows.ranges()) {
            if (range.last() < pageFirstRow || range.first() > pageLast) {
                continue;
            }
            long from = Math.max(range.first(), pageFirstRow);
            long to = Math.min(range.last(), pageLast);
            for (long row = from; row <= to; row++) {
                keep[count++] = (int) (row - pageFirstRow);
            }
        }
        return (count == pageRows) ? keep : Arrays.copyOf(keep, count);
    }

    private void gatherTypedPayloads(int[] keep) {
        if (pageInts != null) {
            pageInts = gatherInts(pageInts, keep);
        } else if (pageLongs != null) {
            pageLongs = gatherLongs(pageLongs, keep);
        } else if (pageFloats != null) {
            pageFloats = gatherFloats(pageFloats, keep);
        } else if (pageDoubles != null) {
            pageDoubles = gatherDoubles(pageDoubles, keep);
        } else if (pageBooleans != null) {
            pageBooleans = gatherBooleans(pageBooleans, keep);
        } else if (pageSegments != null) {
            pageSegments = gatherSegments(pageSegments, keep);
        }
    }

    private static BitSet gatherValidity(BitSet source, int[] keep) {
        BitSet out = new BitSet(keep.length);
        for (int j = 0; j < keep.length; j++) {
            if (source.get(keep[j])) {
                out.set(j);
            }
        }
        return out;
    }

    private static int[] gatherInts(int[] source, int[] keep) {
        int[] out = new int[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source[keep[j]];
        }
        return out;
    }

    private static long[] gatherLongs(long[] source, int[] keep) {
        long[] out = new long[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source[keep[j]];
        }
        return out;
    }

    private static float[] gatherFloats(float[] source, int[] keep) {
        float[] out = new float[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source[keep[j]];
        }
        return out;
    }

    private static double[] gatherDoubles(double[] source, int[] keep) {
        double[] out = new double[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source[keep[j]];
        }
        return out;
    }

    private static boolean[] gatherBooleans(boolean[] source, int[] keep) {
        boolean[] out = new boolean[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source[keep[j]];
        }
        return out;
    }

    private static MemorySegment[] gatherSegments(MemorySegment[] source, int[] keep) {
        MemorySegment[] out = new MemorySegment[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source[keep[j]];
        }
        return out;
    }

    // ---- vector slicing ----

    private ColumnVector sliceVector(int start, int n) {
        int end = start + n;
        BitSet sliceValidity = sliceBitSet(pageValidity, start, n);
        return switch (leaf.kind()) {
            case INT32 -> IntVector.materialized(Arrays.copyOfRange(pageInts, start, end), sliceValidity);
            case INT64 -> LongVector.materialized(Arrays.copyOfRange(pageLongs, start, end), sliceValidity);
            case FLOAT -> FloatVector.materialized(Arrays.copyOfRange(pageFloats, start, end), sliceValidity);
            case DOUBLE -> DoubleVector.materialized(Arrays.copyOfRange(pageDoubles, start, end), sliceValidity);
            case BOOLEAN -> BooleanVector.materialized(Arrays.copyOfRange(pageBooleans, start, end), sliceValidity);
            case BYTE_ARRAY -> BinaryVector.materialized(Arrays.copyOfRange(pageSegments, start, end), sliceValidity);
            case FIXED_LEN_BYTE_ARRAY ->
                FixedLenBinaryVector.materialized(
                        Arrays.copyOfRange(pageSegments, start, end), requiredByteWidth(), sliceValidity);
            case INT96 -> Int96Vector.materialized(Arrays.copyOfRange(pageSegments, start, end), sliceValidity);
        };
    }

    private static BitSet sliceBitSet(BitSet source, int start, int n) {
        BitSet slice = new BitSet(n);
        for (int i = source.nextSetBit(start); i >= 0 && i < start + n; i = source.nextSetBit(i + 1)) {
            slice.set(i - start);
        }
        return slice;
    }

    // ---- close ----

    /** No native memory to release; page Arenas are closed inside {@link #loadNextPage()}. */
    void close() {
        advancePastCurrentPage();
    }

    // ---- diagnostics ----

    /** Data pages this reader actually decoded; pages skipped by the surviving-row mask are not counted. */
    int decodedDataPageCount() {
        return pageCursor.decodedDataPageCount();
    }

    ColumnPath columnPath() {
        return columnPath;
    }

    Optional<Dictionary<?>> dictionary() {
        return dictionary;
    }
}
