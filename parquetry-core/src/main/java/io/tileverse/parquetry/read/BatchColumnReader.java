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
package io.tileverse.parquetry.read;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
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
import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.codec.CodecRegistry;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.page.ByteStreamSplitDoubleDecoder;
import io.tileverse.parquetry.page.ByteStreamSplitFloatDecoder;
import io.tileverse.parquetry.page.DeltaBinaryPackedInt32Decoder;
import io.tileverse.parquetry.page.DeltaBinaryPackedInt64Decoder;
import io.tileverse.parquetry.page.DeltaByteArrayDecoder;
import io.tileverse.parquetry.page.DeltaLengthByteArrayDecoder;
import io.tileverse.parquetry.page.Dictionary;
import io.tileverse.parquetry.page.LevelDecoder;
import io.tileverse.parquetry.page.PageDecoder;
import io.tileverse.parquetry.page.PlainBinaryDecoder;
import io.tileverse.parquetry.page.PlainBooleanDecoder;
import io.tileverse.parquetry.page.PlainDoubleDecoder;
import io.tileverse.parquetry.page.PlainFixedLenBinaryDecoder;
import io.tileverse.parquetry.page.PlainFloatDecoder;
import io.tileverse.parquetry.page.PlainInt32Decoder;
import io.tileverse.parquetry.page.PlainInt64Decoder;
import io.tileverse.parquetry.page.PlainInt96Decoder;
import io.tileverse.parquetry.page.RleBooleanDecoder;
import io.tileverse.parquetry.page.RleDictionaryPageDecoder;
import io.tileverse.parquetry.read.LevelMaximaResolver.LevelMaxima;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;

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
    private final Field.Primitive leaf;
    private final LevelMaxima maxLevels;
    private final Codec codec;
    private final long totalValues;
    private final PageCursor pageCursor;
    private final int defBitWidth;
    private final int repBitWidth;
    private final Optional<Dictionary<?>> dictionary;

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

    BatchColumnReader(@NonNull FetchedColumnChunk chunk, @NonNull Field.Primitive leaf) {
        this.columnPath = chunk.path();
        this.leaf = leaf;
        this.maxLevels = new LevelMaxima(chunk.maxRepetitionLevel(), chunk.maxDefinitionLevel());
        this.codec = CodecRegistry.lookup(chunk.metadata().codec());
        this.totalValues = chunk.metadata().numValues();
        this.pageCursor = new PageCursor(chunk.compressedBuffer().buffer(), columnPath);
        this.defBitWidth = LevelDecoder.computeBitWidth(maxLevels.maxDefinitionLevel());
        this.repBitWidth = LevelDecoder.computeBitWidth(maxLevels.maxRepetitionLevel());
        this.dictionary = chunk.dictionary();
    }

    // ---- public iteration surface ----

    /** True while the current page has unconsumed values or the page cursor has more pages. */
    boolean hasMore() {
        if (pageLoaded && valuesConsumedInCurrentPage < pageSize) {
            return true;
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
        ByteBuffer defBuf = levelByteBuffer(page.defLevelBytes());
        LevelDecoder defDecoder = new LevelDecoder(defBitWidth);
        defDecoder.load(defBuf);
        int[] defLevels = new int[values];
        defDecoder.decode(values, defLevels, 0);
        for (int i = 0; i < values; i++) {
            if (defLevels[i] == maxDef) {
                validity.set(i);
            }
        }
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
        ByteBuffer repBuf = levelByteBuffer(page.repLevelBytes());
        LevelDecoder repDecoder = new LevelDecoder(repBitWidth);
        repDecoder.load(repBuf);
        int[] repLevels = new int[values];
        repDecoder.decode(values, repLevels, 0);
        return repLevels;
    }

    private static ByteBuffer levelByteBuffer(MemorySegment levelBytes) {
        if (levelBytes == MemorySegment.NULL) {
            return ByteBuffer.allocate(0).order(LITTLE_ENDIAN);
        }
        return levelBytes.asByteBuffer().order(LITTLE_ENDIAN);
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
        ByteBuffer valueBuf = page.valueBytes().asByteBuffer().order(LITTLE_ENDIAN);
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

    private int[] decodeInts(ByteBuffer buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
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

    private long[] decodeLongs(ByteBuffer buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
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

    private float[] decodeFloats(ByteBuffer buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
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

    private double[] decodeDoubles(ByteBuffer buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
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

    private boolean[] decodeBooleans(ByteBuffer buf, Encoding encoding, int nonNullCount) {
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

    private MemorySegment[] decodeBinary(ByteBuffer buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
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
        copySegmentsToHeap(out);
        return out;
    }

    private MemorySegment[] decodeFixedLenBinary(
            ByteBuffer buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
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
        copySegmentsToHeap(out);
        return out;
    }

    private MemorySegment[] decodeInt96(ByteBuffer buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
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
        copySegmentsToHeap(out);
        return out;
    }

    private int requiredByteWidth() {
        return leaf.typeLength()
                .orElseThrow(() -> new IllegalStateException(
                        "FIXED_LEN_BYTE_ARRAY column " + columnPath.dot() + " is missing typeLength in schema"));
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

    ColumnPath columnPath() {
        return columnPath;
    }

    Optional<Dictionary<?>> dictionary() {
        return dictionary;
    }
}
