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
package io.tileverse.parquetry.internal.read.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.ParquetLayouts;
import io.tileverse.parquetry.format.UnsupportedFeatureException;

/**
 * Walks the values of one ALP-encoded page, as laid out by parquet-format's AlpEncoding.md: a 7-byte page header, an
 * array of vector offsets, then the vectors. Each vector holds its exponent, factor, exception count, frame of
 * reference and bit width, followed by the bit-packed deltas, the exception positions and the exception values. Turning
 * deltas into floating-point values is left to {@link AlpFloatDecoder} and {@link AlpDoubleDecoder}.
 *
 * <p>Values are read in runs confined to one vector. {@link #beginRun(int)} enters the vector holding the next value
 * and {@link #endRun(int)} consumes the run. In between, {@link #packedDelta(int)} unpacks the delta of a value, and
 * {@link #firstExceptionInRun()} with {@link #nextExceptionInRun(int)} enumerate the exceptions falling inside the run.
 *
 * <p>The page header is validated on {@link #load(MemorySegment)}, and each vector's offset, header and section sizes
 * when the vector is entered. An inconsistency with the page bytes raises {@link MalformedFileException}.
 */
final class AlpPageCursor {

    /** The value type of an ALP page, setting the widths of its frames of reference and exception values. */
    enum ValueType {
        FLOAT(Float.BYTES, 10),
        DOUBLE(Double.BYTES, 18);

        private final int valueBytes;
        private final int maxExponent;

        ValueType(int valueBytes, int maxExponent) {
            this.valueBytes = valueBytes;
            this.maxExponent = maxExponent;
        }

        int maxBitWidth() {
            return valueBytes * Byte.SIZE;
        }

        /** AlpInfo (exponent, factor, exception count) followed by ForInfo (frame of reference, bit width). */
        int vectorHeaderBytes() {
            return ALP_INFO_BYTES + valueBytes + 1;
        }

        long readFrameOfReference(MemorySegment page, long offset) {
            return switch (this) {
                case FLOAT -> page.get(ParquetLayouts.INT32, offset);
                case DOUBLE -> page.get(ParquetLayouts.INT64, offset);
            };
        }
    }

    static final int PAGE_HEADER_BYTES = 7;

    private static final int ALP_INFO_BYTES = 4;
    private static final int OFFSET_BYTES = Integer.BYTES;
    private static final int EXCEPTION_POSITION_BYTES = Short.BYTES;
    private static final int MIN_LOG_VECTOR_SIZE = 3;
    private static final int MAX_LOG_VECTOR_SIZE = 15;
    private static final int NO_VECTOR = -1;
    private static final int NO_EXCEPTION = -1;
    private static final ValueLayout.OfShort UINT16 = JAVA_SHORT_UNALIGNED.withOrder(LITTLE_ENDIAN);

    private final ValueType valueType;

    private MemorySegment page;
    private int logVectorSize;
    private int elementCount;
    private int vectorCount;
    private int position;

    private int vectorIndex = NO_VECTOR;
    private int vectorLength;
    private int exponent;
    private int factor;
    private int exceptionCount;
    private long frameOfReference;
    private int bitWidth;
    private long deltaMask;
    private long packedOffset;
    private long exceptionPositionsOffset;
    private long exceptionValuesOffset;
    private boolean exceptionsAscending;
    private int exceptionCursor;

    private int runStart;
    private int runEnd;

    AlpPageCursor(ValueType valueType) {
        this.valueType = valueType;
    }

    /**
     * Positions the cursor at the first value of {@code alpPage}, the page's value bytes after levels and
     * decompression.
     *
     * @throws UnsupportedFeatureException for a compression mode other than ALP (0) or an integer encoding other than
     *     frame of reference with bit packing (0)
     */
    void load(MemorySegment alpPage) {
        this.page = alpPage;
        requirePageBytes(PAGE_HEADER_BYTES, "the ALP page header");
        int compressionMode = unsignedByteAt(0);
        int integerEncoding = unsignedByteAt(1);
        requireDefinedLayout(compressionMode, integerEncoding);
        logVectorSize = unsignedByteAt(2);
        elementCount = page.get(ParquetLayouts.INT32, 3);
        requireValidPageHeader();
        vectorCount = (int) ((elementCount + vectorSize() - 1L) >>> logVectorSize);
        requirePageBytes(PAGE_HEADER_BYTES + (long) vectorCount * OFFSET_BYTES, "the ALP vector offsets");
        position = 0;
        vectorIndex = NO_VECTOR;
    }

    /** Fails unless {@code count} more values remain in the page. */
    void requireAvailable(int count) {
        if (count < 0 || count > elementCount - position) {
            throw malformed("cannot read " + count + " values at position " + position + " of a page holding "
                    + elementCount + " values");
        }
    }

    /**
     * Starts a run of at most {@code requested} values at the cursor, entering the vector holding the next value.
     *
     * @return the run length, cut short at the end of the vector
     */
    int beginRun(int requested) {
        requireAvailable(1);
        int index = position >>> logVectorSize;
        if (index != vectorIndex) {
            enterVector(index);
        }
        runStart = position - (index << logVectorSize);
        runEnd = runStart + Math.min(requested, vectorLength - runStart);
        skipExceptionsBefore(runStart);
        return runEnd - runStart;
    }

    /** Consumes the {@code count} values of the current run. */
    void endRun(int count) {
        position += count;
    }

    /** Steps over {@code count} values without decoding them; vectors skipped as a whole are never read. */
    void skip(int count) {
        requireAvailable(count);
        position += count;
    }

    /** Index within the current vector of the run's first value. */
    int runStart() {
        return runStart;
    }

    int exponent() {
        return exponent;
    }

    int factor() {
        return factor;
    }

    /** The current vector's frame of reference, sign-extended for FLOAT pages. */
    long frameOfReference() {
        return frameOfReference;
    }

    /**
     * The unsigned delta of the value at {@code indexInVector}: {@code bitWidth} bits read least significant bit first,
     * the packing order of the RLE/bit-packing hybrid encoding.
     */
    long packedDelta(int indexInVector) {
        long bitPosition = (long) indexInVector * bitWidth;
        long byteOffset = packedOffset + (bitPosition >>> 3);
        int shift = (int) (bitPosition & 7);
        long bits = littleEndianWordAt(byteOffset) >>> shift;
        if (shift + bitWidth > Long.SIZE) {
            bits |= (long) unsignedByteAt(byteOffset + Long.BYTES) << (Long.SIZE - shift);
        }
        return bits & deltaMask;
    }

    /** The first exception positioned inside the current run, or a negative value when there is none. */
    int firstExceptionInRun() {
        int candidate = exceptionsAscending ? exceptionCursor : 0;
        return nextExceptionFrom(candidate);
    }

    /**
     * The exception after {@code previous} positioned inside the current run, or a negative value when none is left.
     */
    int nextExceptionInRun(int previous) {
        return nextExceptionFrom(previous + 1);
    }

    /** Index within the current vector of the value replaced by {@code exception}. */
    int exceptionPosition(int exception) {
        long offset = exceptionPositionsOffset + (long) exception * EXCEPTION_POSITION_BYTES;
        return Short.toUnsignedInt(page.get(UINT16, offset));
    }

    /** The stored IEEE 754 bits of a FLOAT page's exception. */
    int exceptionIntBits(int exception) {
        return page.get(ParquetLayouts.INT32, exceptionValueOffset(exception));
    }

    /** The stored IEEE 754 bits of a DOUBLE page's exception. */
    long exceptionLongBits(int exception) {
        return page.get(ParquetLayouts.INT64, exceptionValueOffset(exception));
    }

    private void requireDefinedLayout(int compressionMode, int integerEncoding) {
        if (compressionMode != 0) {
            throw new UnsupportedFeatureException("ALP compression mode " + compressionMode + " is not supported");
        }
        if (integerEncoding != 0) {
            throw new UnsupportedFeatureException("ALP integer encoding " + integerEncoding + " is not supported");
        }
    }

    private void requireValidPageHeader() {
        if (logVectorSize < MIN_LOG_VECTOR_SIZE || logVectorSize > MAX_LOG_VECTOR_SIZE) {
            throw malformed("log_vector_size " + logVectorSize + " is outside [" + MIN_LOG_VECTOR_SIZE + ", "
                    + MAX_LOG_VECTOR_SIZE + "]");
        }
        if (elementCount < 0) {
            throw malformed("negative element count " + elementCount);
        }
    }

    private int vectorSize() {
        return 1 << logVectorSize;
    }

    /**
     * Reads and validates the header and section bounds of vector {@code index}. One pass over the exception positions
     * validates them and tells whether they ascend, as required by the forward-only exception cursor.
     */
    private void enterVector(int index) {
        vectorIndex = NO_VECTOR;
        long start = vectorStart(index);
        vectorLength = (int) Math.min(vectorSize(), elementCount - ((long) index << logVectorSize));
        readVectorHeader(start, index);
        locateDataSections(start, index);
        scanExceptionPositions(index);
        exceptionCursor = 0;
        vectorIndex = index;
    }

    /** Absolute offset of vector {@code index}; the stored offset counts from the start of the offset array. */
    private long vectorStart(int index) {
        long offsetPosition = PAGE_HEADER_BYTES + (long) index * OFFSET_BYTES;
        long offset = Integer.toUnsignedLong(page.get(ParquetLayouts.INT32, offsetPosition));
        long start = PAGE_HEADER_BYTES + offset;
        boolean pointsIntoOffsetArray = offset < (long) vectorCount * OFFSET_BYTES;
        boolean headerOverrunsPage = start + valueType.vectorHeaderBytes() > page.byteSize();
        if (pointsIntoOffsetArray || headerOverrunsPage) {
            throw malformed("vector " + index + " offset " + offset + " lies outside the vector data of a "
                    + page.byteSize() + "-byte page");
        }
        return start;
    }

    private void readVectorHeader(long start, int index) {
        exponent = unsignedByteAt(start);
        factor = unsignedByteAt(start + 1);
        exceptionCount = Short.toUnsignedInt(page.get(UINT16, start + 2));
        long forInfo = start + ALP_INFO_BYTES;
        frameOfReference = valueType.readFrameOfReference(page, forInfo);
        bitWidth = unsignedByteAt(forInfo + valueType.valueBytes);
        requireValidVectorHeader(index);
        deltaMask = bitWidth == 0 ? 0L : -1L >>> (Long.SIZE - bitWidth);
    }

    private void requireValidVectorHeader(int index) {
        if (exponent > valueType.maxExponent || factor > exponent) {
            throw malformed("vector " + index + " has exponent " + exponent + " and factor " + factor + "; a "
                    + valueType + " page allows 0 <= factor <= exponent <= " + valueType.maxExponent);
        }
        if (bitWidth > valueType.maxBitWidth()) {
            throw malformed("vector " + index + " has bit width " + bitWidth + "; a " + valueType
                    + " page allows at most " + valueType.maxBitWidth());
        }
        if (exceptionCount > vectorLength) {
            throw malformed(
                    "vector " + index + " declares " + exceptionCount + " exceptions for " + vectorLength + " values");
        }
    }

    private void locateDataSections(long start, int index) {
        packedOffset = start + valueType.vectorHeaderBytes();
        long packedBytes = ((long) vectorLength * bitWidth + 7) >>> 3;
        exceptionPositionsOffset = packedOffset + packedBytes;
        exceptionValuesOffset = exceptionPositionsOffset + (long) exceptionCount * EXCEPTION_POSITION_BYTES;
        long end = exceptionValuesOffset + (long) exceptionCount * valueType.valueBytes;
        if (end > page.byteSize()) {
            throw malformed("vector " + index + " ends at byte " + end + ", past the end of a " + page.byteSize()
                    + "-byte page");
        }
    }

    private void scanExceptionPositions(int index) {
        exceptionsAscending = true;
        int previous = 0;
        for (int exception = 0; exception < exceptionCount; exception++) {
            int exceptionPosition = exceptionPosition(exception);
            if (exceptionPosition >= vectorLength) {
                throw malformed("vector " + index + " has an exception at position " + exceptionPosition + " of "
                        + vectorLength + " values");
            }
            if (exceptionPosition < previous) {
                exceptionsAscending = false;
            }
            previous = exceptionPosition;
        }
    }

    /** Moves the forward-only exception cursor past the exceptions preceding {@code start}, when positions ascend. */
    private void skipExceptionsBefore(int start) {
        if (!exceptionsAscending) {
            return;
        }
        while (exceptionCursor < exceptionCount && exceptionPosition(exceptionCursor) < start) {
            exceptionCursor++;
        }
    }

    private int nextExceptionFrom(int candidate) {
        for (int exception = candidate; exception < exceptionCount; exception++) {
            int exceptionPosition = exceptionPosition(exception);
            if (exceptionPosition >= runStart && exceptionPosition < runEnd) {
                return exception;
            }
            if (exceptionsAscending && exceptionPosition >= runEnd) {
                return NO_EXCEPTION;
            }
        }
        return NO_EXCEPTION;
    }

    private long exceptionValueOffset(int exception) {
        return exceptionValuesOffset + (long) exception * valueType.valueBytes;
    }

    /**
     * Eight little-endian bytes at {@code byteOffset}; near the end of the page the missing high bytes read as zero.
     * Bytes past a value's own bits are masked off by the caller.
     */
    private long littleEndianWordAt(long byteOffset) {
        if (byteOffset + Long.BYTES <= page.byteSize()) {
            return page.get(ParquetLayouts.INT64, byteOffset);
        }
        long word = 0L;
        long available = page.byteSize() - byteOffset;
        for (int i = 0; i < available; i++) {
            word |= (long) unsignedByteAt(byteOffset + i) << (i * Byte.SIZE);
        }
        return word;
    }

    private int unsignedByteAt(long offset) {
        return Byte.toUnsignedInt(page.get(JAVA_BYTE, offset));
    }

    private void requirePageBytes(long needed, String section) {
        if (page.byteSize() < needed) {
            throw malformed(section + " needs " + needed + " bytes but the page holds " + page.byteSize());
        }
    }

    private static MalformedFileException malformed(String detail) {
        return new MalformedFileException("ALP page: " + detail);
    }
}
