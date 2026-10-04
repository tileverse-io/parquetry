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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds the value bytes of ALP pages (parquet-format AlpEncoding.md) for decoder tests, either from explicit vector
 * fields or by encoding values with the specification's informative encoder. The encoder derives its power-of-ten
 * constants by parsing decimal literals, independently from the decoders' tables.
 */
public final class AlpPageBuilder {

    /** The value type of a page: the width of its frames of reference and exception values. */
    public enum Width {
        FLOAT(Float.BYTES),
        DOUBLE(Double.BYTES);

        private final int valueBytes;

        Width(int valueBytes) {
            this.valueBytes = valueBytes;
        }

        int valueBytes() {
            return valueBytes;
        }
    }

    /**
     * One vector given by its stored fields. Deltas are unsigned and exception values are raw IEEE 754 bits (the low 32
     * bits for FLOAT pages).
     */
    public record Vector(
            int exponent,
            int factor,
            long frameOfReference,
            int bitWidth,
            long[] deltas,
            int[] exceptionPositions,
            long[] exceptionBits) {

        static Vector withoutExceptions(int exponent, int factor, long frameOfReference, int bitWidth, long... deltas) {
            return new Vector(exponent, factor, frameOfReference, bitWidth, deltas, new int[0], new long[0]);
        }

        int length() {
            return deltas.length;
        }

        int exceptionCount() {
            return exceptionPositions.length;
        }

        /** Byte size of the serialized vector: AlpInfo, ForInfo, packed deltas, exception positions and values. */
        int byteSize(Width width) {
            int packedBytes = (int) (((long) length() * bitWidth + 7) / 8);
            int exceptionBytes = exceptionCount() * (Short.BYTES + width.valueBytes());
            return 4 + width.valueBytes() + 1 + packedBytes + exceptionBytes;
        }
    }

    public static final int PAGE_HEADER_BYTES = 7;

    private AlpPageBuilder() {}

    /** A page of explicit vectors; the element count is the sum of the vector lengths. */
    public static byte[] page(Width width, int logVectorSize, List<Vector> vectors) {
        int elementCount = vectors.stream().mapToInt(Vector::length).sum();
        int offsetArrayBytes = vectors.size() * Integer.BYTES;
        int vectorBytes =
                vectors.stream().mapToInt(vector -> vector.byteSize(width)).sum();
        ByteBuffer page = ByteBuffer.allocate(PAGE_HEADER_BYTES + offsetArrayBytes + vectorBytes)
                .order(ByteOrder.LITTLE_ENDIAN);
        writePageHeader(page, logVectorSize, elementCount);
        writeOffsets(page, width, vectors);
        for (Vector vector : vectors) {
            writeVector(page, width, vector);
        }
        return page.array();
    }

    /** Encodes {@code values} with one (exponent, factor) pair for all vectors. */
    public static byte[] floatPage(int logVectorSize, int exponent, int factor, float... values) {
        List<Vector> vectors = new ArrayList<>();
        int vectorSize = 1 << logVectorSize;
        for (int from = 0; from < values.length; from += vectorSize) {
            float[] slice = Arrays.copyOfRange(values, from, Math.min(values.length, from + vectorSize));
            vectors.add(encodeFloats(exponent, factor, slice));
        }
        return page(Width.FLOAT, logVectorSize, vectors);
    }

    /** Encodes {@code values} with one (exponent, factor) pair for all vectors. */
    public static byte[] doublePage(int logVectorSize, int exponent, int factor, double... values) {
        List<Vector> vectors = new ArrayList<>();
        int vectorSize = 1 << logVectorSize;
        for (int from = 0; from < values.length; from += vectorSize) {
            double[] slice = Arrays.copyOfRange(values, from, Math.min(values.length, from + vectorSize));
            vectors.add(encodeDoubles(exponent, factor, slice));
        }
        return page(Width.DOUBLE, logVectorSize, vectors);
    }

    /**
     * The informative encoder of the specification: values failing the decode round trip become exceptions, replaced by
     * the first encodable value before frame-of-reference encoding.
     */
    public static Vector encodeFloats(int exponent, int factor, float[] values) {
        long[] encoded = new long[values.length];
        List<Integer> exceptionPositions = new ArrayList<>();
        List<Long> exceptionBits = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            Long candidate = encodeFloat(values[i], exponent, factor);
            if (candidate == null) {
                exceptionPositions.add(i);
                exceptionBits.add(Integer.toUnsignedLong(Float.floatToRawIntBits(values[i])));
            } else {
                encoded[i] = candidate;
            }
        }
        return frameOfReferenceVector(exponent, factor, encoded, exceptionPositions, exceptionBits, 0xFFFFFFFFL);
    }

    /** The double counterpart of {@link #encodeFloats(int, int, float[])}. */
    public static Vector encodeDoubles(int exponent, int factor, double[] values) {
        long[] encoded = new long[values.length];
        List<Integer> exceptionPositions = new ArrayList<>();
        List<Long> exceptionBits = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            Long candidate = encodeDouble(values[i], exponent, factor);
            if (candidate == null) {
                exceptionPositions.add(i);
                exceptionBits.add(Double.doubleToRawLongBits(values[i]));
            } else {
                encoded[i] = candidate;
            }
        }
        return frameOfReferenceVector(exponent, factor, encoded, exceptionPositions, exceptionBits, -1L);
    }

    private static Long encodeFloat(float value, int exponent, int factor) {
        if (Float.isNaN(value) || Float.isInfinite(value) || isNegativeZero(value)) {
            return null;
        }
        float scaled = value * floatPowerOfTen(exponent) * floatPowerOfTen(-factor);
        if (Math.abs(scaled) > 2147483520.0f) {
            return null;
        }
        int rounded = (int) Math.rint(scaled);
        float decoded = (float) rounded * floatPowerOfTen(factor) * floatPowerOfTen(-exponent);
        if (Float.floatToRawIntBits(decoded) != Float.floatToRawIntBits(value)) {
            return null;
        }
        return (long) rounded;
    }

    private static Long encodeDouble(double value, int exponent, int factor) {
        if (Double.isNaN(value) || Double.isInfinite(value) || isNegativeZero(value)) {
            return null;
        }
        double scaled = value * doublePowerOfTen(exponent) * doublePowerOfTen(-factor);
        if (Math.abs(scaled) > 9223372036854774784.0) {
            return null;
        }
        long rounded = (long) Math.rint(scaled);
        double decoded = (double) rounded * doublePowerOfTen(factor) * doublePowerOfTen(-exponent);
        if (Double.doubleToRawLongBits(decoded) != Double.doubleToRawLongBits(value)) {
            return null;
        }
        return rounded;
    }

    private static boolean isNegativeZero(float value) {
        return Float.floatToRawIntBits(value) == Float.floatToRawIntBits(-0.0f);
    }

    private static boolean isNegativeZero(double value) {
        return Double.doubleToRawLongBits(value) == Double.doubleToRawLongBits(-0.0);
    }

    private static float floatPowerOfTen(int power) {
        return Float.parseFloat("1e" + power);
    }

    private static double doublePowerOfTen(int power) {
        return Double.parseDouble("1e" + power);
    }

    private static Vector frameOfReferenceVector(
            int exponent,
            int factor,
            long[] encoded,
            List<Integer> exceptionPositions,
            List<Long> exceptionBits,
            long deltaMask) {
        substitutePlaceholders(encoded, exceptionPositions);
        long frame = Arrays.stream(encoded).min().orElse(0L);
        long[] deltas = new long[encoded.length];
        long maxDelta = 0L;
        for (int i = 0; i < encoded.length; i++) {
            deltas[i] = (encoded[i] - frame) & deltaMask;
            maxDelta = Long.compareUnsigned(deltas[i], maxDelta) > 0 ? deltas[i] : maxDelta;
        }
        int bitWidth = Long.SIZE - Long.numberOfLeadingZeros(maxDelta);
        int[] positions =
                exceptionPositions.stream().mapToInt(Integer::intValue).toArray();
        long[] bits = exceptionBits.stream().mapToLong(Long::longValue).toArray();
        return new Vector(exponent, factor, frame, bitWidth, deltas, positions, bits);
    }

    private static void substitutePlaceholders(long[] encoded, List<Integer> exceptionPositions) {
        long placeholder = 0L;
        for (int i = 0; i < encoded.length; i++) {
            if (!exceptionPositions.contains(i)) {
                placeholder = encoded[i];
                break;
            }
        }
        for (int position : exceptionPositions) {
            encoded[position] = placeholder;
        }
    }

    private static void writePageHeader(ByteBuffer page, int logVectorSize, int elementCount) {
        page.put((byte) 0); // compression_mode: ALP
        page.put((byte) 0); // integer_encoding: FOR + bit-packing
        page.put((byte) logVectorSize);
        page.putInt(elementCount);
    }

    private static void writeOffsets(ByteBuffer page, Width width, List<Vector> vectors) {
        int offset = vectors.size() * Integer.BYTES;
        for (Vector vector : vectors) {
            page.putInt(offset);
            offset += vector.byteSize(width);
        }
    }

    private static void writeVector(ByteBuffer page, Width width, Vector vector) {
        page.put((byte) vector.exponent());
        page.put((byte) vector.factor());
        page.putShort((short) vector.exceptionCount());
        putValue(page, width, vector.frameOfReference());
        page.put((byte) vector.bitWidth());
        page.put(pack(vector.deltas(), vector.bitWidth()));
        for (int position : vector.exceptionPositions()) {
            page.putShort((short) position);
        }
        for (long bits : vector.exceptionBits()) {
            putValue(page, width, bits);
        }
    }

    private static void putValue(ByteBuffer page, Width width, long value) {
        if (width == Width.FLOAT) {
            page.putInt((int) value);
        } else {
            page.putLong(value);
        }
    }

    /** Packs deltas least significant bit first, one bit at a time. */
    static byte[] pack(long[] deltas, int bitWidth) {
        byte[] packed = new byte[(int) (((long) deltas.length * bitWidth + 7) / 8)];
        long bitPosition = 0;
        for (long delta : deltas) {
            for (int bit = 0; bit < bitWidth; bit++) {
                if (((delta >>> bit) & 1L) != 0) {
                    packed[(int) (bitPosition >>> 3)] |= (byte) (1 << (bitPosition & 7));
                }
                bitPosition++;
            }
        }
        return packed;
    }
}
