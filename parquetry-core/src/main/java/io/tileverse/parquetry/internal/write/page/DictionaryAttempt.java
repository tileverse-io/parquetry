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
package io.tileverse.parquetry.internal.write.page;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import io.tileverse.parquetry.internal.write.ColumnChunkWriter;

/**
 * Per-primitive-kind binding of a {@link DictionaryAttemptEncoder} to the carrier types the dictionary page writer
 * needs. Each case captures the value representation the encoder keys against (boxed values for the numeric kinds,
 * content-hashed bytes for the binary kind), the typed carrier the fallback encoder consumes, and the plain encoder
 * that re-emits the dictionary's unique values as the dictionary page payload.
 *
 * <p>The {@link ColumnChunkWriter} owns one case per column and routes each {@code appendXxx} call to the matching
 * pattern. {@link #writeDictionaryPage} produces the dictionary page bytes against the held plain encoder once the
 * chunk closes.
 */
public sealed interface DictionaryAttempt
        permits DictionaryAttempt.IntAttempt,
                DictionaryAttempt.LongAttempt,
                DictionaryAttempt.FloatAttempt,
                DictionaryAttempt.DoubleAttempt,
                DictionaryAttempt.BinaryAttempt {

    PageDictionaryEncoder encoder();

    /**
     * Writes the dictionary page for the values accumulated in {@link #encoder()}. The plain encoder for the dictionary
     * page payload is fixed per case and matches the column's primitive kind.
     */
    EncodedPage writeDictionaryPage(PageWriter pageWriter, WritableByteChannel dst) throws IOException;

    public record IntAttempt(DictionaryAttemptEncoder<Integer, int[]> encoder) implements DictionaryAttempt {

        public static IntAttempt create(long byteLimit) {
            DictionaryAttemptEncoder<Integer, int[]> enc = new DictionaryAttemptEncoder<>(
                    new PlainInt32Encoder(), IntAttempt::toCarrier, value -> Integer.BYTES, byteLimit);
            return new IntAttempt(enc);
        }

        private static int[] toCarrier(List<Integer> values) {
            int[] out = new int[values.size()];
            for (int i = 0; i < values.size(); i++) {
                out[i] = values.get(i);
            }
            return out;
        }

        @Override
        public EncodedPage writeDictionaryPage(PageWriter pageWriter, WritableByteChannel dst) throws IOException {
            int[] carrier = toCarrier(encoder.dictionaryValues());
            return pageWriter.writeDictionaryPage(carrier, carrier.length, new PlainInt32Encoder(), dst);
        }
    }

    record LongAttempt(DictionaryAttemptEncoder<Long, long[]> encoder) implements DictionaryAttempt {

        public static LongAttempt create(long byteLimit) {
            DictionaryAttemptEncoder<Long, long[]> enc = new DictionaryAttemptEncoder<>(
                    new PlainInt64Encoder(), LongAttempt::toCarrier, value -> Long.BYTES, byteLimit);
            return new LongAttempt(enc);
        }

        private static long[] toCarrier(List<Long> values) {
            long[] out = new long[values.size()];
            for (int i = 0; i < values.size(); i++) {
                out[i] = values.get(i);
            }
            return out;
        }

        @Override
        public EncodedPage writeDictionaryPage(PageWriter pageWriter, WritableByteChannel dst) throws IOException {
            long[] carrier = toCarrier(encoder.dictionaryValues());
            return pageWriter.writeDictionaryPage(carrier, carrier.length, new PlainInt64Encoder(), dst);
        }
    }

    record FloatAttempt(DictionaryAttemptEncoder<Float, float[]> encoder) implements DictionaryAttempt {

        public static FloatAttempt create(long byteLimit) {
            DictionaryAttemptEncoder<Float, float[]> enc = new DictionaryAttemptEncoder<>(
                    new PlainFloatEncoder(), FloatAttempt::toCarrier, value -> Float.BYTES, byteLimit);
            return new FloatAttempt(enc);
        }

        private static float[] toCarrier(List<Float> values) {
            float[] out = new float[values.size()];
            for (int i = 0; i < values.size(); i++) {
                out[i] = values.get(i);
            }
            return out;
        }

        @Override
        public EncodedPage writeDictionaryPage(PageWriter pageWriter, WritableByteChannel dst) throws IOException {
            float[] carrier = toCarrier(encoder.dictionaryValues());
            return pageWriter.writeDictionaryPage(carrier, carrier.length, new PlainFloatEncoder(), dst);
        }
    }

    record DoubleAttempt(DictionaryAttemptEncoder<Double, double[]> encoder) implements DictionaryAttempt {

        public static DoubleAttempt create(long byteLimit) {
            DictionaryAttemptEncoder<Double, double[]> enc = new DictionaryAttemptEncoder<>(
                    new PlainDoubleEncoder(), DoubleAttempt::toCarrier, value -> Double.BYTES, byteLimit);
            return new DoubleAttempt(enc);
        }

        private static double[] toCarrier(List<Double> values) {
            double[] out = new double[values.size()];
            for (int i = 0; i < values.size(); i++) {
                out[i] = values.get(i);
            }
            return out;
        }

        @Override
        public EncodedPage writeDictionaryPage(PageWriter pageWriter, WritableByteChannel dst) throws IOException {
            double[] carrier = toCarrier(encoder.dictionaryValues());
            return pageWriter.writeDictionaryPage(carrier, carrier.length, new PlainDoubleEncoder(), dst);
        }
    }

    public record BinaryAttempt(BinaryDictionaryEncoder encoder, Encoder<byte[][]> plainEncoder)
            implements DictionaryAttempt {

        public static BinaryAttempt create(long byteLimit, Encoder<byte[][]> plainEncoder) {
            return new BinaryAttempt(BinaryDictionaryEncoder.variableLength(plainEncoder, byteLimit), plainEncoder);
        }

        public static BinaryAttempt createFixedLen(long byteLimit, int length) {
            Encoder<byte[][]> plainEncoder = new PlainFixedLenBinaryEncoder(length);
            return new BinaryAttempt(
                    BinaryDictionaryEncoder.fixedLength(plainEncoder, length, byteLimit), plainEncoder);
        }

        @Override
        public EncodedPage writeDictionaryPage(PageWriter pageWriter, WritableByteChannel dst) throws IOException {
            byte[][] carrier = encoder.dictionaryCarrier();
            // The plain encoder for binary dictionary values matches the column's case (BYTE_ARRAY uses length-
            // prefixed payload; FIXED_LEN_BYTE_ARRAY uses fixed-width payload). INT96 reuses the fixed-length encoder.
            return pageWriter.writeDictionaryPage(carrier, carrier.length, plainEncoder, dst);
        }
    }
}
