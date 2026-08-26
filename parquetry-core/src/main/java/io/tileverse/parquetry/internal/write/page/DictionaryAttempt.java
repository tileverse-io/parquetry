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

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.internal.write.ColumnChunkWriter;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Per-primitive-kind binding of a page dictionary encoder to the plain encoder the dictionary page writer needs. The
 * numeric kinds share one bits-keyed case ({@link NumericAttempt} over {@link PrimitiveDictionaryEncoder}); the binary
 * kinds one hash-keyed case ({@link BinaryAttempt} over {@link BinaryDictionaryEncoder}). Each case holds the plain
 * encoder that re-emits the dictionary's unique values as the dictionary page payload.
 *
 * <p>The {@link ColumnChunkWriter} owns one case per column and routes each {@code appendXxx} call to the matching
 * pattern. {@link #writeDictionaryPage} produces the dictionary page bytes against the held plain encoder once the
 * chunk closes.
 */
public sealed interface DictionaryAttempt permits DictionaryAttempt.NumericAttempt, DictionaryAttempt.BinaryAttempt {

    PageDictionaryEncoder encoder();

    /**
     * Writes the dictionary page for the values accumulated in {@link #encoder()}. The plain encoder for the dictionary
     * page payload is fixed per case and matches the column's primitive kind.
     */
    EncodedPage writeDictionaryPage(PageWriter pageWriter, LittleEndianSink dst) throws IOException;

    public record NumericAttempt(PrimitiveKind kind, PrimitiveDictionaryEncoder encoder) implements DictionaryAttempt {

        public static NumericAttempt create(PrimitiveKind kind, long byteLimit) {
            return new NumericAttempt(kind, new PrimitiveDictionaryEncoder(kind, byteLimit));
        }

        @Override
        public EncodedPage writeDictionaryPage(PageWriter pageWriter, LittleEndianSink dst) throws IOException {
            return switch (kind) {
                case INT32 -> {
                    int[] carrier = encoder.intCarrier();
                    yield pageWriter.writeDictionaryPage(carrier, carrier.length, new PlainInt32Encoder(), dst);
                }
                case INT64 -> {
                    long[] carrier = encoder.longCarrier();
                    yield pageWriter.writeDictionaryPage(carrier, carrier.length, new PlainInt64Encoder(), dst);
                }
                case FLOAT -> {
                    float[] carrier = encoder.floatCarrier();
                    yield pageWriter.writeDictionaryPage(carrier, carrier.length, new PlainFloatEncoder(), dst);
                }
                case DOUBLE -> {
                    double[] carrier = encoder.doubleCarrier();
                    yield pageWriter.writeDictionaryPage(carrier, carrier.length, new PlainDoubleEncoder(), dst);
                }
                default -> throw new ParquetWriteException("Numeric dictionary attempt cannot serve kind " + kind);
            };
        }
    }

    public record BinaryAttempt(BinaryDictionaryEncoder encoder, Encoder<BinaryPayload> plainEncoder)
            implements DictionaryAttempt {

        public static BinaryAttempt create(long byteLimit, Encoder<BinaryPayload> plainEncoder) {
            return new BinaryAttempt(BinaryDictionaryEncoder.variableLength(plainEncoder, byteLimit), plainEncoder);
        }

        public static BinaryAttempt createFixedLen(long byteLimit, int length) {
            Encoder<BinaryPayload> plainEncoder = new PlainFixedLenBinaryEncoder(length);
            return new BinaryAttempt(
                    BinaryDictionaryEncoder.fixedLength(plainEncoder, length, byteLimit), plainEncoder);
        }

        @Override
        public EncodedPage writeDictionaryPage(PageWriter pageWriter, LittleEndianSink dst) throws IOException {
            byte[][] carrier = encoder.dictionaryCarrier();
            // The plain encoder for binary dictionary values matches the column's case (BYTE_ARRAY uses length-
            // prefixed payload; FIXED_LEN_BYTE_ARRAY uses fixed-width payload). INT96 reuses the fixed-length encoder.
            return pageWriter.writeDictionaryPage(
                    new ArrayBinaryPayload(carrier, carrier.length), carrier.length, plainEncoder, dst);
        }
    }
}
