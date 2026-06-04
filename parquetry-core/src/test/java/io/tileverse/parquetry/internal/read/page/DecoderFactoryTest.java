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
package io.tileverse.parquetry.internal.read.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.IntBuffer;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.UnknownCodeException;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Verifies the {@link DecoderFactory} dispatch table: correct decoder type for each valid (Encoding, PrimitiveKind)
 * combination, and clear exceptions for invalid combinations.
 */
class DecoderFactoryTest {

    // ---- PLAIN dispatch ----

    @Test
    void plainBoolean() {
        PageDecoder<?> decoder = decoderFor(Encoding.PLAIN, PrimitiveKind.BOOLEAN);
        assertThat(decoder).isInstanceOf(PlainBooleanDecoder.class);
    }

    @Test
    void plainInt32() {
        PageDecoder<?> decoder = decoderFor(Encoding.PLAIN, PrimitiveKind.INT32);
        assertThat(decoder).isInstanceOf(PlainInt32Decoder.class);
    }

    @Test
    void plainInt64() {
        PageDecoder<?> decoder = decoderFor(Encoding.PLAIN, PrimitiveKind.INT64);
        assertThat(decoder).isInstanceOf(PlainInt64Decoder.class);
    }

    @Test
    void plainInt96() {
        PageDecoder<?> decoder = decoderFor(Encoding.PLAIN, PrimitiveKind.INT96);
        assertThat(decoder).isInstanceOf(PlainInt96Decoder.class);
    }

    @Test
    void plainFloat() {
        PageDecoder<?> decoder = decoderFor(Encoding.PLAIN, PrimitiveKind.FLOAT);
        assertThat(decoder).isInstanceOf(PlainFloatDecoder.class);
    }

    @Test
    void plainDouble() {
        PageDecoder<?> decoder = decoderFor(Encoding.PLAIN, PrimitiveKind.DOUBLE);
        assertThat(decoder).isInstanceOf(PlainDoubleDecoder.class);
    }

    @Test
    void plainByteArray() {
        PageDecoder<?> decoder = decoderFor(Encoding.PLAIN, PrimitiveKind.BYTE_ARRAY);
        assertThat(decoder).isInstanceOf(PlainBinaryDecoder.class);
    }

    @Test
    void plainFixedLenByteArray_withTypeLength() {
        PageDecoder<?> decoder = DecoderFactory.decoderFor(
                Encoding.PLAIN, PrimitiveKind.FIXED_LEN_BYTE_ARRAY, OptionalInt.of(16), Optional.empty());
        assertThat(decoder).isInstanceOf(PlainFixedLenBinaryDecoder.class);
    }

    @Test
    @SuppressWarnings("java:S5778") // .empty() can't really throw RTE
    void plainFixedLenByteArray_withoutTypeLength_throws() {
        assertThatExceptionOfType(MalformedFileException.class)
                .isThrownBy(() -> DecoderFactory.decoderFor(
                        Encoding.PLAIN, PrimitiveKind.FIXED_LEN_BYTE_ARRAY, OptionalInt.empty(), Optional.empty()))
                .withMessageContaining("typeLength required");
    }

    // ---- RLE dispatch ----

    @Test
    void rleBoolean() {
        PageDecoder<?> decoder = decoderFor(Encoding.RLE, PrimitiveKind.BOOLEAN);
        assertThat(decoder).isInstanceOf(RleBooleanDecoder.class);
    }

    @Test
    void rleNonBoolean_throws() {
        assertThatExceptionOfType(MalformedFileException.class)
                .isThrownBy(() -> decoderFor(Encoding.RLE, PrimitiveKind.INT32))
                .withMessageContaining("BOOLEAN");
    }

    // ---- Dictionary dispatch ----

    @Test
    void plainDictionary_withDictionary() {
        Dictionary<Integer> dict = new Dictionary.IntDict(intBuf(1, 2, 3));
        PageDecoder<?> decoder = DecoderFactory.decoderFor(
                Encoding.PLAIN_DICTIONARY, PrimitiveKind.INT32, OptionalInt.empty(), Optional.of(dict));
        assertThat(decoder).isInstanceOf(RleDictionaryPageDecoder.class);
    }

    @Test
    void rleDictionary_withDictionary() {
        Dictionary<Integer> dict = new Dictionary.IntDict(intBuf(10, 20));
        PageDecoder<?> decoder = DecoderFactory.decoderFor(
                Encoding.RLE_DICTIONARY, PrimitiveKind.INT32, OptionalInt.empty(), Optional.of(dict));
        assertThat(decoder).isInstanceOf(RleDictionaryPageDecoder.class);
    }

    @Test
    @SuppressWarnings("java:S5778") // .empty() can't really throw RTE
    void dictionary_withoutDictionary_throws() {
        assertThatExceptionOfType(MalformedFileException.class)
                .isThrownBy(() -> DecoderFactory.decoderFor(
                        Encoding.RLE_DICTIONARY, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty()))
                .withMessageContaining("Dictionary");
    }

    // ---- DELTA_BINARY_PACKED dispatch ----

    @Test
    void deltaBinaryPackedInt32() {
        PageDecoder<?> decoder = decoderFor(Encoding.DELTA_BINARY_PACKED, PrimitiveKind.INT32);
        assertThat(decoder).isInstanceOf(DeltaBinaryPackedInt32Decoder.class);
    }

    @Test
    void deltaBinaryPackedInt64() {
        PageDecoder<?> decoder = decoderFor(Encoding.DELTA_BINARY_PACKED, PrimitiveKind.INT64);
        assertThat(decoder).isInstanceOf(DeltaBinaryPackedInt64Decoder.class);
    }

    @Test
    void deltaBinaryPackedFloat_throws() {
        assertThatExceptionOfType(MalformedFileException.class)
                .isThrownBy(() -> decoderFor(Encoding.DELTA_BINARY_PACKED, PrimitiveKind.FLOAT))
                .withMessageContaining("INT32/INT64");
    }

    // ---- DELTA_LENGTH_BYTE_ARRAY dispatch ----

    @Test
    void deltaLengthByteArray() {
        PageDecoder<?> decoder = decoderFor(Encoding.DELTA_LENGTH_BYTE_ARRAY, PrimitiveKind.BYTE_ARRAY);
        assertThat(decoder).isInstanceOf(DeltaLengthByteArrayDecoder.class);
    }

    @Test
    void deltaLengthByteArrayNonBinary_throws() {
        assertThatExceptionOfType(MalformedFileException.class)
                .isThrownBy(() -> decoderFor(Encoding.DELTA_LENGTH_BYTE_ARRAY, PrimitiveKind.INT32))
                .withMessageContaining("BYTE_ARRAY");
    }

    // ---- DELTA_BYTE_ARRAY dispatch ----

    @Test
    void deltaByteArray() {
        PageDecoder<?> decoder = decoderFor(Encoding.DELTA_BYTE_ARRAY, PrimitiveKind.BYTE_ARRAY);
        assertThat(decoder).isInstanceOf(DeltaByteArrayDecoder.class);
    }

    @Test
    void deltaByteArrayNonBinary_throws() {
        assertThatExceptionOfType(MalformedFileException.class)
                .isThrownBy(() -> decoderFor(Encoding.DELTA_BYTE_ARRAY, PrimitiveKind.INT64))
                .withMessageContaining("BYTE_ARRAY");
    }

    // ---- BYTE_STREAM_SPLIT dispatch ----

    @Test
    void byteStreamSplitFloat() {
        PageDecoder<?> decoder = decoderFor(Encoding.BYTE_STREAM_SPLIT, PrimitiveKind.FLOAT);
        assertThat(decoder).isInstanceOf(ByteStreamSplitFloatDecoder.class);
    }

    @Test
    void byteStreamSplitDouble() {
        PageDecoder<?> decoder = decoderFor(Encoding.BYTE_STREAM_SPLIT, PrimitiveKind.DOUBLE);
        assertThat(decoder).isInstanceOf(ByteStreamSplitDoubleDecoder.class);
    }

    @Test
    void byteStreamSplitInt32_throws() {
        assertThatExceptionOfType(MalformedFileException.class)
                .isThrownBy(() -> decoderFor(Encoding.BYTE_STREAM_SPLIT, PrimitiveKind.INT32))
                .withMessageContaining("FLOAT/DOUBLE");
    }

    // ---- Unsupported encodings ----

    @Test
    void bitPacked_throws() {
        assertThatThrownBy(() -> decoderFor(Encoding.BIT_PACKED, PrimitiveKind.INT32))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("BIT_PACKED");
    }

    @Test
    void unknownEncodingWireCodeRejectedAtDeserialize() {
        // GROUP_VAR_INT (wire code 1) was previously a placeholder in the enum so DecoderFactory could reject it. Now
        // that Encoding.valueOf(int) throws on unknown codes, the rejection happens one layer earlier - inside the
        // page-header deserializer - and DecoderFactory only ever sees defined variants.
        assertThatThrownBy(() -> Encoding.valueOf(1))
                .isInstanceOf(UnknownCodeException.class)
                .hasMessageContaining("Unknown Encoding");
    }

    // ---- helper ----

    private static PageDecoder<?> decoderFor(Encoding encoding, PrimitiveKind kind) {
        return DecoderFactory.decoderFor(encoding, kind, OptionalInt.empty(), Optional.empty());
    }

    private static IntBuffer intBuf(int... values) {
        return IntBuffer.wrap(values);
    }
}
