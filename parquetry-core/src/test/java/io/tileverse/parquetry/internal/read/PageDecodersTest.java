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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.IntBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.internal.read.page.AlpDoubleDecoder;
import io.tileverse.parquetry.internal.read.page.AlpFloatDecoder;
import io.tileverse.parquetry.internal.read.page.ByteStreamSplitDoubleDecoder;
import io.tileverse.parquetry.internal.read.page.ByteStreamSplitFloatDecoder;
import io.tileverse.parquetry.internal.read.page.DeltaBinaryPackedInt32Decoder;
import io.tileverse.parquetry.internal.read.page.DeltaBinaryPackedInt64Decoder;
import io.tileverse.parquetry.internal.read.page.DeltaByteArrayDecoder;
import io.tileverse.parquetry.internal.read.page.DeltaLengthByteArrayDecoder;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.internal.read.page.PlainBinaryDecoder;
import io.tileverse.parquetry.internal.read.page.PlainBooleanDecoder;
import io.tileverse.parquetry.internal.read.page.PlainDoubleDecoder;
import io.tileverse.parquetry.internal.read.page.PlainFixedLenBinaryDecoder;
import io.tileverse.parquetry.internal.read.page.PlainFloatDecoder;
import io.tileverse.parquetry.internal.read.page.PlainInt32Decoder;
import io.tileverse.parquetry.internal.read.page.PlainInt64Decoder;
import io.tileverse.parquetry.internal.read.page.PlainInt96Decoder;
import io.tileverse.parquetry.internal.read.page.RleBooleanDecoder;
import io.tileverse.parquetry.internal.read.page.RleDictionaryPageDecoder;
import io.tileverse.parquetry.schema.PrimitiveKind;

/** The decoder chosen for each column kind and page encoding, and the pairs without one. */
class PageDecodersTest {

    private static final int BYTE_WIDTH = 16;

    static Stream<Arguments> decoders() {
        return Stream.of(
                arguments(PrimitiveKind.BOOLEAN, Encoding.PLAIN, PlainBooleanDecoder.class),
                arguments(PrimitiveKind.BOOLEAN, Encoding.RLE, RleBooleanDecoder.class),
                arguments(PrimitiveKind.INT32, Encoding.PLAIN, PlainInt32Decoder.class),
                arguments(PrimitiveKind.INT32, Encoding.DELTA_BINARY_PACKED, DeltaBinaryPackedInt32Decoder.class),
                arguments(PrimitiveKind.INT64, Encoding.PLAIN, PlainInt64Decoder.class),
                arguments(PrimitiveKind.INT64, Encoding.DELTA_BINARY_PACKED, DeltaBinaryPackedInt64Decoder.class),
                arguments(PrimitiveKind.INT96, Encoding.PLAIN, PlainInt96Decoder.class),
                arguments(PrimitiveKind.FLOAT, Encoding.PLAIN, PlainFloatDecoder.class),
                arguments(PrimitiveKind.FLOAT, Encoding.BYTE_STREAM_SPLIT, ByteStreamSplitFloatDecoder.class),
                arguments(PrimitiveKind.FLOAT, Encoding.ALP, AlpFloatDecoder.class),
                arguments(PrimitiveKind.DOUBLE, Encoding.PLAIN, PlainDoubleDecoder.class),
                arguments(PrimitiveKind.DOUBLE, Encoding.BYTE_STREAM_SPLIT, ByteStreamSplitDoubleDecoder.class),
                arguments(PrimitiveKind.DOUBLE, Encoding.ALP, AlpDoubleDecoder.class),
                arguments(PrimitiveKind.BYTE_ARRAY, Encoding.PLAIN, PlainBinaryDecoder.class),
                arguments(
                        PrimitiveKind.BYTE_ARRAY, Encoding.DELTA_LENGTH_BYTE_ARRAY, DeltaLengthByteArrayDecoder.class),
                arguments(PrimitiveKind.BYTE_ARRAY, Encoding.DELTA_BYTE_ARRAY, DeltaByteArrayDecoder.class),
                arguments(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, Encoding.PLAIN, PlainFixedLenBinaryDecoder.class));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("decoders")
    void decoderPerKindAndEncoding(PrimitiveKind kind, Encoding encoding, Class<?> expected) {
        assertThat(PageDecoders.decoderFor(kind, () -> BYTE_WIDTH, encoding, null))
                .isInstanceOf(expected);
    }

    // BYTE_STREAM_SPLIT on INT32 is valid in the format, but this reader has no decoder for it.
    static Stream<Arguments> pairsWithoutDecoder() {
        return Stream.of(
                arguments(PrimitiveKind.INT32, Encoding.RLE),
                arguments(PrimitiveKind.FLOAT, Encoding.DELTA_BINARY_PACKED),
                arguments(PrimitiveKind.INT32, Encoding.DELTA_LENGTH_BYTE_ARRAY),
                arguments(PrimitiveKind.INT64, Encoding.DELTA_BYTE_ARRAY),
                arguments(PrimitiveKind.INT32, Encoding.BYTE_STREAM_SPLIT),
                arguments(PrimitiveKind.INT32, Encoding.BIT_PACKED));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("pairsWithoutDecoder")
    void pairWithoutDecoderFails(PrimitiveKind kind, Encoding encoding) {
        assertThatThrownBy(() -> PageDecoders.decoderFor(kind, () -> BYTE_WIDTH, encoding, null))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining(encoding.name());
    }

    @ParameterizedTest
    @EnumSource(
            value = Encoding.class,
            names = {"PLAIN_DICTIONARY", "RLE_DICTIONARY"})
    void dictionaryPageDecoder(Encoding encoding) {
        Dictionary<Integer> dictionary = new Dictionary.IntDict(IntBuffer.wrap(new int[] {1, 2, 3}));

        assertThat(PageDecoders.decoderFor(PrimitiveKind.INT32, () -> BYTE_WIDTH, encoding, dictionary))
                .isInstanceOf(RleDictionaryPageDecoder.class);
    }

    @Test
    void dictionaryPageNeedsDictionary() {
        assertThatThrownBy(() ->
                        PageDecoders.decoderFor(PrimitiveKind.INT32, () -> BYTE_WIDTH, Encoding.RLE_DICTIONARY, null))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("needs a dictionary page");
    }
}
