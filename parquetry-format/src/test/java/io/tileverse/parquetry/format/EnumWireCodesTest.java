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
package io.tileverse.parquetry.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Pins each format enum's Thrift wire code to the values defined in {@code parquet.thrift}. If a contributor adds a
 * constant out of order, drops a slot, or otherwise drifts the {@code value()} accessor away from the spec, this
 * tripwire fails before any runtime data gets misread.
 *
 * <p>Each assertion also covers the per-enum {@code valueOf(int)} switch by performing a roundtrip lookup; a missing
 * case in the switch expression surfaces here too.
 */
class EnumWireCodesTest {

    @Test
    void physicalTypeCodesMatchThrift() {
        assertCode(PhysicalType.BOOLEAN, 0);
        assertCode(PhysicalType.INT32, 1);
        assertCode(PhysicalType.INT64, 2);
        assertCode(PhysicalType.INT96, 3);
        assertCode(PhysicalType.FLOAT, 4);
        assertCode(PhysicalType.DOUBLE, 5);
        assertCode(PhysicalType.BYTE_ARRAY, 6);
        assertCode(PhysicalType.FIXED_LEN_BYTE_ARRAY, 7);
        assertThatThrownBy(() -> PhysicalType.valueOf(99))
                .isInstanceOf(UnknownCodeException.class)
                .hasMessageContaining("99");
    }

    @Test
    void fieldRepetitionTypeCodesMatchThrift() {
        assertCode(FieldRepetitionType.REQUIRED, 0);
        assertCode(FieldRepetitionType.OPTIONAL, 1);
        assertCode(FieldRepetitionType.REPEATED, 2);
        assertThatThrownBy(() -> FieldRepetitionType.valueOf(3)).isInstanceOf(UnknownCodeException.class);
    }

    @Test
    void encodingCodesMatchThriftIncludingGapAtOne() {
        // Wire code 1 is the deprecated GROUP_VAR_INT slot - parquetry doesn't model it; valueOf(1) must throw.
        assertCode(Encoding.PLAIN, 0);
        assertCode(Encoding.PLAIN_DICTIONARY, 2);
        assertCode(Encoding.RLE, 3);
        assertCode(Encoding.BIT_PACKED, 4);
        assertCode(Encoding.DELTA_BINARY_PACKED, 5);
        assertCode(Encoding.DELTA_LENGTH_BYTE_ARRAY, 6);
        assertCode(Encoding.DELTA_BYTE_ARRAY, 7);
        assertCode(Encoding.RLE_DICTIONARY, 8);
        assertCode(Encoding.BYTE_STREAM_SPLIT, 9);
        assertThatThrownBy(() -> Encoding.valueOf(1))
                .as("GROUP_VAR_INT slot must be rejected by the parquetry decoder")
                .isInstanceOf(UnknownCodeException.class);
    }

    @Test
    void compressionCodecCodesMatchThrift() {
        assertCode(CompressionCodec.UNCOMPRESSED, 0);
        assertCode(CompressionCodec.SNAPPY, 1);
        assertCode(CompressionCodec.GZIP, 2);
        assertCode(CompressionCodec.LZO, 3);
        assertCode(CompressionCodec.BROTLI, 4);
        assertCode(CompressionCodec.LZ4, 5);
        assertCode(CompressionCodec.ZSTD, 6);
        assertCode(CompressionCodec.LZ4_RAW, 7);
    }

    @Test
    void pageTypeCodesMatchThrift() {
        assertCode(PageType.DATA_PAGE, 0);
        assertCode(PageType.INDEX_PAGE, 1);
        assertCode(PageType.DICTIONARY_PAGE, 2);
        assertCode(PageType.DATA_PAGE_V2, 3);
    }

    @Test
    void boundaryOrderCodesMatchThrift() {
        assertCode(BoundaryOrder.UNORDERED, 0);
        assertCode(BoundaryOrder.ASCENDING, 1);
        assertCode(BoundaryOrder.DESCENDING, 2);
    }

    @Test
    void edgeInterpolationAlgorithmCodesMatchThrift() {
        assertCode(EdgeInterpolationAlgorithm.SPHERICAL, 0);
        assertCode(EdgeInterpolationAlgorithm.VINCENTY, 1);
        assertCode(EdgeInterpolationAlgorithm.THOMAS, 2);
        assertCode(EdgeInterpolationAlgorithm.ANDOYER, 3);
        assertCode(EdgeInterpolationAlgorithm.KARNEY, 4);
    }

    @Test
    void convertedTypeCodesMatchThrift() {
        assertCode(ConvertedType.UTF8, 0);
        assertCode(ConvertedType.MAP, 1);
        assertCode(ConvertedType.MAP_KEY_VALUE, 2);
        assertCode(ConvertedType.LIST, 3);
        assertCode(ConvertedType.ENUM, 4);
        assertCode(ConvertedType.DECIMAL, 5);
        assertCode(ConvertedType.DATE, 6);
        assertCode(ConvertedType.TIME_MILLIS, 7);
        assertCode(ConvertedType.TIME_MICROS, 8);
        assertCode(ConvertedType.TIMESTAMP_MILLIS, 9);
        assertCode(ConvertedType.TIMESTAMP_MICROS, 10);
        assertCode(ConvertedType.UINT_8, 11);
        assertCode(ConvertedType.UINT_16, 12);
        assertCode(ConvertedType.UINT_32, 13);
        assertCode(ConvertedType.UINT_64, 14);
        assertCode(ConvertedType.INT_8, 15);
        assertCode(ConvertedType.INT_16, 16);
        assertCode(ConvertedType.INT_32, 17);
        assertCode(ConvertedType.INT_64, 18);
        assertCode(ConvertedType.JSON, 19);
        assertCode(ConvertedType.BSON, 20);
        assertCode(ConvertedType.INTERVAL, 21);
    }

    private static void assertCode(PhysicalType v, int code) {
        assertThat(v.value()).isEqualTo(code);
        assertThat(PhysicalType.valueOf(code)).isSameAs(v);
    }

    private static void assertCode(FieldRepetitionType v, int code) {
        assertThat(v.value()).isEqualTo(code);
        assertThat(FieldRepetitionType.valueOf(code)).isSameAs(v);
    }

    private static void assertCode(Encoding v, int code) {
        assertThat(v.value()).isEqualTo(code);
        assertThat(Encoding.valueOf(code)).isSameAs(v);
    }

    private static void assertCode(CompressionCodec v, int code) {
        assertThat(v.value()).isEqualTo(code);
        assertThat(CompressionCodec.valueOf(code)).isSameAs(v);
    }

    private static void assertCode(PageType v, int code) {
        assertThat(v.value()).isEqualTo(code);
        assertThat(PageType.valueOf(code)).isSameAs(v);
    }

    private static void assertCode(BoundaryOrder v, int code) {
        assertThat(v.value()).isEqualTo(code);
        assertThat(BoundaryOrder.valueOf(code)).isSameAs(v);
    }

    private static void assertCode(EdgeInterpolationAlgorithm v, int code) {
        assertThat(v.value()).isEqualTo(code);
        assertThat(EdgeInterpolationAlgorithm.valueOf(code)).isSameAs(v);
    }

    private static void assertCode(ConvertedType v, int code) {
        assertThat(v.value()).isEqualTo(code);
        assertThat(ConvertedType.valueOf(code)).isSameAs(v);
    }
}
