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
package io.tileverse.parquetry.format.enums;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EnumOrdinalsTest {

    @Test
    void typeOrdinalsMatchThrift() {
        assertThat(Type.BOOLEAN.ordinal()).isZero();
        assertThat(Type.INT32.ordinal()).isEqualTo(1);
        assertThat(Type.INT64.ordinal()).isEqualTo(2);
        assertThat(Type.INT96.ordinal()).isEqualTo(3);
        assertThat(Type.FLOAT.ordinal()).isEqualTo(4);
        assertThat(Type.DOUBLE.ordinal()).isEqualTo(5);
        assertThat(Type.BYTE_ARRAY.ordinal()).isEqualTo(6);
        assertThat(Type.FIXED_LEN_BYTE_ARRAY.ordinal()).isEqualTo(7);
    }

    @Test
    void fieldRepetitionTypeOrdinalsMatchThrift() {
        assertThat(FieldRepetitionType.REQUIRED.ordinal()).isZero();
        assertThat(FieldRepetitionType.OPTIONAL.ordinal()).isEqualTo(1);
        assertThat(FieldRepetitionType.REPEATED.ordinal()).isEqualTo(2);
    }

    @Test
    void encodingOrdinalsMatchThrift() {
        assertThat(Encoding.PLAIN.ordinal()).isZero();
        assertThat(Encoding.PLAIN_DICTIONARY.ordinal()).isEqualTo(2);
        assertThat(Encoding.RLE.ordinal()).isEqualTo(3);
        assertThat(Encoding.BIT_PACKED.ordinal()).isEqualTo(4);
        assertThat(Encoding.DELTA_BINARY_PACKED.ordinal()).isEqualTo(5);
        assertThat(Encoding.DELTA_LENGTH_BYTE_ARRAY.ordinal()).isEqualTo(6);
        assertThat(Encoding.DELTA_BYTE_ARRAY.ordinal()).isEqualTo(7);
        assertThat(Encoding.RLE_DICTIONARY.ordinal()).isEqualTo(8);
        assertThat(Encoding.BYTE_STREAM_SPLIT.ordinal()).isEqualTo(9);
    }

    @Test
    void compressionCodecOrdinalsMatchThrift() {
        assertThat(CompressionCodec.UNCOMPRESSED.ordinal()).isZero();
        assertThat(CompressionCodec.SNAPPY.ordinal()).isEqualTo(1);
        assertThat(CompressionCodec.GZIP.ordinal()).isEqualTo(2);
        assertThat(CompressionCodec.LZO.ordinal()).isEqualTo(3);
        assertThat(CompressionCodec.BROTLI.ordinal()).isEqualTo(4);
        assertThat(CompressionCodec.LZ4.ordinal()).isEqualTo(5);
        assertThat(CompressionCodec.ZSTD.ordinal()).isEqualTo(6);
        assertThat(CompressionCodec.LZ4_RAW.ordinal()).isEqualTo(7);
    }

    @Test
    void boundaryOrderOrdinalsMatchThrift() {
        assertThat(BoundaryOrder.UNORDERED.ordinal()).isZero();
        assertThat(BoundaryOrder.ASCENDING.ordinal()).isEqualTo(1);
        assertThat(BoundaryOrder.DESCENDING.ordinal()).isEqualTo(2);
    }

    @Test
    void edgeInterpolationAlgorithmOrdinalsMatchThrift() {
        assertThat(EdgeInterpolationAlgorithm.SPHERICAL.ordinal()).isZero();
        assertThat(EdgeInterpolationAlgorithm.VINCENTY.ordinal()).isEqualTo(1);
        assertThat(EdgeInterpolationAlgorithm.THOMAS.ordinal()).isEqualTo(2);
        assertThat(EdgeInterpolationAlgorithm.ANDOYER.ordinal()).isEqualTo(3);
        assertThat(EdgeInterpolationAlgorithm.KARNEY.ordinal()).isEqualTo(4);
    }
}
