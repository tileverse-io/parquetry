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
package io.tileverse.parquetry.format;

/**
 * Value or level encoding applied within a page; mirror of {@code Encoding} in {@code parquet.thrift}.
 *
 * <p>The Thrift wire codes have a gap at 1 (a deprecated {@code GROUP_VAR_INT} that was never used in real files) and
 * skip {@link #PLAIN_DICTIONARY} ahead to 2. Each constant carries its code in {@link #value()}; deserializers resolve
 * incoming i32 values via {@link #valueOf(int)}, so the source-order of these constants is not load-bearing.
 */
public enum Encoding {
    PLAIN(0),
    PLAIN_DICTIONARY(2),
    RLE(3),
    BIT_PACKED(4), // legacy
    DELTA_BINARY_PACKED(5),
    DELTA_LENGTH_BYTE_ARRAY(6),
    DELTA_BYTE_ARRAY(7),
    RLE_DICTIONARY(8),
    BYTE_STREAM_SPLIT(9);

    private final int value;

    Encoding(int value) {
        this.value = value;
    }

    /** Thrift wire code for this constant, matching the value defined in {@code parquet.thrift}. */
    public int value() {
        return value;
    }

    /**
     * @throws UnknownCodeException if no defined variant carries that code (including the spec's reserved-but-unused
     *     slot at 1, {@code GROUP_VAR_INT}, which the parquetry decoder rejects rather than mapping to a placeholder)
     */
    public static Encoding valueOf(int code) {
        return switch (code) {
            case 0 -> PLAIN;
            case 2 -> PLAIN_DICTIONARY;
            case 3 -> RLE;
            case 4 -> BIT_PACKED;
            case 5 -> DELTA_BINARY_PACKED;
            case 6 -> DELTA_LENGTH_BYTE_ARRAY;
            case 7 -> DELTA_BYTE_ARRAY;
            case 8 -> RLE_DICTIONARY;
            case 9 -> BYTE_STREAM_SPLIT;
            default -> throw new UnknownCodeException("Unknown Encoding wire code: " + code);
        };
    }
}
