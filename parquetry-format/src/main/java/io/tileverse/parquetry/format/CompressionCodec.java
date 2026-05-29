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
 * Compression codec applied to a column chunk's page payloads; mirror of {@code CompressionCodec} in
 * {@code parquet.thrift}.
 *
 * <p>Selected via {@link ColumnMetaData#codec()}. {@link #LZ4} is deprecated in favor of {@link #LZ4_RAW}; {@link #LZO}
 * is rare in modern pipelines (historically encumbered by C-library licensing) but not deprecated by the spec.
 *
 * <p>Each constant carries its Thrift wire code in {@link #value()}; resolve incoming i32 codes via
 * {@link #valueOf(int)}.
 */
public enum CompressionCodec {
    UNCOMPRESSED(0),
    SNAPPY(1),
    GZIP(2),
    LZO(3),
    BROTLI(4),
    LZ4(5),
    ZSTD(6),
    LZ4_RAW(7);

    private final int value;

    CompressionCodec(int value) {
        this.value = value;
    }

    /** Thrift wire code for this constant, matching the value defined in {@code parquet.thrift}. */
    public int value() {
        return value;
    }

    /** @throws UnknownCodeException if no defined case carries that code */
    public static CompressionCodec valueOf(int code) {
        return switch (code) {
            case 0 -> UNCOMPRESSED;
            case 1 -> SNAPPY;
            case 2 -> GZIP;
            case 3 -> LZO;
            case 4 -> BROTLI;
            case 5 -> LZ4;
            case 6 -> ZSTD;
            case 7 -> LZ4_RAW;
            default -> throw new UnknownCodeException("Unknown CompressionCodec wire code: " + code);
        };
    }
}
