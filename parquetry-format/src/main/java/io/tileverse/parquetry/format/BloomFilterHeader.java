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
package io.tileverse.parquetry.format;

/**
 * Header that precedes a column chunk's bloom-filter bitset; mirror of {@code BloomFilterHeader} in
 * {@code parquet.thrift}.
 *
 * <p>Located at {@link ColumnMetaData#bloomFilterOffset()} within the file. The header is Thrift-compact-encoded; the
 * bitset follows immediately after it and spans exactly {@link #numBytes()} bytes.
 *
 * <p>The three trailing fields ({@link Algorithm}, {@link HashStrategy}, {@link Compression}) each mirror a Thrift
 * union of empty structs. Every defined case carries no payload, so parquetry surfaces them as nested enums.
 *
 * @param numBytes size of the bitset in bytes (always a multiple of 32 in a valid Parquet file)
 * @param algorithm bit-setting algorithm; currently only {@link Algorithm#SPLIT_BLOCK} is defined
 * @param hash hash function applied to plain-encoded values; currently only {@link HashStrategy#XXHASH}
 * @param compression compression applied to the bitset; currently only {@link Compression#UNCOMPRESSED}
 */
public record BloomFilterHeader(int numBytes, Algorithm algorithm, HashStrategy hash, Compression compression) {

    /**
     * The algorithm used to set bits in a Parquet bloom filter. Mirror of the {@code BloomFilterAlgorithm} Thrift union
     * (a single-case union of empty structs at time of writing).
     *
     * <p>Each case carries its Thrift compact-protocol {@link #fieldId() field id} within the union struct.
     * Deserializers resolve incoming ids via {@link #valueOf(int)}, which fails fast on unknown variants so a future
     * spec extension can't be silently misinterpreted as {@link #SPLIT_BLOCK}.
     */
    public enum Algorithm {
        /** Block-based bloom filter (a.k.a. SBBF). The only algorithm defined by the Parquet format. */
        SPLIT_BLOCK(1);

        private final int fieldId;

        Algorithm(int fieldId) {
            this.fieldId = fieldId;
        }

        /** Thrift compact-protocol field id of this case within the {@code BloomFilterAlgorithm} union. */
        public int fieldId() {
            return fieldId;
        }

        /** @throws UnknownCodeException if no defined variant carries that field id */
        public static Algorithm valueOf(int fieldId) {
            return switch (fieldId) {
                case 1 -> SPLIT_BLOCK;
                default -> throw new UnknownCodeException("Unknown BloomFilterAlgorithm field id: " + fieldId);
            };
        }
    }

    /**
     * The hash function used by a Parquet bloom filter. Mirror of the {@code BloomFilterHash} Thrift union (a
     * single-case union of empty structs at time of writing).
     *
     * <p>See {@link Algorithm} for the wire-id rationale.
     */
    public enum HashStrategy {
        /** xxHash64 with seed 0, applied to the plain-encoded bytes of the column value. */
        XXHASH(1);

        private final int fieldId;

        HashStrategy(int fieldId) {
            this.fieldId = fieldId;
        }

        /** Thrift compact-protocol field id of this case within the {@code BloomFilterHash} union. */
        public int fieldId() {
            return fieldId;
        }

        /** @throws UnknownCodeException if no defined variant carries that field id */
        public static HashStrategy valueOf(int fieldId) {
            return switch (fieldId) {
                case 1 -> XXHASH;
                default -> throw new UnknownCodeException("Unknown BloomFilterHash field id: " + fieldId);
            };
        }
    }

    /**
     * Compression applied to a Parquet bloom filter's bitset. Mirror of the {@code BloomFilterCompression} Thrift union
     * (a single-case union of empty structs at time of writing).
     *
     * <p>See {@link Algorithm} for the wire-id rationale.
     */
    public enum Compression {
        /** No compression; the bitset bytes follow the header verbatim. */
        UNCOMPRESSED(1);

        private final int fieldId;

        Compression(int fieldId) {
            this.fieldId = fieldId;
        }

        /** Thrift compact-protocol field id of this case within the {@code BloomFilterCompression} union. */
        public int fieldId() {
            return fieldId;
        }

        /** @throws UnknownCodeException if no defined variant carries that field id */
        public static Compression valueOf(int fieldId) {
            return switch (fieldId) {
                case 1 -> UNCOMPRESSED;
                default -> throw new UnknownCodeException("Unknown BloomFilterCompression field id: " + fieldId);
            };
        }
    }
}
