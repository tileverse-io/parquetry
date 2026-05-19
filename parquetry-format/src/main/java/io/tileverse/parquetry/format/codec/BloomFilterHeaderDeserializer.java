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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;

import io.tileverse.parquetry.format.BloomFilterAlgorithm;
import io.tileverse.parquetry.format.BloomFilterCompression;
import io.tileverse.parquetry.format.BloomFilterHashStrategy;
import io.tileverse.parquetry.format.BloomFilterHeader;
import io.tileverse.parquetry.format.ParquetFormatException;

/**
 * Deserializer for the Thrift {@code BloomFilterHeader} struct.
 *
 * <pre>
 * struct BloomFilterHeader {
 *   1: required i32 numBytes;
 *   2: required BloomFilterAlgorithm algorithm;
 *   3: required BloomFilterHash hash;
 *   4: required BloomFilterCompression compression;
 * }
 * </pre>
 *
 * Each of the three trailing unions has exactly one defined variant today; this deserializer returns the matching enum
 * value and rejects unknown variants so a future Parquet version that adds new algorithms can't be silently
 * misinterpreted.
 */
final class BloomFilterHeaderDeserializer {

    private BloomFilterHeaderDeserializer() {}

    static BloomFilterHeader read(CompactProtocolReader r) throws IOException {
        int numBytes = -1;
        BloomFilterAlgorithm algorithm = null;
        BloomFilterHashStrategy hash = null;
        BloomFilterCompression compression = null;
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) break;
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> numBytes = r.readI32();
                case 2 -> algorithm = readAlgorithm(r);
                case 3 -> hash = readHash(r);
                case 4 -> compression = readCompression(r);
                default -> r.skipField(fh.type());
            }
        }
        return new BloomFilterHeader(
                requireNumBytes(numBytes),
                requireField(algorithm, "algorithm"),
                requireField(hash, "hash"),
                requireField(compression, "compression"));
    }

    /** Validates {@code numBytes} (i32 field 1). Required and must be non-negative per the Parquet spec. */
    private static int requireNumBytes(int numBytes) {
        if (numBytes < 0) {
            throw new ParquetFormatException("BloomFilterHeader missing required field numBytes");
        }
        return numBytes;
    }

    /** Validates that the union-typed required field {@code fieldName} was present in the encoded header. */
    private static <T> T requireField(T value, String fieldName) {
        if (value == null) {
            throw new ParquetFormatException("BloomFilterHeader missing required field " + fieldName);
        }
        return value;
    }

    /**
     * Reads the one-of-N {@code BloomFilterAlgorithm} union. Field 1 is the only defined variant ({@code BLOCK} a.k.a.
     * split-block bloom filter); any other variant fails fast to keep readers honest about new algorithms.
     */
    private static BloomFilterAlgorithm readAlgorithm(CompactProtocolReader r) throws IOException {
        int variantId = readSingleUnionVariantId(r);
        if (variantId == 1) {
            return BloomFilterAlgorithm.SPLIT_BLOCK;
        }
        throw new ParquetFormatException("Unknown BloomFilterAlgorithm variant id: " + variantId);
    }

    /**
     * Reads the one-of-N {@code BloomFilterHash} union. Field 1 is the only defined variant ({@code XXHASH}); any other
     * variant fails fast.
     */
    private static BloomFilterHashStrategy readHash(CompactProtocolReader r) throws IOException {
        int variantId = readSingleUnionVariantId(r);
        if (variantId == 1) {
            return BloomFilterHashStrategy.XXHASH;
        }
        throw new ParquetFormatException("Unknown BloomFilterHash variant id: " + variantId);
    }

    /**
     * Reads the one-of-N {@code BloomFilterCompression} union. Field 1 is the only defined variant
     * ({@code UNCOMPRESSED}); any other variant fails fast - a compressed bitset would need codec wiring before it can
     * be evaluated.
     */
    private static BloomFilterCompression readCompression(CompactProtocolReader r) throws IOException {
        int variantId = readSingleUnionVariantId(r);
        if (variantId == 1) {
            return BloomFilterCompression.UNCOMPRESSED;
        }
        throw new ParquetFormatException("Unknown BloomFilterCompression variant id: " + variantId);
    }

    /**
     * Reads a Thrift union that has exactly one field set (each variant being an empty struct). Returns the variant's
     * field id. Skips the empty-struct payload and any trailing fields a forward-compatible writer may have added.
     */
    private static int readSingleUnionVariantId(CompactProtocolReader r) throws IOException {
        FieldHeader fh = r.readFieldHeader(0);
        if (fh.isStop()) {
            throw new ParquetFormatException("Bloom-filter union struct has no variant set");
        }
        int variantId = fh.fieldId();
        r.skipStruct();
        // Consume the trailing STOP of the union struct, tolerating forward-compat extra fields.
        FieldHeader stop = r.readFieldHeader(fh.fieldId());
        while (!stop.isStop()) {
            r.skipField(stop.type());
            stop = r.readFieldHeader(stop.fieldId());
        }
        return variantId;
    }
}
