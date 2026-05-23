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

import io.tileverse.parquetry.format.BloomFilterHeader;

/**
 * Serializer mirror of {@link BloomFilterHeaderDeserializer}. The three union fields each carry their selected case as
 * a single-field nested struct containing an empty payload struct.
 */
final class BloomFilterHeaderSerializer {

    private BloomFilterHeaderSerializer() {}

    static void serialize(CompactProtocolWriter w, BloomFilterHeader header) throws IOException {
        w.writeStructBegin();
        w.writeI32Field((short) 1, header.numBytes());
        writeUnionVariant(w, (short) 2, (short) header.algorithm().fieldId());
        writeUnionVariant(w, (short) 3, (short) header.hash().fieldId());
        writeUnionVariant(w, (short) 4, (short) header.compression().fieldId());
        w.writeFieldStop();
        w.writeStructEnd();
    }

    /**
     * Writes one of the three single-case unions ({@code BloomFilterAlgorithm}, {@code BloomFilterHash},
     * {@code BloomFilterCompression}). The outer struct carries one struct field at {@code variantId}, whose body is an
     * empty struct (the case's payload type).
     */
    private static void writeUnionVariant(CompactProtocolWriter w, short fieldId, short variantId) throws IOException {
        w.writeFieldBegin(fieldId, CompactType.STRUCT);
        w.writeStructBegin();
        w.writeFieldBegin(variantId, CompactType.STRUCT);
        // Empty case payload struct
        w.writeStructBegin();
        w.writeFieldStop();
        w.writeStructEnd();
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
