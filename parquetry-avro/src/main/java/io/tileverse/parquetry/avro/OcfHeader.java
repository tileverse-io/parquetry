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
package io.tileverse.parquetry.avro;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The parsed OCF header: the metadata map, the parsed write schema, the block codec, the 16-byte sync marker, and the
 * file offset of the first data block. Binary metadata values are kept as read-only {@link MemorySegment}.
 */
// Internal record; instances are never compared by equals/hashCode or used as keys, and syncMarker is a small
// fixed-size byte[] marker, hence the default array-identity equals/hashCode is harmless.
@SuppressWarnings("java:S6218")
record OcfHeader(
        Map<String, MemorySegment> metadata,
        AvroSchema schema,
        BlockCodec codec,
        byte[] syncMarker,
        long firstBlockOffset) {

    private static final byte[] MAGIC = {'O', 'b', 'j', 1};
    private static final int SYNC_SIZE = 16;

    static OcfHeader read(ByteRangeCursor cursor) {
        verifyMagic(cursor);
        Map<String, MemorySegment> metadata = readMetadata(cursor);
        byte[] syncMarker = cursor.readSegment(SYNC_SIZE).toArray(ValueLayout.JAVA_BYTE);
        long firstBlockOffset = cursor.position();

        AvroSchema schema = AvroSchemaParser.parse(stringValue(metadata, "avro.schema"));
        BlockCodec codec = BlockCodec.fromName(optionalStringValue(metadata, "avro.codec"));
        return new OcfHeader(metadata, schema, codec, syncMarker, firstBlockOffset);
    }

    private static void verifyMagic(ByteRangeCursor cursor) {
        for (byte expected : MAGIC) {
            if ((byte) cursor.readRawByte() != expected) {
                throw new AvroFormatException("Not an Avro OCF file: bad magic");
            }
        }
    }

    private static Map<String, MemorySegment> readMetadata(ByteRangeCursor cursor) {
        Map<String, MemorySegment> metadata = new LinkedHashMap<>();
        for (long count = blockCount(cursor); count != 0; count = blockCount(cursor)) {
            for (long i = 0; i < count; i++) {
                String key = readString(cursor);
                MemorySegment value = readBytes(cursor);
                metadata.put(key, value);
            }
        }
        return metadata;
    }

    private static long blockCount(ByteRangeCursor cursor) {
        long count = cursor.readLong();
        if (count < 0) {
            cursor.readLong();
            if (count == Long.MIN_VALUE) {
                // negating Long.MIN_VALUE yields itself; the still-negative count would misparse the
                // remaining header bytes as metadata entries
                throw new AvroFormatException("Block count " + count + " is not representable");
            }
            return -count;
        }
        return count;
    }

    private static String readString(ByteRangeCursor cursor) {
        return new String(readRaw(cursor), StandardCharsets.UTF_8);
    }

    private static MemorySegment readBytes(ByteRangeCursor cursor) {
        long length = cursor.readLong();
        return cursor.readSegment(requireValidLength(length)).asReadOnly();
    }

    private static byte[] readRaw(ByteRangeCursor cursor) {
        long length = cursor.readLong();
        return cursor.readSegment(requireValidLength(length)).toArray(ValueLayout.JAVA_BYTE);
    }

    private static int requireValidLength(long length) {
        if (length < 0 || length > Integer.MAX_VALUE) {
            throw new AvroFormatException("OCF field length out of range: " + length);
        }
        return (int) length;
    }

    private static String stringValue(Map<String, MemorySegment> metadata, String key) {
        MemorySegment value = metadata.get(key);
        if (value == null) {
            throw new AvroFormatException("Missing required OCF metadata key: " + key);
        }
        return new String(value.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    private static String optionalStringValue(Map<String, MemorySegment> metadata, String key) {
        MemorySegment value = metadata.get(key);
        return value == null ? null : new String(value.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }
}
