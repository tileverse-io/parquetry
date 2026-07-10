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
package io.tileverse.parquetry.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import org.roaringbitmap.RoaringBitmap;

import io.tileverse.parquetry.filter.RowPositionSet;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.iceberg.IcebergManifests.DeleteFileRef;
import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Reads an Iceberg v3 deletion vector: a roaring bitmap of deleted absolute row positions stored in a Puffin blob,
 * decoded into the same {@link RowPositionSet} a positional delete file produces. A deletion vector deletes from the
 * single data file the manifest names; the delete planner applies it by referenced-path and sequence the same way it
 * applies a positional delete file.
 *
 * <p>The decode is clean-room from the Iceberg deletion-vector-v1 blob format, depending only on the 32-bit
 * {@link RoaringBitmap} class. The blob bytes at {@code [contentOffset, contentOffset + contentSizeInBytes)} are framed
 * as:
 *
 * <ul>
 *   <li>a 4-byte big-endian length of the magic bytes plus bitmap ({@code contentSizeInBytes - 8});
 *   <li>a 4-byte little-endian {@link #MAGIC_NUMBER};
 *   <li>the portable roaring position bitmap, little-endian (an 8-byte bitmap count, then per 32-bit bitmap a 4-byte
 *       unsigned key and a standard 32-bit roaring serialization);
 *   <li>a 4-byte big-endian CRC-32 of the magic bytes plus bitmap.
 * </ul>
 *
 * Each 64-bit position is reconstructed as {@code (key << 32) | (value & 0xFFFFFFFFL)}.
 */
final class IcebergDeletionVectors {

    private static final int MAGIC_NUMBER = 1681511377;
    private static final int LENGTH_SIZE_BYTES = 4;
    private static final int MAGIC_SIZE_BYTES = 4;
    private static final int CRC_SIZE_BYTES = 4;

    private IcebergDeletionVectors() {}

    /** Reads {@code deletionVector}'s Puffin blob through {@code io} and decodes it to its deleted row positions. */
    static RowPositionSet read(IcebergFileIO io, DeleteFileRef deletionVector) {
        MemorySegment blob = readBlob(io, deletionVector);
        return decode(blob, deletionVector.recordCount());
    }

    private static MemorySegment readBlob(IcebergFileIO io, DeleteFileRef deletionVector) {
        long offset = requirePresent(deletionVector.contentOffset(), "content_offset", deletionVector);
        long size = requirePresent(deletionVector.contentSizeInBytes(), "content_size_in_bytes", deletionVector);
        if (size < LENGTH_SIZE_BYTES + MAGIC_SIZE_BYTES + CRC_SIZE_BYTES) {
            throw new IcebergFormatException("deletion-vector blob is too small: " + size + " bytes");
        }
        if (size > Integer.MAX_VALUE) {
            throw new IcebergFormatException("deletion-vector blob is too large: " + size + " bytes");
        }
        MemorySegment blob = MemorySegment.ofArray(new byte[(int) size]);
        try (ByteRangeSource source = io.open(deletionVector.location())) {
            source.readFully(offset, blob);
        }
        return blob;
    }

    private static long requirePresent(Long value, String field, DeleteFileRef deletionVector) {
        if (value == null) {
            throw new IcebergFormatException("deletion vector has no " + field + ": " + deletionVector.location());
        }
        return value;
    }

    /**
     * Decodes one framed deletion-vector blob into its positions, validating the magic number, the framed length, the
     * CRC-32, and the bitmap cardinality the manifest records.
     */
    private static RowPositionSet decode(MemorySegment blob, long expectedCardinality) {
        int bitmapDataLength = readFramedLength(blob);
        validateMagic(blob);
        validateChecksum(blob, bitmapDataLength);
        long[] positions = readPositions(bitmapData(blob, bitmapDataLength));
        validateCardinality(positions.length, expectedCardinality);
        return SortedLongPositionSet.of(positions);
    }

    /** The big-endian length of the magic bytes plus bitmap, which must equal the blob minus its length and CRC. */
    private static int readFramedLength(MemorySegment blob) {
        int length = blob.get(ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN), 0);
        long expected = blob.byteSize() - LENGTH_SIZE_BYTES - CRC_SIZE_BYTES;
        if (length != expected) {
            throw new IcebergFormatException(
                    "deletion-vector framed length " + length + " does not match blob length " + expected);
        }
        return length;
    }

    private static void validateMagic(MemorySegment blob) {
        int magic = blob.get(ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN), LENGTH_SIZE_BYTES);
        if (magic != MAGIC_NUMBER) {
            throw new IcebergFormatException("deletion-vector magic " + magic + " does not match " + MAGIC_NUMBER);
        }
    }

    /** The CRC-32 covers the magic bytes plus bitmap; the stored CRC is the trailing big-endian int. */
    private static void validateChecksum(MemorySegment blob, int bitmapDataLength) {
        CRC32 crc = new CRC32();
        crc.update(blob.asSlice(LENGTH_SIZE_BYTES, bitmapDataLength).asByteBuffer());
        int computed = (int) crc.getValue();
        int crcOffset = LENGTH_SIZE_BYTES + bitmapDataLength;
        int stored = blob.get(ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN), crcOffset);
        if (computed != stored) {
            throw new IcebergFormatException(
                    "deletion-vector CRC mismatch: computed " + computed + ", stored " + stored);
        }
    }

    /** The little-endian roaring view starting just past the magic number, used to deserialize each 32-bit bitmap. */
    private static ByteBuffer bitmapData(MemorySegment blob, int bitmapDataLength) {
        int bitmapOffset = LENGTH_SIZE_BYTES + MAGIC_SIZE_BYTES;
        int bitmapLength = bitmapDataLength - MAGIC_SIZE_BYTES;
        ByteBuffer roaring = blob.asSlice(bitmapOffset, bitmapLength).asByteBuffer();
        roaring.order(ByteOrder.LITTLE_ENDIAN);
        return roaring;
    }

    /**
     * Reads the portable roaring position bitmap into absolute positions. The leading 8-byte count gives the number of
     * 32-bit bitmaps; each is preceded by its unsigned high key, and every 32-bit value combines with that key into a
     * 64-bit position. The decoded bitmaps are sized into a single {@code long[]} from their own cardinalities, never
     * from the unvalidated manifest record count.
     */
    private static long[] readPositions(ByteBuffer roaring) {
        long bitmapCount = roaring.getLong();
        if (bitmapCount < 0 || bitmapCount > Integer.MAX_VALUE) {
            throw new IcebergFormatException("invalid deletion-vector bitmap count: " + bitmapCount);
        }
        List<KeyedBitmap> bitmaps = new ArrayList<>((int) bitmapCount);
        for (long i = 0; i < bitmapCount; i++) {
            long key = readKey(roaring);
            bitmaps.add(new KeyedBitmap(key, deserializeBitmap(roaring)));
        }
        return toPositions(bitmaps);
    }

    /** One 32-bit roaring bitmap with the unsigned high key the low 32-bit values combine with. */
    private record KeyedBitmap(long key, RoaringBitmap bitmap) {}

    private static long[] toPositions(List<KeyedBitmap> bitmaps) {
        long[] positions = new long[totalCardinality(bitmaps)];
        Cursor cursor = new Cursor(positions);
        for (KeyedBitmap keyed : bitmaps) {
            appendBitmapPositions(keyed, cursor);
        }
        return positions;
    }

    private static int totalCardinality(List<KeyedBitmap> bitmaps) {
        long total = 0L;
        for (KeyedBitmap keyed : bitmaps) {
            total += Integer.toUnsignedLong(keyed.bitmap().getCardinality());
        }
        if (total > Integer.MAX_VALUE) {
            throw new IcebergFormatException("too many deleted positions in one deletion vector: " + total);
        }
        return (int) total;
    }

    private static long readKey(ByteBuffer roaring) {
        return Integer.toUnsignedLong(roaring.getInt());
    }

    /**
     * The high key never sets bit 31: a deletion vector addresses one data file, whose row positions stay below 2^31
     * (well below 2^63), and the portable roaring keys run in ascending order from key 0. The {@code (key << 32)} shift
     * therefore never reaches the sign bit of the assembled 64-bit position.
     */
    private static void appendBitmapPositions(KeyedBitmap keyed, Cursor cursor) {
        long high = keyed.key() << 32;
        keyed.bitmap().forEach((int value) -> cursor.add(high | Integer.toUnsignedLong(value)));
    }

    /** A write cursor over the positions array; the roaring callback appends in ascending order. */
    private static final class Cursor {
        private final long[] positions;
        private int next;

        Cursor(long[] positions) {
            this.positions = positions;
        }

        void add(long position) {
            positions[next++] = position;
        }
    }

    /**
     * Deserializes one standard 32-bit roaring bitmap from {@code roaring}, advancing the buffer past it. The roaring
     * library's {@code deserialize(ByteBuffer)} does not move the buffer position, hence the explicit advance.
     */
    private static RoaringBitmap deserializeBitmap(ByteBuffer roaring) {
        try {
            RoaringBitmap bitmap = new RoaringBitmap();
            bitmap.deserialize(roaring);
            roaring.position(roaring.position() + bitmap.serializedSizeInBytes());
            return bitmap;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void validateCardinality(int decoded, long expected) {
        if (decoded != expected) {
            throw new IcebergFormatException(
                    "deletion-vector cardinality " + decoded + " does not match the recorded " + expected);
        }
    }
}
