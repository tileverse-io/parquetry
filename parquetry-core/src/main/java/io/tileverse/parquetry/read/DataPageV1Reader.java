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
package io.tileverse.parquetry.read;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.read.LevelMaximaResolver.LevelMaxima;

/**
 * Reads a Parquet 1.x Data Page V1.
 *
 * <p>V1 layout (after decompression, per the Parquet spec's Encodings doc):
 *
 * <pre>
 *   [int32 LE repetition_levels_byte_length][repetition_levels bytes]   // omitted when maxRepLevel == 0
 *   [int32 LE definition_levels_byte_length][definition_levels bytes]   // omitted when maxDefLevel == 0
 *   [value bytes]
 * </pre>
 *
 * Unlike V2, V1 compresses the entire payload as one blob and embeds the level-byte lengths inside it. The page header
 * itself only carries the total compressed and uncompressed sizes, so the reader needs the column's max repetition and
 * definition levels (passed in via {@link LevelMaxima}) to know whether each length prefix is present.
 *
 * <p>Arena contract: the full decompressed payload is allocated from the caller-supplied {@link Arena}. The rep- and
 * def-level segments are read-only slices of that allocation; the values segment begins after the level sections. All
 * three segments share the Arena's lifetime. The caller (not this reader) is responsible for closing the Arena.
 */
final class DataPageV1Reader implements DataPageReader {

    @Override
    public DecodedPage read(
            PageHeader header, LevelMaxima maxLevels, ByteBuffer compressedPagePayload, Codec codec, Arena pageArena)
            throws IOException {
        DataPageHeader v1 = header.dataPageHeader()
                .orElseThrow(() -> new IllegalArgumentException("PageHeader does not carry a DataPageHeader"));
        MemorySegment payload = decompressPayload(header, compressedPagePayload, codec, pageArena);
        // Walk the decompressed payload with a ByteBuffer cursor for the length-prefix reads.
        ByteBuffer cursor = payload.asByteBuffer().order(LITTLE_ENDIAN);
        MemorySegment repLevels = readOptionalLevelSlice(payload, cursor, maxLevels.maxRepetitionLevel(), "repetition");
        MemorySegment defLevels = readOptionalLevelSlice(payload, cursor, maxLevels.maxDefinitionLevel(), "definition");
        // After the level slices are consumed, the cursor position is the start of the values section.
        MemorySegment valueBytes = payload.asSlice(cursor.position()).asReadOnly();
        Encoding valuesEncoding = DataPageReader.normalizeEncoding(v1.encoding());
        return new DecodedPage(v1.numValues(), valuesEncoding, repLevels, defLevels, valueBytes, pageArena);
    }

    /**
     * Decompresses the full page payload into an Arena-allocated segment sized for
     * {@link PageHeader#uncompressedPageSize()}.
     */
    private static MemorySegment decompressPayload(
            PageHeader header, ByteBuffer compressedPagePayload, Codec codec, Arena pageArena) throws IOException {
        int uncompressedSize = header.uncompressedPageSize();
        if (uncompressedSize < 0) {
            throw new MalformedFileException(
                    "V1 page uncompressedPageSize must be non-negative, got " + uncompressedSize);
        }
        MemorySegment payload = pageArena.allocate(uncompressedSize);
        MemorySegment compressedSource = MemorySegment.ofBuffer(compressedPagePayload.duplicate());
        codec.decompress(compressedSource, payload);
        return payload;
    }

    /**
     * Reads the next {@code [int32 LE length][bytes...]} pair when the column has a non-zero max level, returning
     * {@link MemorySegment#NULL} otherwise. The returned slice is a read-only view into {@code payload}.
     */
    private static MemorySegment readOptionalLevelSlice(
            MemorySegment payload, ByteBuffer cursor, int maxLevel, String levelKind) {
        if (maxLevel == 0) {
            return MemorySegment.NULL;
        }
        int prefix = readLengthPrefix(cursor, levelKind);
        return sliceAndAdvance(payload, cursor, prefix, levelKind);
    }

    private static int readLengthPrefix(ByteBuffer cursor, String levelKind) {
        if (cursor.remaining() < Integer.BYTES) {
            throw new MalformedFileException("V1 page truncated before " + levelKind
                    + " level length prefix: remaining=" + cursor.remaining() + " bytes, need 4");
        }
        int length = cursor.getInt();
        if (length < 0) {
            throw new MalformedFileException("V1 page " + levelKind + " level length prefix is negative: " + length);
        }
        return length;
    }

    /**
     * Returns a read-only slice of the next {@code length} bytes of {@code payload} starting at the cursor's current
     * position, then advances the cursor past them. Returns {@link MemorySegment#NULL} when {@code length == 0}.
     */
    private static MemorySegment sliceAndAdvance(
            MemorySegment payload, ByteBuffer cursor, int length, String levelKind) {
        if (length > cursor.remaining()) {
            throw new MalformedFileException("V1 page " + levelKind + " level length " + length
                    + " exceeds remaining payload bytes " + cursor.remaining());
        }
        if (length == 0) {
            return MemorySegment.NULL;
        }
        int offset = cursor.position();
        MemorySegment slice = payload.asSlice(offset, length).asReadOnly();
        cursor.position(offset + length);
        return slice;
    }
}
