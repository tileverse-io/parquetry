/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.internal.read.page;

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.schema.LevelMaxima;

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
public final class DataPageV1Reader implements DataPageReader {

    @Override
    public DecodedPage read(
            PageHeader header,
            LevelMaxima maxLevels,
            MemorySegment compressedPagePayload,
            Compression codec,
            Arena pageArena)
            throws IOException {
        DataPageHeader v1 = header.dataPageHeader()
                .orElseThrow(() -> new MalformedFileException("PageHeader does not carry a DataPageHeader"));
        MemorySegment payload = decompressPayload(header, compressedPagePayload, codec, pageArena);
        // Walk the decompressed payload with a byte offset for the length-prefix reads.
        LevelSlice rep = readOptionalLevelSlice(payload, 0L, maxLevels.maxRepetitionLevel(), "repetition");
        LevelSlice def =
                readOptionalLevelSlice(payload, rep.nextOffset(), maxLevels.maxDefinitionLevel(), "definition");
        // After the level slices are consumed, the offset is the start of the values section.
        MemorySegment valueBytes = payload.asSlice(def.nextOffset()).asReadOnly();
        Encoding valuesEncoding = DataPageReader.normalizeEncoding(v1.encoding());
        return new DecodedPage(v1.numValues(), valuesEncoding, rep.slice(), def.slice(), valueBytes, pageArena);
    }

    /** A level-bytes slice paired with the payload offset immediately after it. */
    private record LevelSlice(MemorySegment slice, long nextOffset) {}

    /**
     * Decompresses the full page payload into an Arena-allocated segment sized for
     * {@link PageHeader#uncompressedPageSize()}.
     */
    private static MemorySegment decompressPayload(
            PageHeader header, MemorySegment compressedPagePayload, Compression codec, Arena pageArena)
            throws IOException {
        int uncompressedSize = header.uncompressedPageSize();
        if (uncompressedSize < 0) {
            throw new MalformedFileException(
                    "V1 page uncompressedPageSize must be non-negative, got " + uncompressedSize);
        }
        MemorySegment payload = pageArena.allocate(uncompressedSize);
        codec.decompress(compressedPagePayload, payload);
        return payload;
    }

    /**
     * Reads the next {@code [int32 LE length][bytes...]} pair when the column has a non-zero max level, returning a
     * {@link MemorySegment#NULL} slice (and an unchanged offset) otherwise. The returned slice is a read-only view into
     * {@code payload}.
     */
    private static LevelSlice readOptionalLevelSlice(MemorySegment payload, long offset, int maxLevel, String kind) {
        if (maxLevel == 0) {
            return new LevelSlice(MemorySegment.NULL, offset);
        }
        int prefix = readLengthPrefix(payload, offset, kind);
        return sliceAndAdvance(payload, offset + Integer.BYTES, prefix, kind);
    }

    private static int readLengthPrefix(MemorySegment payload, long offset, String kind) {
        if (payload.byteSize() - offset < Integer.BYTES) {
            throw new MalformedFileException("V1 page truncated before " + kind + " level length prefix: remaining="
                    + (payload.byteSize() - offset) + " bytes, need 4");
        }
        int length = payload.get(INT32, offset);
        if (length < 0) {
            throw new MalformedFileException("V1 page " + kind + " level length prefix is negative: " + length);
        }
        return length;
    }

    /**
     * Returns a read-only slice of {@code length} bytes of {@code payload} starting at {@code offset}, paired with the
     * offset immediately after it. Uses a {@link MemorySegment#NULL} slice when {@code length == 0}.
     */
    private static LevelSlice sliceAndAdvance(MemorySegment payload, long offset, int length, String kind) {
        if (length > payload.byteSize() - offset) {
            throw new MalformedFileException("V1 page " + kind + " level length " + length
                    + " exceeds remaining payload bytes " + (payload.byteSize() - offset));
        }
        if (length == 0) {
            return new LevelSlice(MemorySegment.NULL, offset);
        }
        MemorySegment slice = payload.asSlice(offset, length).asReadOnly();
        return new LevelSlice(slice, offset + length);
    }
}
