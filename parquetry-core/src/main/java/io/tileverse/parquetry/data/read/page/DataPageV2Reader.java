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
package io.tileverse.parquetry.data.read.page;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.schema.LevelMaxima;

/**
 * Reads a Parquet 2.0 Data Page V2.
 *
 * <p>V2 layout (per the Parquet specification's Encodings doc and {@code parquet.thrift}):
 *
 * <pre>
 *   [repetition_levels_byte_length bytes]   // uncompressed RLE-bit-packed rep levels
 *   [definition_levels_byte_length bytes]   // uncompressed RLE-bit-packed def levels
 *   [values bytes]                          // compressed iff is_compressed is true
 * </pre>
 *
 * The level bytes are always uncompressed, which lets page skipping read just the levels without paying for
 * decompression. The values portion uses the codec named by {@link io.tileverse.parquetry.format.ColumnMetaData#codec};
 * the {@code is_compressed} flag (default {@code true} per the Thrift schema) controls whether to invoke the codec or
 * copy the bytes verbatim.
 *
 * <p>Arena contract: the decompressed value bytes are allocated from the caller-supplied {@link Arena}. The rep- and
 * def-level segments are read-only slices of the caller's compressed buffer (zero-copy), but they are wrapped as
 * read-only {@link MemorySegment} views. {@link MemorySegment#NULL} is used for absent level regions (when the
 * corresponding byte length from the header is zero). The caller owns the Arena's lifecycle.
 *
 * <p>The {@code maxLevels} parameter is unused: V2 page headers stamp the level byte lengths explicitly. It is kept on
 * the interface so {@link DataPageV1Reader}, which depends on the column's max levels to know whether each length
 * prefix is present, can share the {@link DataPageReader#read} signature.
 */
public final class DataPageV2Reader implements DataPageReader {

    @Override
    public DecodedPage read(
            PageHeader header,
            LevelMaxima maxLevels,
            ByteBuffer compressedPagePayload,
            Compression codec,
            Arena pageArena)
            throws IOException {
        DataPageHeaderV2 v2 = header.dataPageHeaderV2()
                .orElseThrow(() -> new MalformedFileException("PageHeader does not carry a DataPageHeaderV2"));

        int repLen = v2.repetitionLevelsByteLength();
        int defLen = v2.definitionLevelsByteLength();
        if (repLen < 0 || defLen < 0) {
            throw new MalformedFileException(
                    "V2 level byte lengths must be non-negative: repLen=" + repLen + ", defLen=" + defLen);
        }
        int valuesUncompressedSize = computeValuesUncompressedSize(header, repLen, defLen);

        ByteBuffer cursor = compressedPagePayload.duplicate().order(LITTLE_ENDIAN);
        MemorySegment repLevels = sliceAndAdvance(cursor, repLen);
        MemorySegment defLevels = sliceAndAdvance(cursor, defLen);

        Encoding valuesEncoding = DataPageReader.normalizeEncoding(v2.encoding());
        boolean compressed = v2.isCompressed();
        MemorySegment valueBytes = decodeValues(cursor, codec, pageArena, valuesUncompressedSize, compressed);
        return new DecodedPage(v2.numValues(), valuesEncoding, repLevels, defLevels, valueBytes, pageArena);
    }

    /**
     * Decompresses (or copies, when {@code !compressed}) the remaining bytes of {@code cursor} into an Arena-allocated
     * segment of size {@code uncompressedSize}.
     */
    private static MemorySegment decodeValues(
            ByteBuffer cursor, Compression codec, Arena pageArena, int uncompressedSize, boolean compressed)
            throws IOException {
        MemorySegment valueBytes = pageArena.allocate(uncompressedSize);
        if (compressed) {
            MemorySegment compressedSource = MemorySegment.ofBuffer(cursor.duplicate());
            codec.decompress(compressedSource, valueBytes);
        } else {
            copyBytes(cursor, valueBytes, uncompressedSize);
        }
        return valueBytes;
    }

    /**
     * Copies exactly {@code length} bytes from {@code src} into {@code dst} when the V2 header marks the page values as
     * uncompressed. Avoids the codec round-trip for the {@code is_compressed=false} path.
     */
    private static void copyBytes(ByteBuffer src, MemorySegment dst, int length) {
        ByteBuffer slice = src.slice().limit(length).asReadOnlyBuffer();
        dst.copyFrom(MemorySegment.ofBuffer(slice));
        src.position(src.position() + length);
    }

    /**
     * Returns the uncompressed-bytes size of the values section, derived from the page header's
     * {@code uncompressedPageSize} minus the (uncompressed) level prefixes.
     */
    private static int computeValuesUncompressedSize(PageHeader header, int repLen, int defLen) {
        int total = header.uncompressedPageSize();
        int values = total - repLen - defLen;
        if (values < 0) {
            throw new MalformedFileException("V2 page has level byte lengths (" + repLen + " + " + defLen
                    + ") larger than uncompressedPageSize (" + total + ")");
        }
        return values;
    }

    /**
     * Returns a read-only zero-copy {@link MemorySegment} view of the next {@code len} bytes of {@code cursor} and
     * advances the cursor past them. Returns {@link MemorySegment#NULL} when {@code len == 0}.
     */
    private static MemorySegment sliceAndAdvance(ByteBuffer cursor, int len) {
        if (len == 0) {
            return MemorySegment.NULL;
        }
        ByteBuffer slice = cursor.slice().limit(len).order(LITTLE_ENDIAN).asReadOnlyBuffer();
        cursor.position(cursor.position() + len);
        return MemorySegment.ofBuffer(slice);
    }
}
