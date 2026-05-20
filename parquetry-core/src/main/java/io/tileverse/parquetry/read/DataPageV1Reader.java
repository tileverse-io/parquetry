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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.read.LevelMaximaResolver.LevelMaxima;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;

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
 * <p>Pooling contract: one pooled buffer is borrowed for the full decompressed payload. The returned
 * {@link DecodedPage}'s {@code valueBuffer} owns that single pool ticket; the rep- and def-level slices are zero-copy
 * read-only views into the same pooled buffer. Closing the {@link DecodedPage} returns the ticket and invalidates all
 * three views together. On any failure path the borrowed buffer is returned to the pool before the exception
 * propagates.
 */
final class DataPageV1Reader implements DataPageReader {

    @Override
    public DecodedPage read(
            PageHeader header,
            LevelMaxima maxLevels,
            ByteBuffer compressedPagePayload,
            Codec codec,
            ByteBufferPool pool)
            throws IOException {
        DataPageHeader v1 = header.dataPageHeader()
                .orElseThrow(() -> new IllegalArgumentException("PageHeader does not carry a DataPageHeader"));
        PooledByteBuffer pooled = decompressPayload(header, compressedPagePayload, codec, pool);
        try {
            ByteBuffer decompressed = pooled.buffer();
            decompressed.order(ByteOrder.LITTLE_ENDIAN);
            ByteBuffer repLevels = readOptionalLevelSlice(decompressed, maxLevels.maxRepetitionLevel(), "repetition");
            ByteBuffer defLevels = readOptionalLevelSlice(decompressed, maxLevels.maxDefinitionLevel(), "definition");
            // After the slices consume their length-prefixed regions, the pooled buffer's position sits at the start
            // of the values section. DecodedPage.values() returns that buffer, so callers see exactly the value bytes.
            Encoding valuesEncoding = DataPageReader.normalizeEncoding(v1.encoding());
            return new DecodedPage(v1.numValues(), valuesEncoding, repLevels, defLevels, pooled);
        } catch (RuntimeException e) {
            pooled.close();
            throw e;
        }
    }

    /**
     * Decompresses the full page payload into a pooled buffer sized for {@link PageHeader#uncompressedPageSize()}. On
     * any failure the borrowed buffer is returned to the pool before the exception leaves this method.
     */
    private static PooledByteBuffer decompressPayload(
            PageHeader header, ByteBuffer compressedPagePayload, Codec codec, ByteBufferPool pool) throws IOException {
        int uncompressedSize = header.uncompressedPageSize();
        if (uncompressedSize < 0) {
            throw new MalformedFileException(
                    "V1 page uncompressedPageSize must be non-negative, got " + uncompressedSize);
        }
        PooledByteBuffer pooled = pool.borrowDirect(uncompressedSize);
        try {
            ByteBuffer target = pooled.buffer();
            target.clear();
            target.limit(uncompressedSize);
            ByteBuffer source = compressedPagePayload.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            codec.decompress(source, target);
            target.flip();
            return pooled;
        } catch (IOException | RuntimeException e) {
            pooled.close();
            throw e;
        }
    }

    /**
     * Reads the next {@code [int32 LE length][bytes...]} pair when the column has a non-zero max level, returning an
     * empty read-only buffer otherwise. The returned slice is a zero-copy view into {@code decompressed}.
     */
    private static ByteBuffer readOptionalLevelSlice(ByteBuffer decompressed, int maxLevel, String levelKind) {
        if (maxLevel == 0) {
            return emptyLittleEndianSlice();
        }
        int prefix = readLengthPrefix(decompressed, levelKind);
        return sliceAndAdvance(decompressed, prefix, levelKind);
    }

    private static int readLengthPrefix(ByteBuffer decompressed, String levelKind) {
        if (decompressed.remaining() < Integer.BYTES) {
            throw new MalformedFileException("V1 page truncated before " + levelKind
                    + " level length prefix: remaining=" + decompressed.remaining() + " bytes, need 4");
        }
        int length = decompressed.getInt();
        if (length < 0) {
            throw new MalformedFileException("V1 page " + levelKind + " level length prefix is negative: " + length);
        }
        return length;
    }

    /**
     * Slices off the next {@code length} bytes of {@code decompressed} as a read-only little-endian view and advances
     * the cursor past them. Throws when fewer than {@code length} bytes remain.
     */
    private static ByteBuffer sliceAndAdvance(ByteBuffer decompressed, int length, String levelKind) {
        if (length > decompressed.remaining()) {
            throw new MalformedFileException("V1 page " + levelKind + " level length " + length
                    + " exceeds remaining payload bytes " + decompressed.remaining());
        }
        if (length == 0) {
            return emptyLittleEndianSlice();
        }
        ByteBuffer slice = decompressed.slice();
        slice.limit(length);
        slice.order(ByteOrder.LITTLE_ENDIAN);
        decompressed.position(decompressed.position() + length);
        return slice.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
    }

    private static ByteBuffer emptyLittleEndianSlice() {
        return ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN).asReadOnlyBuffer();
    }
}
