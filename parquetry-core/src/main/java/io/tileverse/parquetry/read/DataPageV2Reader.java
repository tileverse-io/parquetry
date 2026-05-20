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
import java.nio.ByteBuffer;

import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.read.LevelMaximaResolver.LevelMaxima;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;

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
 * <p>Pooling contract: the rep- and def-level slices are zero-copy views into the caller's compressed buffer; the
 * decompressed value buffer is borrowed from {@link ByteBufferPool}. On the failure path the borrowed buffer is
 * returned to the pool before the exception propagates.
 *
 * <p>The {@code maxLevels} parameter is unused: V2 page headers stamp the level byte lengths explicitly. It is kept on
 * the interface so {@link DataPageV1Reader}, which depends on the column's max levels to know whether each length
 * prefix is present, can share the {@link DataPageReader#read} signature.
 */
final class DataPageV2Reader implements DataPageReader {

    @Override
    public DecodedPage read(
            PageHeader header,
            LevelMaxima maxLevels,
            ByteBuffer compressedPagePayload,
            Codec codec,
            ByteBufferPool pool)
            throws IOException {
        DataPageHeaderV2 v2 = header.dataPageHeaderV2()
                .orElseThrow(() -> new IllegalArgumentException("PageHeader does not carry a DataPageHeaderV2"));

        int repLen = v2.repetitionLevelsByteLength();
        int defLen = v2.definitionLevelsByteLength();
        if (repLen < 0 || defLen < 0) {
            throw new MalformedFileException(
                    "V2 level byte lengths must be non-negative: repLen=" + repLen + ", defLen=" + defLen);
        }
        int valuesUncompressedSize = computeValuesUncompressedSize(header, repLen, defLen);

        ByteBuffer cursor = compressedPagePayload.duplicate().order(LITTLE_ENDIAN);
        ByteBuffer repLevels = sliceAndAdvance(cursor, repLen);
        ByteBuffer defLevels = sliceAndAdvance(cursor, defLen);

        Encoding valuesEncoding = DataPageReader.normalizeEncoding(v2.encoding());
        boolean compressed = v2.isCompressed();
        PooledByteBuffer valuesPooled = decodeValues(cursor, codec, pool, valuesUncompressedSize, compressed);
        return new DecodedPage(v2.numValues(), valuesEncoding, repLevels, defLevels, valuesPooled);
    }

    /**
     * Decompresses (or copies, when {@code !compressed}) the remaining bytes of {@code cursor} into a freshly borrowed
     * pooled buffer of size {@code uncompressedSize}. On any failure the borrowed buffer is returned to the pool before
     * the exception leaves this method, preserving the the package documentation lifecycle invariant.
     */
    private static PooledByteBuffer decodeValues(
            ByteBuffer cursor, Codec codec, ByteBufferPool pool, int uncompressedSize, boolean compressed)
            throws IOException {
        PooledByteBuffer pooled = pool.borrowDirect(uncompressedSize);
        try {
            ByteBuffer target = pooled.buffer();
            target.clear();
            target.limit(uncompressedSize);
            if (compressed) {
                codec.decompress(cursor, target);
            } else {
                copyBytes(cursor, target, uncompressedSize);
            }
            target.flip();
            return pooled;
        } catch (IOException | RuntimeException e) {
            pooled.close();
            throw e;
        }
    }

    /**
     * Copies exactly {@code length} bytes from {@code src} into {@code dst} when the V2 header marks the page values as
     * uncompressed. Avoids the codec round-trip for the {@code is_compressed=false} path.
     */
    private static void copyBytes(ByteBuffer src, ByteBuffer dst, int length) {
        ByteBuffer slice = src.slice();
        slice.limit(length);
        dst.put(slice);
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
     * Returns a read-only zero-copy slice of the next {@code len} bytes of {@code cursor} and advances the cursor past
     * them. Returns an empty read-only buffer when {@code len == 0} so callers always receive a non-null buffer with
     * little-endian byte order.
     */
    private static ByteBuffer sliceAndAdvance(ByteBuffer cursor, int len) {
        if (len == 0) {
            return ByteBuffer.allocate(0).order(LITTLE_ENDIAN).asReadOnlyBuffer();
        }
        ByteBuffer slice = cursor.slice();
        slice.limit(len);
        slice.order(LITTLE_ENDIAN);
        cursor.position(cursor.position() + len);
        return slice.asReadOnlyBuffer().order(LITTLE_ENDIAN);
    }
}
