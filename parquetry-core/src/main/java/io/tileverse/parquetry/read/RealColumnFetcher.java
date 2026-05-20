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
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.codec.CodecRegistry;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.DictionaryPageHeader;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.page.Dictionary;
import io.tileverse.parquetry.page.DictionaryDecoder;
import io.tileverse.parquetry.read.LevelMaximaResolver.LevelMaxima;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;

/**
 * Production {@link ColumnFetcher} backed by a {@link RangeReader} and a {@link ByteBufferPool}.
 *
 * <p>For each fetch:
 *
 * <ol>
 *   <li>Resolve the byte range covering the full compressed column chunk (dictionary page if present + all data pages).
 *   <li>Borrow a pooled direct buffer sized to the chunk's {@code totalCompressedSize} and read the range into it.
 *   <li>If the column chunk has a dictionary page (metadata's {@code dictionaryPageOffset} is set), decode it eagerly
 *       into a {@link Dictionary} via {@link DictionaryDecoder} and advance the buffer's position past the dictionary
 *       page so the buffer handed back to the column reader starts at the first data page.
 *   <li>Resolve max repetition / definition levels for the leaf via {@link LevelMaximaResolver}.
 *   <li>Return a {@link FetchedColumnChunk} that owns the pooled buffer.
 * </ol>
 *
 * <p>The dictionary is decoded into heap memory (it is bounded by the writer's row-group dictionary size cap), so the
 * scratch pooled buffer used for its decompression is released before the fetch returns. The big buffer kept inside
 * {@code FetchedColumnChunk} is the still-compressed chunk; data pages are decoded lazily by the column reader.
 */
final class RealColumnFetcher implements ColumnFetcher {

    private final RangeReader rangeReader;
    private final ParquetSchema fileSchema;
    private final ByteBufferPool pool;

    public RealColumnFetcher(RangeReader rangeReader, ParquetSchema fileSchema, ByteBufferPool pool) {
        this.rangeReader = Objects.requireNonNull(rangeReader, "rangeReader");
        this.fileSchema = Objects.requireNonNull(fileSchema, "fileSchema");
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public FetchedColumnChunk fetch(ColumnChunk chunk, ColumnPath path) throws IOException {
        Objects.requireNonNull(chunk, "chunk");
        Objects.requireNonNull(path, "path");
        ColumnMetaData meta = chunk.metaData()
                .orElseThrow(() ->
                        new ParquetFormatException("ColumnChunk for " + path.dot() + " is missing inline metaData"));

        long chunkStart = computeChunkStart(meta);
        long chunkSize = meta.totalCompressedSize();
        if (chunkSize < 0 || chunkSize > Integer.MAX_VALUE) {
            throw new MalformedFileException(
                    "Column chunk for " + path.dot() + " has an unsupported totalCompressedSize " + chunkSize);
        }
        int chunkLen = (int) chunkSize;

        PooledByteBuffer pooled = pool.borrowDirect(chunkLen);
        try {
            ByteBuffer chunkBuffer = pooled.buffer();
            chunkBuffer.clear();
            chunkBuffer.limit(chunkLen);
            rangeReader.readRange(chunkStart, chunkLen, chunkBuffer);
            chunkBuffer.flip();
            chunkBuffer.order(LITTLE_ENDIAN);

            Optional<Dictionary<?>> dictionary = maybeDecodeDictionary(chunkBuffer, meta, path);
            LevelMaxima maxima = LevelMaximaResolver.resolve(fileSchema, path);
            return new FetchedColumnChunk(
                    path, meta, maxima.maxRepetitionLevel(), maxima.maxDefinitionLevel(), pooled, dictionary);
        } catch (IOException | RuntimeException e) {
            pooled.close();
            throw e;
        }
    }

    /**
     * Returns the file offset of the column chunk's first page: the dictionary page when present, otherwise the first
     * data page.
     */
    private static long computeChunkStart(ColumnMetaData meta) {
        return meta.dictionaryPageOffset().orElse(meta.dataPageOffset());
    }

    /**
     * Reads and decodes the dictionary page at the front of {@code chunkBuffer} when {@code meta.dictionaryPageOffset}
     * is set, advancing {@code chunkBuffer.position()} past the dictionary page's bytes so subsequent slices start at
     * the first data page. Returns {@link Optional#empty()} when no dictionary page is declared.
     *
     * <p>The dictionary is decompressed into a temporary pooled buffer that is closed before the method returns; the
     * resulting {@link Dictionary} holds heap-resident Java values referenced from the {@link FetchedColumnChunk}.
     */
    private Optional<Dictionary<?>> maybeDecodeDictionary(ByteBuffer chunkBuffer, ColumnMetaData meta, ColumnPath path)
            throws IOException {
        if (meta.dictionaryPageOffset().isEmpty()) {
            return Optional.empty();
        }
        int headerStartPosition = chunkBuffer.position();
        PageHeader header = readPageHeader(chunkBuffer, path);
        DictionaryPageHeader dictHeader = header.dictionaryPageHeader()
                .orElseThrow(() -> new ParquetFormatException("Column " + path.dot()
                        + " declares dictionaryPageOffset but its first page is not a dictionary page (type="
                        + header.type() + ")"));
        if (header.type() != PageType.DICTIONARY_PAGE) {
            throw new MalformedFileException(
                    "Column " + path.dot() + " dictionary page header has unexpected type " + header.type());
        }
        int compressedSize = header.compressedPageSize();
        int uncompressedSize = header.uncompressedPageSize();
        if (chunkBuffer.remaining() < compressedSize) {
            throw new MalformedFileException("Column " + path.dot()
                    + " dictionary page compressed payload (" + compressedSize
                    + ") overruns the chunk buffer (remaining=" + chunkBuffer.remaining() + ")");
        }
        ByteBuffer compressedPayload = sliceAndAdvance(chunkBuffer, compressedSize);
        Dictionary<?> dictionary = decodeDictionary(meta, path, dictHeader, compressedPayload, uncompressedSize);
        // Guard against a malformed header that claims zero bytes consumed; if that ever happened it would loop later.
        if (chunkBuffer.position() == headerStartPosition) {
            throw new MalformedFileException("Column " + path.dot() + " dictionary page advanced zero bytes");
        }
        return Optional.of(dictionary);
    }

    /**
     * Decompresses the dictionary page payload into a scratch pooled buffer and dispatches to {@link DictionaryDecoder}
     * using the column's {@link PrimitiveKind} and optional {@code typeLength}. The scratch buffer is released before
     * this method returns.
     */
    private Dictionary<?> decodeDictionary(
            ColumnMetaData meta,
            ColumnPath path,
            DictionaryPageHeader dictHeader,
            ByteBuffer compressedPayload,
            int uncompressedSize)
            throws IOException {
        Codec codec = CodecRegistry.lookup(meta.codec());
        PrimitiveKind kind = primitiveKindOf(meta.type(), path);
        OptionalInt typeLength = OptionalInt.empty();
        try (PooledByteBuffer scratch = pool.borrowHeap(uncompressedSize)) {
            ByteBuffer dst = scratch.buffer();
            dst.clear();
            dst.limit(uncompressedSize);
            codec.decompress(MemorySegment.ofBuffer(compressedPayload), MemorySegment.ofBuffer(dst));
            dst.rewind();
            return DictionaryDecoder.read(dst, kind, dictHeader.numValues(), typeLength);
        }
    }

    private static PrimitiveKind primitiveKindOf(PhysicalType type, ColumnPath path) {
        return switch (type) {
            case BOOLEAN -> PrimitiveKind.BOOLEAN;
            case INT32 -> PrimitiveKind.INT32;
            case INT64 -> PrimitiveKind.INT64;
            case INT96 -> PrimitiveKind.INT96;
            case FLOAT -> PrimitiveKind.FLOAT;
            case DOUBLE -> PrimitiveKind.DOUBLE;
            case BYTE_ARRAY -> PrimitiveKind.BYTE_ARRAY;
            // FIXED_LEN_BYTE_ARRAY dictionaries are uncommon but legal; if we ever hit one we need typeLength from
            // the schema. Throw a clear error rather than silently mis-decode, since the dictionary cache only ever
            // sees the bytes from the page header's typeLength.
            case FIXED_LEN_BYTE_ARRAY ->
                throw new UnsupportedOperationException(
                        "FIXED_LEN_BYTE_ARRAY dictionary decoding not yet supported (column "
                                + path.dot()
                                + "); typeLength must be sourced from the schema leaf Field.Primitive");
        };
    }

    /**
     * Reads a {@link PageHeader} from {@code buffer}, advancing the buffer's position past the header bytes consumed.
     * Wraps the buffer in a tiny adapter rather than copying to a {@code byte[]}, so direct buffers stay direct.
     */
    private static PageHeader readPageHeader(ByteBuffer buffer, ColumnPath path) {
        try {
            return ParquetFormat.readPageHeader(new ByteBufferInputStream(buffer));
        } catch (ParquetFormatException e) {
            throw e.withContext("Failed to read dictionary page header for column " + path.dot(), -1L, "PageHeader");
        }
    }

    /**
     * Returns a read-only zero-copy slice of the next {@code length} bytes of {@code buffer} and advances the buffer
     * past them.
     */
    private static ByteBuffer sliceAndAdvance(ByteBuffer buffer, int length) {
        ByteBuffer slice = buffer.slice();
        slice.limit(length);
        slice.order(LITTLE_ENDIAN);
        buffer.position(buffer.position() + length);
        return slice.asReadOnlyBuffer().order(LITTLE_ENDIAN);
    }

    /**
     * Minimal {@link InputStream} adapter over a {@link ByteBuffer} so the Thrift compact protocol reader can pull
     * bytes without an intermediate {@code byte[]} copy. Advances the underlying buffer's position as bytes are read.
     */
    private static final class ByteBufferInputStream extends InputStream {

        private final ByteBuffer buffer;

        ByteBufferInputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            return buffer.get() & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            int toRead = Math.min(len, buffer.remaining());
            buffer.get(b, off, toRead);
            return toRead;
        }

        @Override
        public int available() {
            return buffer.remaining();
        }
    }
}
