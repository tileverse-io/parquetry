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
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;

import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.read.LevelMaximaResolver.LevelMaxima;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Walks the chunk's compressed byte buffer, yielding one decompressed {@link DecodedPage} per call.
 *
 * <p>The cursor tracks position inside an underlying {@link ByteBuffer} (duplicated from the caller's buffer so the
 * original position is never mutated) and dispatches header reads through {@link ParquetFormat#readPageHeader}.
 * Non-data pages (e.g. a misplaced dictionary or index page) are skipped silently; {@link #nextDataPage} returns
 * {@code null} when the buffer is exhausted.
 *
 * <p>Extracted from the private inner class of {@link StreamingColumnReader} so that {@link BatchColumnReader} can
 * share the same page-fetching loop without duplicating the logic.
 */
final class PageCursor {

    private final ByteBuffer chunk;
    private final ColumnPath columnPath;

    PageCursor(ByteBuffer chunk, ColumnPath columnPath) {
        this.chunk = chunk.duplicate().order(LITTLE_ENDIAN);
        this.columnPath = columnPath;
    }

    /**
     * Returns the next {@link DecodedPage} from the chunk's compressed bytes, or {@code null} when the cursor is
     * exhausted. Non-data pages (e.g. a misplaced dictionary or index page in the data-page region) are skipped.
     */
    DecodedPage nextDataPage(LevelMaxima maxLevels, Codec codec, Arena pageArena) throws IOException {
        while (chunk.hasRemaining()) {
            PageHeader header = readNextPageHeader();
            int compressedSize = header.compressedPageSize();
            if (compressedSize < 0) {
                throw new MalformedFileException(
                        "Negative compressedPageSize " + compressedSize + " for column " + columnPath.dot());
            }
            ByteBuffer pagePayload = sliceAndAdvance(compressedSize);
            if (header.type() == PageType.DATA_PAGE || header.type() == PageType.DATA_PAGE_V2) {
                return DataPageReader.forHeader(header).read(header, maxLevels, pagePayload, codec, pageArena);
            }
            // Skip any leftover dictionary/index pages quietly; their payload bytes were already advanced past.
        }
        return null;
    }

    /**
     * Returns {@code true} while the underlying buffer has bytes remaining to decode. A {@code true} result does not
     * guarantee that the next call to {@link #nextDataPage} yields a data page; there may only be non-data pages left.
     */
    boolean hasRemaining() {
        return chunk.hasRemaining();
    }

    /**
     * Reads the next {@link PageHeader} from the cursor by wrapping the remaining buffer as an {@link InputStream} and
     * advancing the buffer's position by exactly the number of header bytes consumed. Throws
     * {@link ParquetFormatException} on premature end-of-stream or Thrift decode failure.
     */
    private PageHeader readNextPageHeader() {
        ByteBufferInputStream stream = new ByteBufferInputStream(chunk);
        int startPos = chunk.position();
        try {
            PageHeader header = ParquetFormat.readPageHeader(stream);
            int consumed = chunk.position() - startPos;
            if (consumed <= 0) {
                throw new MalformedFileException("Page header read advanced the cursor by " + consumed
                        + " bytes for column " + columnPath.dot());
            }
            return header;
        } catch (ParquetFormatException e) {
            throw e.withContext("Failed to read page header for column " + columnPath.dot(), -1L, "PageHeader");
        }
    }

    /**
     * Returns a read-only zero-copy slice of the next {@code length} bytes of the cursor and advances past them. Throws
     * {@link ParquetFormatException} when the cursor does not have that many bytes left.
     */
    private ByteBuffer sliceAndAdvance(int length) {
        if (chunk.remaining() < length) {
            throw new MalformedFileException("Column " + columnPath.dot() + " page payload of " + length
                    + " bytes overruns chunk (remaining=" + chunk.remaining() + ")");
        }
        ByteBuffer slice = chunk.slice();
        slice.limit(length);
        slice.order(LITTLE_ENDIAN);
        chunk.position(chunk.position() + length);
        return slice.asReadOnlyBuffer().order(LITTLE_ENDIAN);
    }

    /**
     * Minimal {@link InputStream} adapter over a {@link ByteBuffer}, used to feed
     * {@link ParquetFormat#readPageHeader(InputStream)} from either a heap-backed or direct compressed chunk buffer
     * without allocating an intermediate {@code byte[]}. The adapter advances the underlying buffer's position as it
     * reads, so the caller can resume slicing from the buffer right after the header consumes its bytes.
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
