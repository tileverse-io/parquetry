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
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.codec.CodecRegistry;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.page.DecoderFactory;
import io.tileverse.parquetry.page.Dictionary;
import io.tileverse.parquetry.page.LevelDecoder;
import io.tileverse.parquetry.page.PageDecoder;
import io.tileverse.parquetry.read.LevelMaximaResolver.LevelMaxima;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;

import io.tileverse.io.ByteBufferPool;

/**
 * Lazy, page-at-a-time {@link ColumnReader} backed by a {@code FetchedColumnChunk}'s compressed buffer.
 *
 * <p>The reader walks the compressed column chunk one page at a time: read the next {@link PageHeader}, slice off the
 * page's compressed bytes, hand them to a {@link DataPageReader} that produces a {@link DecodedPage}, build the
 * per-page {@link LevelDecoder}s and {@link PageDecoder}, and yield values to the assembler row by row. When the page
 * exhausts, the previous {@link DecodedPage} is closed (returning its pooled decompressed-values buffer to the pool)
 * before the next page is loaded. This is the streaming memory contract described in the package documentation: per
 * active row group, resident bytes is bounded by {@code Sigma_columns (compressed chunk bytes) + Sigma_columns (one
 * decompressed page worth)}.
 *
 * <p>Dictionary pages are <em>not</em> walked by this reader; the {@code RealColumnFetcher} decodes the dictionary
 * eagerly during the column-chunk fetch and skips past it when computing the start position of the compressed buffer
 * slice handed in here. The constructor therefore receives only the data-page region.
 *
 * <p>The wrapped {@code FetchedColumnChunk}'s compressed buffer is owned by the {@code RowGroupReader}; this reader
 * neither retains nor closes it. The only resource the reader owns is the current page's {@link DecodedPage}, released
 * either when advancing to the next page or by {@link #close()}.
 */
final class StreamingColumnReader implements ColumnReader {

    private final ColumnPath columnPath;
    private final int maxRepetitionLevel;
    private final int maxDefinitionLevel;
    private final LevelMaxima maxLevels;
    private final Field.Primitive leaf;
    private final Codec codec;
    private final long totalValues;
    private final ByteBufferPool pool;
    private final Optional<Dictionary<?>> dictionary;
    private final PageCursor pageCursor;
    private final int repBitWidth;
    private final int defBitWidth;

    private DecodedPage currentPage;
    private LevelDecoder repetitionDecoder;
    private LevelDecoder definitionDecoder;
    private PageDecoder<?> valueDecoder;
    private int valuesConsumedInCurrentPage;
    private int valuesInCurrentPage;
    private long valuesConsumedTotal;

    private boolean positioned;
    private int currentRepLevel;
    private int currentDefLevel;
    private Object currentValue;
    private boolean closed;

    @SuppressWarnings("java:S107") // 9 params is intentional: ColumnReader is the consumer-facing
    // type and we want zero hidden state; bundling into a config record would obscure the
    // construction-site contract that paired RealColumnFetcher already enforces.
    public StreamingColumnReader(
            ColumnPath columnPath,
            int maxRepetitionLevel,
            int maxDefinitionLevel,
            Field.Primitive leaf,
            ByteBuffer compressedChunkBytes,
            Optional<Dictionary<?>> dictionary,
            CompressionCodec compressionCodec,
            long totalValues,
            ByteBufferPool pool) {
        this.columnPath = Objects.requireNonNull(columnPath, "columnPath");
        if (maxRepetitionLevel < 0) {
            throw new IllegalArgumentException("maxRepetitionLevel must be >= 0, got " + maxRepetitionLevel);
        }
        if (maxDefinitionLevel < 0) {
            throw new IllegalArgumentException("maxDefinitionLevel must be >= 0, got " + maxDefinitionLevel);
        }
        if (totalValues < 0) {
            throw new IllegalArgumentException("totalValues must be >= 0, got " + totalValues);
        }
        this.maxRepetitionLevel = maxRepetitionLevel;
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.maxLevels = new LevelMaxima(maxRepetitionLevel, maxDefinitionLevel);
        this.leaf = Objects.requireNonNull(leaf, "leaf");
        this.dictionary = Objects.requireNonNull(dictionary, "dictionary");
        this.codec = CodecRegistry.lookup(Objects.requireNonNull(compressionCodec, "compressionCodec"));
        this.totalValues = totalValues;
        this.pool = Objects.requireNonNull(pool, "pool");
        this.pageCursor =
                new PageCursor(Objects.requireNonNull(compressedChunkBytes, "compressedChunkBytes"), columnPath);
        this.repBitWidth = LevelDecoder.computeBitWidth(maxRepetitionLevel);
        this.defBitWidth = LevelDecoder.computeBitWidth(maxDefinitionLevel);
    }

    @Override
    public boolean hasNext() {
        return valuesConsumedTotal < totalValues;
    }

    @Override
    public int currentRepetitionLevel() {
        positionIfNeeded();
        return currentRepLevel;
    }

    @Override
    public int currentDefinitionLevel() {
        positionIfNeeded();
        return currentDefLevel;
    }

    @Override
    public Object currentValue() {
        positionIfNeeded();
        return currentValue;
    }

    @Override
    public void consume() {
        positionIfNeeded();
        positioned = false;
        valuesConsumedInCurrentPage++;
        valuesConsumedTotal++;
    }

    @Override
    public ColumnPath columnPath() {
        return columnPath;
    }

    @Override
    public int maxRepetitionLevel() {
        return maxRepetitionLevel;
    }

    @Override
    public int maxDefinitionLevel() {
        return maxDefinitionLevel;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        releaseCurrentPage();
    }

    // --- positioning ---

    private void positionIfNeeded() {
        if (!positioned) {
            positionNext();
        }
    }

    private void positionNext() {
        if (valuesConsumedTotal >= totalValues) {
            throw new IllegalStateException(
                    "No more rows to read in column " + columnPath.dot() + " (totalValues=" + totalValues + ")");
        }
        if (currentPage == null || valuesConsumedInCurrentPage >= valuesInCurrentPage) {
            advanceToNextPage();
        }
        currentRepLevel = readRepetitionLevel();
        currentDefLevel = readDefinitionLevel();
        currentValue = (currentDefLevel == maxDefinitionLevel) ? valueDecoder.next() : null;
        positioned = true;
    }

    private int readRepetitionLevel() {
        return (maxRepetitionLevel == 0) ? 0 : repetitionDecoder.next();
    }

    private int readDefinitionLevel() {
        return (maxDefinitionLevel == 0) ? maxDefinitionLevel : definitionDecoder.next();
    }

    // --- page advance ---

    /**
     * Releases the current page (if any) and decodes the next data page from the chunk's compressed bytes. Skips any
     * intervening non-data pages (e.g. an unexpected dictionary page in the data-page region); if the cursor exhausts
     * without yielding a data page while {@code valuesConsumedTotal &lt; totalValues}, that is a format error and a
     * {@link ParquetFormatException} is thrown.
     */
    private void advanceToNextPage() {
        releaseCurrentPage();
        DecodedPage next = pageCursor.nextDataPage(maxLevels, codec, pool);
        if (next == null) {
            throw new MalformedFileException("Column " + columnPath.dot() + " exhausted page stream after "
                    + valuesConsumedTotal + " values; expected " + totalValues);
        }
        try {
            repetitionDecoder = buildLevelDecoder(repBitWidth, next.repLevelBytes());
            definitionDecoder = buildLevelDecoder(defBitWidth, next.defLevelBytes());
            valueDecoder = DecoderFactory.decoderFor(next.valuesEncoding(), leaf.kind(), typeLength(leaf), dictionary);
            valueDecoder.load(next.values(), next.valueCount());
        } catch (RuntimeException e) {
            next.close();
            throw e;
        }
        currentPage = next;
        valuesInCurrentPage = next.valueCount();
        valuesConsumedInCurrentPage = 0;
    }

    private void releaseCurrentPage() {
        if (currentPage != null) {
            try {
                currentPage.close();
            } finally {
                currentPage = null;
                repetitionDecoder = null;
                definitionDecoder = null;
                valueDecoder = null;
                valuesInCurrentPage = 0;
                valuesConsumedInCurrentPage = 0;
            }
        }
    }

    private static LevelDecoder buildLevelDecoder(int bitWidth, ByteBuffer bytes) {
        LevelDecoder decoder = new LevelDecoder(bitWidth);
        decoder.load(bytes.duplicate());
        return decoder;
    }

    private static OptionalInt typeLength(Field.Primitive leaf) {
        return leaf.typeLength();
    }

    // --- page cursor (compressed chunk walker) ---

    /**
     * Walks the chunk's compressed byte buffer, yielding one decompressed {@link DecodedPage} per call. The cursor
     * tracks position inside an underlying {@link ByteBuffer} (duplicated from the caller's buffer so the original
     * position is never mutated) and dispatches header reads through {@link Format#readPageHeader(InputStream)}.
     */
    private static final class PageCursor {

        private final ByteBuffer chunk;
        private final ColumnPath columnPath;

        PageCursor(ByteBuffer chunk, ColumnPath columnPath) {
            this.chunk = chunk.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            this.columnPath = columnPath;
        }

        /**
         * Returns the next {@link DecodedPage} from the chunk's compressed bytes, or {@code null} when the cursor is
         * exhausted. Non-data pages (e.g. a misplaced dictionary or index page in the data-page region) are skipped.
         */
        DecodedPage nextDataPage(LevelMaxima maxLevels, Codec codec, ByteBufferPool pool) {
            while (chunk.hasRemaining()) {
                PageHeader header = readNextPageHeader();
                int compressedSize = header.compressedPageSize();
                if (compressedSize < 0) {
                    throw new MalformedFileException(
                            "Negative compressedPageSize " + compressedSize + " for column " + columnPath.dot());
                }
                ByteBuffer pagePayload = sliceAndAdvance(compressedSize);
                if (header.type() == PageType.DATA_PAGE || header.type() == PageType.DATA_PAGE_V2) {
                    try {
                        return DataPageReader.forHeader(header).read(header, maxLevels, pagePayload, codec, pool);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to decode data page for column " + columnPath.dot(), e);
                    }
                }
                // Skip any leftover dictionary/index pages quietly; their payload bytes were already advanced past.
            }
            return null;
        }

        /**
         * Reads the next {@link PageHeader} from the cursor by wrapping the remaining buffer as an {@link InputStream}
         * and advancing the buffer's position by exactly the number of header bytes consumed. Throws
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
         * Returns a read-only zero-copy slice of the next {@code length} bytes of the cursor and advances past them.
         * Throws {@link ParquetFormatException} when the cursor does not have that many bytes left.
         */
        private ByteBuffer sliceAndAdvance(int length) {
            if (chunk.remaining() < length) {
                throw new MalformedFileException("Column " + columnPath.dot() + " page payload of " + length
                        + " bytes overruns chunk (remaining=" + chunk.remaining() + ")");
            }
            ByteBuffer slice = chunk.slice();
            slice.limit(length);
            slice.order(ByteOrder.LITTLE_ENDIAN);
            chunk.position(chunk.position() + length);
            return slice.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
        }
    }

    /**
     * Minimal {@link InputStream} adapter over a {@link ByteBuffer}, used to feed
     * {@link Format#readPageHeader(InputStream)} from either a heap-backed or direct compressed chunk buffer without
     * allocating an intermediate {@code byte[]}. The adapter advances the underlying buffer's position as it reads, so
     * the caller can resume slicing from the buffer right after the header consumes its bytes.
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
