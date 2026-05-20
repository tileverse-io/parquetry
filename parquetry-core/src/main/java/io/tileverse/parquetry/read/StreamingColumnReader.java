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
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.codec.CodecRegistry;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.page.DecoderFactory;
import io.tileverse.parquetry.page.Dictionary;
import io.tileverse.parquetry.page.LevelDecoder;
import io.tileverse.parquetry.page.PageDecoder;
import io.tileverse.parquetry.read.LevelMaximaResolver.LevelMaxima;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;

/**
 * Lazy, page-at-a-time {@link ColumnReader} backed by a {@code FetchedColumnChunk}'s compressed buffer.
 *
 * <p>The reader walks the compressed column chunk one page at a time: read the next {@link PageHeader}, slice off the
 * page's compressed bytes, hand them to a {@link DataPageReader} that produces a {@link DecodedPage}, build the
 * per-page {@link LevelDecoder}s and {@link PageDecoder}, and yield values to the assembler row by row. When the page
 * exhausts, the previous {@link DecodedPage} is closed (closing its {@link Arena} and releasing the native memory for
 * that page's decompressed bytes) before the next page is loaded. This is the streaming memory contract documented in
 * the package documentation: per active row group, resident bytes is bounded by {@code Sigma_columns (compressed chunk
 * bytes) + Sigma_columns (one decompressed page worth)}.
 *
 * <p>Dictionary pages are <em>not</em> walked by this reader; the {@code RealColumnFetcher} decodes the dictionary
 * eagerly during the column-chunk fetch and skips past it when computing the start position of the compressed buffer
 * slice handed in here. The constructor therefore receives only the data-page region.
 *
 * <p>The wrapped {@code FetchedColumnChunk}'s compressed buffer is owned by the {@code RowGroupReader}; this reader
 * neither retains nor closes it. The reader owns one {@link Arena} per active page, allocated freshly with
 * {@link Arena#ofConfined()} when advancing. Closing the previous page's {@link DecodedPage} closes that Arena,
 * releasing native memory before the next Arena is opened.
 */
final class StreamingColumnReader implements ColumnReader {

    private final ColumnPath columnPath;
    private final int maxRepetitionLevel;
    private final int maxDefinitionLevel;
    private final LevelMaxima maxLevels;
    private final Field.Primitive leaf;
    private final Codec codec;
    private final long totalValues;
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

    @SuppressWarnings("java:S107") // 8 params is intentional: ColumnReader is the consumer-facing
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
            long totalValues) {
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
     *
     * <p>One {@link Arena#ofConfined()} is allocated per page. The Arena is passed to the page reader and is owned by
     * the resulting {@link DecodedPage}; closing the page closes the Arena.
     *
     * <p>Transition note: the value decoder receives a heap-backed {@link ByteBuffer} copy of the Arena-allocated value
     * bytes. Binary decoders (e.g. {@code PlainBinaryDecoder}) return {@link MemorySegment} views of their buffer that
     * are stored in assembled rows. In the row API, those rows outlive the page Arena (they are collected into a list
     * while the stream is still open, then accessed after the stream closes). A heap copy ensures the decoder's buffer
     * - and any views derived from it - remain valid after the Arena is closed. Once the row API reads directly from
     * materialized vectors this copy will no longer be needed.
     */
    private void advanceToNextPage() {
        releaseCurrentPage();
        Arena arena = Arena.ofConfined();
        DecodedPage next;
        try {
            next = pageCursor.nextDataPage(maxLevels, codec, arena);
        } catch (RuntimeException | IOException e) {
            arena.close();
            throw (e instanceof RuntimeException re) ? re : new UncheckedIOException((IOException) e);
        }
        if (next == null) {
            arena.close();
            throw new MalformedFileException("Column " + columnPath.dot() + " exhausted page stream after "
                    + valuesConsumedTotal + " values; expected " + totalValues);
        }
        try {
            repetitionDecoder = buildLevelDecoder(repBitWidth, next.repLevelBytes());
            definitionDecoder = buildLevelDecoder(defBitWidth, next.defLevelBytes());
            valueDecoder = DecoderFactory.decoderFor(next.valuesEncoding(), leaf.kind(), typeLength(leaf), dictionary);
            valueDecoder.load(heapCopyOf(next.valueBytes()), next.valueCount());
        } catch (RuntimeException e) {
            next.close();
            throw e;
        }
        currentPage = next;
        valuesInCurrentPage = next.valueCount();
        valuesConsumedInCurrentPage = 0;
    }

    /**
     * Copies the contents of an Arena-allocated {@link MemorySegment} into a fresh heap {@link ByteBuffer} in
     * little-endian order. The returned buffer is positioned at zero and ready to read. Heap backing ensures any
     * {@link MemorySegment} slices produced by a decoder from this buffer remain valid after the Arena closes.
     * Transition artifact while the row API still calls next(); will be removed once the row API reads directly from
     * materialized vectors.
     */
    private static ByteBuffer heapCopyOf(MemorySegment segment) {
        int size = (int) segment.byteSize();
        ByteBuffer heap = ByteBuffer.allocate(size).order(LITTLE_ENDIAN);
        MemorySegment.copy(segment, 0, MemorySegment.ofBuffer(heap), 0, size);
        heap.rewind();
        return heap;
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

    /**
     * Builds a level decoder for the given bit width and level bytes. When {@code bytes} is {@link MemorySegment#NULL}
     * (column with max level == 0), an empty ByteBuffer is passed; the decoder's {@link LevelDecoder#next()} returns 0
     * without reading from the buffer when bit width is 0.
     */
    private static LevelDecoder buildLevelDecoder(int bitWidth, MemorySegment bytes) {
        LevelDecoder decoder = new LevelDecoder(bitWidth);
        ByteBuffer buf = (bytes == MemorySegment.NULL)
                ? ByteBuffer.allocate(0).order(LITTLE_ENDIAN)
                : bytes.asByteBuffer().order(LITTLE_ENDIAN);
        decoder.load(buf);
        return decoder;
    }

    private static OptionalInt typeLength(Field.Primitive leaf) {
        return leaf.typeLength();
    }
}
