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
package io.tileverse.parquetry.format;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.format.codec.ParquetFormatDeserializer;
import io.tileverse.parquetry.format.codec.ParquetFormatSerializer;

/**
 * Reads and writes Parquet file metadata and page-level Thrift structures.
 *
 * <p>The read side ({@code readXxx}) pulls bytes off a {@link RangeReader} or {@link InputStream}.
 * {@link #readFooter(RangeReader)} locates the {@code FileMetaData} Thrift struct at the end of a Parquet file and
 * decodes it. The page-level read methods decode the corresponding structures at offsets the caller has obtained from
 * the footer (typically {@code ColumnChunk.column_index_offset}, {@code .offset_index_offset}, or
 * {@code .data_page_offset}).
 *
 * <p>The write side ({@code writeXxx}) encodes the same Thrift structures to an {@link OutputStream}. Callers are
 * responsible for placing the bytes at the right offsets in the resulting file (the page header before the page bytes,
 * the column index / offset index / bloom filter after each row group's data, and the footer + footer length + trailing
 * magic at the end).
 *
 * <p>Parquet file layout:
 *
 * <pre>
 *   [4-byte magic "PAR1"]
 *   [row group data ...]
 *   [per-row-group column index / offset index / bloom filter blobs (optional)]
 *   [FileMetaData Thrift compact bytes]
 *   [4-byte footer length (little-endian)]
 *   [4-byte magic "PAR1"]
 * </pre>
 *
 * <p>If you already have the raw Thrift bytes in hand (e.g. by slicing a {@code ByteBuffer} from a custom transport) or
 * want to write directly to a {@code ByteBuffer}, call {@link ParquetFormatDeserializer} or
 * {@link ParquetFormatSerializer} directly. The methods here are the convenience surface that wraps those serializers
 * around a stream-shaped caller.
 */
public final class ParquetFormat {

    private static final byte[] MAGIC = {'P', 'A', 'R', '1'};
    private static final byte[] MAGIC_ENCRYPTED = {'P', 'A', 'R', 'E'};

    /** Minimum valid file: 4-byte header magic + 4-byte footer length + 4-byte tail magic. */
    private static final int MIN_FILE_SIZE = 12;

    private ParquetFormat() {}

    /**
     * Reads a {@link PageHeader} from {@code in}.
     *
     * <p>Each page in a column chunk is preceded by a Thrift-encoded {@code PageHeader} struct. The caller positions
     * the stream at the start of the header (typically the first byte after the column chunk's {@code data_page_offset}
     * or just after the previous page's payload).
     *
     * @param in stream positioned at the start of a page header; caller retains ownership
     * @return the decoded {@link PageHeader}
     * @throws ParquetFormatException if the bytes don't conform to the Thrift-compact / Parquet page-header layout
     * @throws UncheckedIOException if the underlying {@code InputStream} fails or ends prematurely
     */
    public static PageHeader readPageHeader(InputStream in) {
        return ParquetFormatDeserializer.readPageHeader(in);
    }

    /**
     * Reads the {@link ColumnIndex} for a column chunk from a Parquet file. The {@code offset} and {@code length} are
     * taken from {@code ColumnChunk.column_index_offset} and {@code .column_index_length}.
     *
     * @param reader source of byte-range reads for the file; caller retains ownership
     * @param offset byte offset of the ColumnIndex in the file
     * @param length byte length of the ColumnIndex
     * @return the decoded {@link ColumnIndex}
     * @throws ParquetFormatException if the bytes at {@code offset} don't conform to the {@code ColumnIndex} layout
     * @throws UncheckedIOException if the underlying {@link RangeReader} read fails
     */
    public static ColumnIndex readColumnIndex(RangeReader reader, long offset, int length) {
        ByteBuffer buf = reader.readRange(offset, length);
        buf.flip();
        return ParquetFormatDeserializer.readColumnIndex(toInputStream(buf));
    }

    /**
     * Reads the {@link OffsetIndex} for a column chunk from a Parquet file. The {@code offset} and {@code length} are
     * taken from {@code ColumnChunk.offset_index_offset} and {@code .offset_index_length}.
     *
     * @param reader source of byte-range reads for the file; caller retains ownership
     * @param offset byte offset of the OffsetIndex in the file
     * @param length byte length of the OffsetIndex
     * @return the decoded {@link OffsetIndex}
     * @throws ParquetFormatException if the bytes at {@code offset} don't conform to the {@code OffsetIndex} layout
     * @throws UncheckedIOException if the underlying {@link RangeReader} read fails
     */
    public static OffsetIndex readOffsetIndex(RangeReader reader, long offset, int length) {
        ByteBuffer buf = reader.readRange(offset, length);
        buf.flip();
        return ParquetFormatDeserializer.readOffsetIndex(toInputStream(buf));
    }

    /**
     * Reads the {@link FileMetaData} footer from a Parquet file.
     *
     * @param reader source of byte-range reads for the file; caller retains ownership
     * @return the decoded {@link FileMetaData}
     * @throws ParquetFormatException if the file is too small, has wrong magic bytes, uses encryption (PARE magic),
     *     declares an invalid footer length, or contains Thrift bytes that don't match the {@code FileMetaData} layout
     * @throws UncheckedIOException if the underlying {@link RangeReader} read fails, or the reader cannot determine the
     *     file size
     */
    public static FileMetaData readFooter(RangeReader reader) {
        final long size =
                reader.size().orElseThrow(() -> new MalformedFileException("RangeReader cannot determine file size"));
        if (size < MIN_FILE_SIZE) {
            throw new NotAParquetFileException("File too small to be a Parquet file: " + size + " bytes");
        }
        ByteBuffer tail = reader.readRange(size - 8, 8);
        tail.flip();
        tail.order(LITTLE_ENDIAN);
        int footerLen = tail.getInt();
        byte[] magic = new byte[4];
        tail.get(magic);
        if (Arrays.equals(magic, MAGIC_ENCRYPTED)) {
            throw new UnsupportedFeatureException("Encrypted file (PARE magic); requires parquetry-encryption module");
        }
        if (!Arrays.equals(magic, MAGIC)) {
            throw new NotAParquetFileException("Not a Parquet file (bad magic): " + new String(magic));
        }
        if (footerLen <= 0) {
            throw new MalformedFileException("Invalid footer length: " + footerLen);
        }
        long footerStart = size - 8 - footerLen;
        if (footerStart < 0) {
            throw new MalformedFileException("Footer length " + footerLen + " extends before start of file");
        }
        ByteBuffer footerBytes = reader.readRange(footerStart, footerLen);
        footerBytes.flip();
        return ParquetFormatDeserializer.readFileMetaData(toInputStream(footerBytes));
    }

    /**
     * Writes a {@link PageHeader} to {@code out} as Thrift compact bytes.
     *
     * <p>Symmetric inverse of {@link #readPageHeader(InputStream)}: the bytes this method emits parse back through the
     * reader to a {@link PageHeader} equal to {@code header}. Caller positions / flushes the stream and retains
     * ownership.
     *
     * @param out stream to receive the encoded bytes
     * @param header the page header to encode
     * @throws UncheckedIOException if the underlying {@link OutputStream} fails
     */
    public static void writePageHeader(OutputStream out, PageHeader header) {
        ParquetFormatSerializer.writePageHeader(out, header);
    }

    /**
     * Writes a {@link ColumnIndex} to {@code out} as Thrift compact bytes. Symmetric inverse of
     * {@link #readColumnIndex(RangeReader, long, int)}.
     *
     * @param out stream to receive the encoded bytes
     * @param index the column index to encode
     * @throws UncheckedIOException if the underlying {@link OutputStream} fails
     */
    public static void writeColumnIndex(OutputStream out, ColumnIndex index) {
        ParquetFormatSerializer.writeColumnIndex(out, index);
    }

    /**
     * Writes an {@link OffsetIndex} to {@code out} as Thrift compact bytes. Symmetric inverse of
     * {@link #readOffsetIndex(RangeReader, long, int)}.
     *
     * @param out stream to receive the encoded bytes
     * @param index the offset index to encode
     * @throws UncheckedIOException if the underlying {@link OutputStream} fails
     */
    public static void writeOffsetIndex(OutputStream out, OffsetIndex index) {
        ParquetFormatSerializer.writeOffsetIndex(out, index);
    }

    /**
     * Writes a {@link BloomFilterHeader} to {@code out} as Thrift compact bytes. The bitset bytes follow this header in
     * the file layout but are emitted by the caller; this method only encodes the header itself.
     *
     * @param out stream to receive the encoded bytes
     * @param header the bloom filter header to encode
     * @throws UncheckedIOException if the underlying {@link OutputStream} fails
     */
    public static void writeBloomFilterHeader(OutputStream out, BloomFilterHeader header) {
        ParquetFormatSerializer.writeBloomFilterHeader(out, header);
    }

    /**
     * Writes the {@link FileMetaData} footer to {@code out} as Thrift compact bytes.
     *
     * <p>Emits the footer struct only; callers that need the full Parquet file framing (magic bytes, footer length,
     * tail magic) must produce those themselves. Use {@link #toBytes(FileMetaData)} when an in-memory byte buffer is
     * more convenient than a stream.
     *
     * @param out stream to receive the encoded bytes
     * @param footer the file-level metadata to encode
     * @throws UncheckedIOException if the underlying {@link OutputStream} fails
     */
    public static void writeFooter(OutputStream out, FileMetaData footer) {
        ParquetFormatSerializer.writeFileMetaData(out, footer);
    }

    /**
     * Convenience: encodes {@code footer} into a fresh byte array via {@link #writeFooter(OutputStream, FileMetaData)}.
     *
     * @param footer the file-level metadata to encode
     * @return the encoded Thrift compact bytes of {@code footer}
     * @throws UncheckedIOException if the in-memory write somehow fails
     */
    public static byte[] toBytes(FileMetaData footer) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeFooter(out, footer);
        return out.toByteArray();
    }

    private static InputStream toInputStream(ByteBuffer buf) {
        return new ByteBufferInputStream(buf);
    }

    private static final class ByteBufferInputStream extends InputStream {

        private final ByteBuffer buffer;

        ByteBufferInputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() {
            if (buffer.hasRemaining()) {
                return buffer.get() & 0xff;
            }
            return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (buffer.hasRemaining()) {
                int toRead = Math.min(len, buffer.remaining());
                buffer.get(b, off, toRead);
                return toRead;
            }
            return -1;
        }

        @Override
        public int available() {
            return buffer.remaining();
        }
    }
}
