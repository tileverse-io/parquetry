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
package io.tileverse.parquetry.internal.write.page;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.DictionaryPageHeader;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.write.ColumnContext;

/**
 * Serializes data and dictionary pages onto a column-chunk channel.
 *
 * <p>The two data-page methods, {@link #writeDataPageV2} and {@link #writeDataPageV1}, share the same input shape -- a
 * primitive-carrier of values, the rep/def level arrays, a value encoder, and a {@link PageStatistics} snapshot -- but
 * differ in wire layout. V2 stores rep/def bytes uncompressed in front of the compressed values; V1 concatenates the
 * level-length-prefixed level bytes with the values and compresses the whole blob. The {@code *PreEncoded} variants
 * accept the already-encoded value bytes plus an explicit encoding marker, supporting page-level encoders such as
 * {@link DictionaryAttemptEncoder} that emit the value bytes themselves and only need the framing service.
 * {@link #writeDictionaryPage} emits a PLAIN-encoded dictionary that precedes any dictionary-encoded data page in the
 * same column chunk.
 *
 * <p>One writer is bound to a single {@link ColumnContext}: the context resolves the codec instance, the V1/V2 page
 * layout, and the page-header encoding markers that match the column's physical type.
 */
public final class PageWriter {

    private final ColumnContext column;
    private final Compression codec;

    public PageWriter(ColumnContext column) {
        this.column = column;
        this.codec = column.compression();
    }

    /**
     * Writes a V2 data page to {@code dst}. Levels are emitted uncompressed before the (optionally) compressed values.
     */
    public EncodedPage writeDataPageV2(PageEncodeJob job, WritableByteChannel dst) throws IOException {
        int nonNullCount = job.payloadValueCount() - job.nullCount();
        byte[] rawValueBytes = encodeValues(job.valuesEncoder(), job.payloadValues(), nonNullCount);
        MemorySegment encodedValues = MemorySegment.ofArray(rawValueBytes).asReadOnly();
        PreEncodedPageJob preEncoded = new PreEncodedPageJob(
                encodedValues,
                job.valuesEncoder().parquetEncoding(),
                job.payloadValueCount(),
                job.nullCount(),
                job.rowCount(),
                job.repetitionLevels(),
                job.definitionLevels(),
                job.pageStats());
        return emitDataPageV2(preEncoded, dst);
    }

    /**
     * Writes a V2 data page using value bytes that have already been encoded by the caller. The
     * {@link PreEncodedPageJob#valuesEncoding()} marker is placed verbatim into the page header.
     */
    public EncodedPage writeDataPageV2PreEncoded(PreEncodedPageJob job, WritableByteChannel dst) throws IOException {
        return emitDataPageV2(job, dst);
    }

    /**
     * Writes a V1 data page to {@code dst}. The level-length-prefixed level bytes and the value bytes are concatenated
     * and compressed as a single blob; the page header carries only the total compressed and uncompressed sizes.
     */
    public EncodedPage writeDataPageV1(PageEncodeJob job, WritableByteChannel dst) throws IOException {
        int nonNullCount = job.payloadValueCount() - job.nullCount();
        byte[] rawValueBytes = encodeValues(job.valuesEncoder(), job.payloadValues(), nonNullCount);
        MemorySegment encodedValues = MemorySegment.ofArray(rawValueBytes).asReadOnly();
        PreEncodedPageJob preEncoded = new PreEncodedPageJob(
                encodedValues,
                job.valuesEncoder().parquetEncodingV1(),
                job.payloadValueCount(),
                job.nullCount(),
                job.rowCount(),
                job.repetitionLevels(),
                job.definitionLevels(),
                job.pageStats());
        return emitDataPageV1(preEncoded, dst);
    }

    /**
     * Writes a V1 data page using value bytes that have already been encoded by the caller. The
     * {@link PreEncodedPageJob#valuesEncoding()} marker is placed verbatim into the page header.
     */
    public EncodedPage writeDataPageV1PreEncoded(PreEncodedPageJob job, WritableByteChannel dst) throws IOException {
        return emitDataPageV1(job, dst);
    }

    /**
     * Writes a dictionary page to {@code dst}. The dictionary values are PLAIN-encoded, then compressed via the
     * column's codec. The encoding marker on the page header is V1's {@link Encoding#PLAIN_DICTIONARY} for V1 columns
     * and {@link Encoding#PLAIN} for V2 columns, matching what the read-side dictionary decoder expects.
     */
    public EncodedPage writeDictionaryPage(
            Object dictionaryValues, int valueCount, Encoder<?> plainEncoder, WritableByteChannel dst)
            throws IOException {

        byte[] rawValueBytes = encodeValues(plainEncoder, dictionaryValues, valueCount);
        boolean shouldCompress = !(column.compression() instanceof Compression.Uncompressed);
        byte[] pageBytes = shouldCompress ? compress(rawValueBytes) : rawValueBytes;

        Encoding encoding = column.parquetVersion() == ParquetVersion.V1_1 ? Encoding.PLAIN_DICTIONARY : Encoding.PLAIN;
        DictionaryPageHeader dict = new DictionaryPageHeader(valueCount, encoding, false);
        PageHeader header = new PageHeader(
                PageType.DICTIONARY_PAGE,
                rawValueBytes.length,
                pageBytes.length,
                OptionalInt.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(dict),
                Optional.empty());

        int headerBytes = writeHeader(header, dst);
        ChannelWrites.writeFully(dst, ByteBuffer.wrap(pageBytes));

        return new EncodedPage(header, headerBytes, pageBytes.length);
    }

    private EncodedPage emitDataPageV2(PreEncodedPageJob job, WritableByteChannel dst) throws IOException {
        byte[] repLevelBytes = encodeLevels(job.repetitionLevels(), column.maxRepetitionLevel());
        byte[] defLevelBytes = encodeLevels(job.definitionLevels(), column.maxDefinitionLevel());
        byte[] rawValueBytes = job.encodedValueBytes().toArray(ValueLayout.JAVA_BYTE);

        boolean shouldCompress = !(column.compression() instanceof Compression.Uncompressed);
        byte[] valueBytes = shouldCompress ? compress(rawValueBytes) : rawValueBytes;

        int uncompressedPageSize = repLevelBytes.length + defLevelBytes.length + rawValueBytes.length;
        int compressedPageSize = repLevelBytes.length + defLevelBytes.length + valueBytes.length;

        DataPageHeaderV2 v2 = new DataPageHeaderV2(
                job.payloadValueCount(),
                job.nullCount(),
                job.rowCount(),
                job.valuesEncoding(),
                defLevelBytes.length,
                repLevelBytes.length,
                shouldCompress,
                statisticsFromPage(job.pageStats()));
        PageHeader header = new PageHeader(
                PageType.DATA_PAGE_V2,
                uncompressedPageSize,
                compressedPageSize,
                OptionalInt.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(v2));

        int headerBytes = writeHeader(header, dst);
        ChannelWrites.writeFully(dst, ByteBuffer.wrap(repLevelBytes));
        ChannelWrites.writeFully(dst, ByteBuffer.wrap(defLevelBytes));
        ChannelWrites.writeFully(dst, ByteBuffer.wrap(valueBytes));

        return new EncodedPage(header, headerBytes, compressedPageSize);
    }

    private EncodedPage emitDataPageV1(PreEncodedPageJob job, WritableByteChannel dst) throws IOException {
        byte[] repBlock = encodeV1LevelBlock(job.repetitionLevels(), column.maxRepetitionLevel());
        byte[] defBlock = encodeV1LevelBlock(job.definitionLevels(), column.maxDefinitionLevel());
        byte[] rawValueBytes = job.encodedValueBytes().toArray(ValueLayout.JAVA_BYTE);

        byte[] uncompressedPayload = concat(repBlock, defBlock, rawValueBytes);
        boolean shouldCompress = !(column.compression() instanceof Compression.Uncompressed);
        byte[] pageBytes = shouldCompress ? compress(uncompressedPayload) : uncompressedPayload;

        DataPageHeader v1 = new DataPageHeader(
                job.payloadValueCount(),
                job.valuesEncoding(),
                Encoding.RLE,
                Encoding.RLE,
                statisticsFromPage(job.pageStats()));
        PageHeader header = new PageHeader(
                PageType.DATA_PAGE,
                uncompressedPayload.length,
                pageBytes.length,
                OptionalInt.empty(),
                Optional.of(v1),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        int headerBytes = writeHeader(header, dst);
        ChannelWrites.writeFully(dst, ByteBuffer.wrap(pageBytes));

        return new EncodedPage(header, headerBytes, pageBytes.length);
    }

    private byte[] encodeLevels(int[] levels, int maxLevel) throws IOException {
        if (maxLevel == 0) {
            return new byte[0];
        }
        if (levels == null || levels.length == 0) {
            return new byte[0];
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        WritableByteChannel bufferChannel = Channels.newChannel(buffer);
        new LevelEncoder(maxLevel).encode(levels, levels.length, bufferChannel);
        return buffer.toByteArray();
    }

    /**
     * Builds the V1 level stream: when the column has a non-zero max level, prefix the RLE-encoded level bytes with
     * their length as a little-endian 4-byte integer; otherwise emit nothing.
     */
    private byte[] encodeV1LevelBlock(int[] levels, int maxLevel) throws IOException {
        if (maxLevel == 0) {
            return new byte[0];
        }
        byte[] encoded = encodeLevels(levels, maxLevel);
        byte[] block = new byte[Integer.BYTES + encoded.length];
        ByteBuffer.wrap(block).order(LITTLE_ENDIAN).putInt(encoded.length);
        System.arraycopy(encoded, 0, block, Integer.BYTES, encoded.length);
        return block;
    }

    private static byte[] encodeValues(Encoder<?> encoder, Object values, int n) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        WritableByteChannel bufferChannel = Channels.newChannel(buffer);
        encodeWithErasedCarrier(encoder, values, n, bufferChannel);
        return buffer.toByteArray();
    }

    // Carrier types are encoder-specific (int[], long[], byte[][], ...); the runtime cast is intentional.
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void encodeWithErasedCarrier(Encoder encoder, Object values, int n, WritableByteChannel dst)
            throws IOException {
        encoder.encode(values, n, dst);
    }

    private byte[] compress(byte[] source) throws IOException {
        if (source.length == 0) {
            return source;
        }
        long maxLen = codec.maxCompressedLength(source.length);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment src = arena.allocate(source.length);
            MemorySegment.copy(source, 0, src, ValueLayout.JAVA_BYTE, 0L, source.length);
            MemorySegment dst = arena.allocate(maxLen);
            int written = codec.compress(src, dst);
            byte[] out = new byte[written];
            MemorySegment.copy(dst, ValueLayout.JAVA_BYTE, 0L, out, 0, written);
            return out;
        }
    }

    private static int writeHeader(PageHeader header, WritableByteChannel dst) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(buffer, header);
        byte[] bytes = buffer.toByteArray();
        ChannelWrites.writeFully(dst, ByteBuffer.wrap(bytes));
        return bytes.length;
    }

    private Optional<Statistics> statisticsFromPage(PageStatistics pageStats) {
        if (pageStats == null) {
            return Optional.empty();
        }
        MemorySegment min = pageStats.min();
        MemorySegment max = pageStats.max();
        boolean hasMinMax = min != MemorySegment.NULL || max != MemorySegment.NULL;
        boolean hasNullCount = pageStats.nullCount() > 0L || pageStats.isNullPage();
        if (!hasMinMax && !hasNullCount) {
            return Optional.empty();
        }
        Statistics statistics = Statistics.builder()
                .nullCount(OptionalLong.of(pageStats.nullCount()))
                .distinctCount(OptionalLong.empty())
                .minValue(min)
                .maxValue(max)
                .isMinValueExact(min != MemorySegment.NULL)
                .isMaxValueExact(max != MemorySegment.NULL)
                .build();
        return Optional.of(statistics);
    }

    private static byte[] concat(byte[] a, byte[] b, byte[] c) {
        byte[] out = new byte[a.length + b.length + c.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        System.arraycopy(c, 0, out, a.length + b.length, c.length);
        return out;
    }
}
