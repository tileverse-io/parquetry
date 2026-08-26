/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
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
 * Serializes data and dictionary pages into a page byte sink.
 *
 * <p>The two data-page methods, {@link #writeDataPageV2} and {@link #writeDataPageV1}, share the same input shape -- a
 * primitive-carrier of values, the rep/def level arrays, a value encoder, and a {@link PageStatistics} snapshot -- but
 * differ in wire layout. V2 stores rep/def bytes uncompressed in front of the compressed values; V1 concatenates the
 * level-length-prefixed level bytes with the values and compresses the whole blob. The {@code *PreEncoded} variants
 * accept the already-encoded value bytes plus an explicit encoding marker, supporting {@link PageDictionaryEncoder}
 * implementations that emit the value bytes themselves and only need the framing service. {@link #writeDictionaryPage}
 * emits a PLAIN-encoded dictionary that precedes any dictionary-encoded data page in the same column chunk.
 *
 * <p>One writer is bound to a single {@link ColumnContext}: the context resolves the codec instance, the V1/V2 page
 * layout, and the page-header encoding markers that match the column's physical type.
 */
public final class PageWriter {

    private final ColumnContext column;
    private final Compression codec;

    private final GrowableByteSink plainValueSink = new GrowableByteSink(64);
    private final GrowableByteSink repLevelSink = new GrowableByteSink(64);
    private final GrowableByteSink defLevelSink = new GrowableByteSink(64);
    private final GrowableByteSink v1PayloadSink = new GrowableByteSink(64);
    private final ByteArrayOutputStream headerScratch = new ByteArrayOutputStream();
    private byte[] compressScratch = new byte[0];

    public PageWriter(ColumnContext column) {
        this.column = column;
        this.codec = column.compression();
    }

    /**
     * Writes a V2 data page to {@code dst}. Levels are emitted uncompressed before the (optionally) compressed values.
     */
    public EncodedPage writeDataPageV2(PageEncodeJob job, LittleEndianSink dst) throws IOException {
        int nonNullCount = job.payloadValueCount() - job.nullCount();
        MemorySegment encodedValues = encodeValuesSegment(job.valuesEncoder(), job.payloadValues(), nonNullCount);
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
    public EncodedPage writeDataPageV2PreEncoded(PreEncodedPageJob job, LittleEndianSink dst) throws IOException {
        return emitDataPageV2(job, dst);
    }

    /**
     * Writes a V1 data page to {@code dst}. The level-length-prefixed level bytes and the value bytes are concatenated
     * and compressed as a single blob; the page header carries only the total compressed and uncompressed sizes.
     */
    public EncodedPage writeDataPageV1(PageEncodeJob job, LittleEndianSink dst) throws IOException {
        int nonNullCount = job.payloadValueCount() - job.nullCount();
        MemorySegment encodedValues = encodeValuesSegment(job.valuesEncoder(), job.payloadValues(), nonNullCount);
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
    public EncodedPage writeDataPageV1PreEncoded(PreEncodedPageJob job, LittleEndianSink dst) throws IOException {
        return emitDataPageV1(job, dst);
    }

    /**
     * Writes a dictionary page to {@code dst}. The dictionary values are PLAIN-encoded, then compressed via the
     * column's codec. The encoding marker on the page header is V1's {@link Encoding#PLAIN_DICTIONARY} for V1 columns
     * and {@link Encoding#PLAIN} for V2 columns, matching what the read-side dictionary decoder expects.
     */
    public EncodedPage writeDictionaryPage(
            Object dictionaryValues, int valueCount, Encoder<?> plainEncoder, LittleEndianSink dst) throws IOException {

        byte[] rawValueBytes = encodeValues(plainEncoder, dictionaryValues, valueCount);
        boolean shouldCompress = !(column.compression() instanceof Compression.Uncompressed);
        byte[] pageBytes;
        int pageLen;
        if (shouldCompress) {
            pageLen = compressInto(MemorySegment.ofArray(rawValueBytes));
            pageBytes = compressScratch;
        } else {
            pageBytes = rawValueBytes;
            pageLen = rawValueBytes.length;
        }

        Encoding encoding = column.parquetVersion() == ParquetVersion.V1_1 ? Encoding.PLAIN_DICTIONARY : Encoding.PLAIN;
        DictionaryPageHeader dict = new DictionaryPageHeader(valueCount, encoding, false);
        PageHeader header = new PageHeader(
                PageType.DICTIONARY_PAGE,
                rawValueBytes.length,
                pageLen,
                OptionalInt.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(dict),
                Optional.empty());

        int headerBytes = writeHeader(header, dst);
        dst.write(pageBytes, 0, pageLen);

        return new EncodedPage(header, headerBytes, pageLen);
    }

    private EncodedPage emitDataPageV2(PreEncodedPageJob job, LittleEndianSink dst) throws IOException {
        int repLen = encodeLevelsInto(
                repLevelSink, job.repetitionLevels(), job.payloadValueCount(), column.maxRepetitionLevel());
        int defLen = encodeLevelsInto(
                defLevelSink, job.definitionLevels(), job.payloadValueCount(), column.maxDefinitionLevel());
        MemorySegment encodedValues = job.encodedValueBytes();
        int rawValueByteSize = Math.toIntExact(encodedValues.byteSize());
        boolean shouldCompress = !(column.compression() instanceof Compression.Uncompressed);

        int valueLen;
        if (shouldCompress) {
            valueLen = compressInto(encodedValues);
        } else {
            valueLen = rawValueByteSize;
        }

        int uncompressedPageSize = repLen + defLen + rawValueByteSize;
        int compressedPageSize = repLen + defLen + valueLen;

        DataPageHeaderV2 v2 = new DataPageHeaderV2(
                job.payloadValueCount(),
                job.nullCount(),
                job.rowCount(),
                job.valuesEncoding(),
                defLen,
                repLen,
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
        repLevelSink.writeInto(dst);
        defLevelSink.writeInto(dst);
        if (shouldCompress) {
            dst.write(compressScratch, 0, valueLen);
        } else {
            dst.write(encodedValues);
        }

        return new EncodedPage(header, headerBytes, compressedPageSize);
    }

    private EncodedPage emitDataPageV1(PreEncodedPageJob job, LittleEndianSink dst) throws IOException {
        v1PayloadSink.reset();
        int repBlockLen = writeV1LevelBlock(
                v1PayloadSink, job.repetitionLevels(), job.payloadValueCount(), column.maxRepetitionLevel());
        int defBlockLen = writeV1LevelBlock(
                v1PayloadSink, job.definitionLevels(), job.payloadValueCount(), column.maxDefinitionLevel());
        MemorySegment encodedValues = job.encodedValueBytes();
        int rawValueByteSize = Math.toIntExact(encodedValues.byteSize());
        v1PayloadSink.write(encodedValues);
        int uncompressedPayloadLen = repBlockLen + defBlockLen + rawValueByteSize;

        boolean shouldCompress = !(column.compression() instanceof Compression.Uncompressed);
        byte[] pageBytes;
        int pageLen;
        if (shouldCompress) {
            pageLen = compressInto(v1PayloadSink.codecSegment());
            pageBytes = compressScratch;
        } else {
            pageBytes = v1PayloadSink.array();
            pageLen = v1PayloadSink.size();
        }

        DataPageHeader v1 = new DataPageHeader(
                job.payloadValueCount(),
                job.valuesEncoding(),
                Encoding.RLE,
                Encoding.RLE,
                statisticsFromPage(job.pageStats()));
        PageHeader header = new PageHeader(
                PageType.DATA_PAGE,
                uncompressedPayloadLen,
                pageLen,
                OptionalInt.empty(),
                Optional.of(v1),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        int headerBytes = writeHeader(header, dst);
        dst.write(pageBytes, 0, pageLen);

        return new EncodedPage(header, headerBytes, pageLen);
    }

    private MemorySegment encodeValuesSegment(Encoder<?> encoder, Object values, int n) throws IOException {
        plainValueSink.reset();
        encodeWithErasedCarrier(encoder, values, n, plainValueSink);
        return plainValueSink.codecSegment();
    }

    private byte[] encodeValues(Encoder<?> encoder, Object values, int n) throws IOException {
        plainValueSink.reset();
        encodeWithErasedCarrier(encoder, values, n, plainValueSink);
        return plainValueSink.toByteArray();
    }

    /**
     * Encodes the first {@code count} levels from {@code levels} into {@code target}; returns the byte length. Empty
     * when {@code maxLevel == 0}. {@code levels} may be an oversized backing array reused across pages, hence the
     * explicit count rather than the array length.
     */
    private int encodeLevelsInto(GrowableByteSink target, int[] levels, int count, int maxLevel) throws IOException {
        target.reset();
        if (maxLevel == 0 || levels == null || count == 0) {
            return 0;
        }
        return new LevelEncoder(maxLevel).encode(levels, count, target);
    }

    /**
     * Writes the V1 level block (a 4-byte little-endian length prefix followed by the RLE level bytes) into
     * {@code target} and returns the total bytes written; writes nothing and returns zero when the column has no levels
     * at this max. The transient RLE bytes go through {@link #repLevelSink}, which is copied into {@code target} right
     * away, hence the rep and def calls do not collide.
     */
    private int writeV1LevelBlock(GrowableByteSink target, int[] levels, int count, int maxLevel) throws IOException {
        if (maxLevel == 0) {
            return 0;
        }
        int encodedLen = encodeLevelsInto(repLevelSink, levels, count, maxLevel);
        target.writeInt(encodedLen);
        repLevelSink.writeInto(target);
        return Integer.BYTES + encodedLen;
    }

    // Carrier types are encoder-specific (int[], long[], byte[][], ...); the runtime cast is intentional.
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void encodeWithErasedCarrier(Encoder encoder, Object values, int n, LittleEndianSink dst)
            throws IOException {
        encoder.encode(values, n, dst);
    }

    /**
     * Compresses {@code source} into the reusable {@link #compressScratch} buffer, growing it when the codec's worst
     * case exceeds the current capacity, and returns the number of compressed bytes. Callers read
     * {@code compressScratch[0, returned)}; the compressed bytes on the wire are unchanged because the codec is
     * deterministic and sees the same destination bounds as a freshly sized array.
     */
    private int compressInto(MemorySegment source) throws IOException {
        long sourceSize = source.byteSize();
        if (sourceSize == 0L) {
            return 0;
        }
        int maxLen = Math.toIntExact(codec.maxCompressedLength(sourceSize));
        if (compressScratch.length < maxLen) {
            compressScratch = new byte[maxLen];
        }
        MemorySegment dst = MemorySegment.ofArray(compressScratch).asSlice(0L, maxLen);
        return codec.compress(source, dst);
    }

    private int writeHeader(PageHeader header, LittleEndianSink dst) throws IOException {
        headerScratch.reset();
        ParquetFormat.writePageHeader(headerScratch, header);
        byte[] bytes = headerScratch.toByteArray();
        dst.write(bytes, 0, bytes.length);
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
}
