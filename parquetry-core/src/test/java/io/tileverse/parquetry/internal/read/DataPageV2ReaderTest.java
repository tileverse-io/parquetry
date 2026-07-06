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
package io.tileverse.parquetry.internal.read;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.internal.read.page.DataPageV2Reader;
import io.tileverse.parquetry.internal.read.page.DecodedPage;
import io.tileverse.parquetry.schema.LevelMaxima;

/**
 * Unit tests for {@link DataPageV2Reader} that exercise the V2 layout (uncompressed level prefix +
 * optionally-compressed values section) using synthetic payloads constructed in-memory. Codec compression is not
 * exercised here - the values portion uses {@code UNCOMPRESSED} with {@code is_compressed=false} so we can assert
 * byte-exact equality after decode. The streaming path (which does invoke real codecs) is exercised separately by the
 * conformance tests.
 */
class DataPageV2ReaderTest {

    // V2 ignores the maxLevels argument (its level byte lengths come from the header). Pass a non-null sentinel.
    private static final LevelMaxima IGNORED = new LevelMaxima(0, 0);

    private final DataPageV2Reader reader = new DataPageV2Reader();
    private final Compression uncompressed = Compression.uncompressed();

    @Test
    void readsUncompressedV2PageWithoutLevels() throws IOException {
        int[] expected = {10, 20, 30, 40};
        byte[] valueBytes = encodeInt32sLittleEndian(expected);
        byte[] payload = valueBytes;
        PageHeader header =
                newV2Header(expected.length, /*repLen*/ 0, /*defLen*/ 0, payload.length, /*compressed*/ false);

        // DecodedPage owns the Arena; closing it closes the Arena.
        try (DecodedPage page =
                reader.read(header, IGNORED, MemorySegment.ofArray(payload), uncompressed, Arena.ofConfined())) {
            assertThat(page.valueCount()).isEqualTo(expected.length);
            assertThat(page.repLevelBytes())
                    .as("rep segment is NULL when repLen=0")
                    .isEqualTo(MemorySegment.NULL);
            assertThat(page.defLevelBytes())
                    .as("def segment is NULL when defLen=0")
                    .isEqualTo(MemorySegment.NULL);
            assertThat(page.valuesEncoding()).isEqualTo(Encoding.PLAIN);

            int[] decoded =
                    decodeInt32sLittleEndian(page.valueBytes().asByteBuffer().order(LITTLE_ENDIAN), expected.length);
            assertThat(decoded).containsExactly(expected);
        }
    }

    @Test
    void readsUncompressedV2PageWithLevels() throws IOException {
        byte[] repLevels = {0x01, 0x02, 0x03};
        byte[] defLevels = {0x07, 0x08, 0x09, 0x0A};
        int[] expected = {1, 2, 3};
        byte[] valueBytes = encodeInt32sLittleEndian(expected);

        byte[] payload = concat(repLevels, defLevels, valueBytes);
        PageHeader header =
                newV2Header(expected.length, repLevels.length, defLevels.length, payload.length, /*compressed*/ false);

        try (DecodedPage page =
                reader.read(header, IGNORED, MemorySegment.ofArray(payload), uncompressed, Arena.ofConfined())) {
            assertThat(toBytes(page.repLevelBytes())).containsExactly(repLevels);
            assertThat(toBytes(page.defLevelBytes())).containsExactly(defLevels);

            int[] decoded =
                    decodeInt32sLittleEndian(page.valueBytes().asByteBuffer().order(LITTLE_ENDIAN), expected.length);
            assertThat(decoded).containsExactly(expected);
        }
    }

    @Test
    void exercisesCompressedBranchWithIdentityCodec() throws IOException {
        // The UNCOMPRESSED codec copies bytes verbatim from input to output, which lets us exercise the
        // codec-driven decode path without bringing in zstd/snappy infrastructure.
        byte[] repLevels = {0x10, 0x11};
        byte[] defLevels = {0x20, 0x21, 0x22};
        int[] expected = {100, 200};
        byte[] valueBytes = encodeInt32sLittleEndian(expected);

        byte[] payload = concat(repLevels, defLevels, valueBytes);
        PageHeader header =
                newV2Header(expected.length, repLevels.length, defLevels.length, payload.length, /*compressed*/ true);

        try (DecodedPage page =
                reader.read(header, IGNORED, MemorySegment.ofArray(payload), uncompressed, Arena.ofConfined())) {
            assertThat(toBytes(page.repLevelBytes())).containsExactly(repLevels);
            assertThat(toBytes(page.defLevelBytes())).containsExactly(defLevels);
            int[] decoded =
                    decodeInt32sLittleEndian(page.valueBytes().asByteBuffer().order(LITTLE_ENDIAN), expected.length);
            assertThat(decoded).containsExactly(expected);
        }
    }

    @Test
    void normalizesPlainDictionaryToRleDictionary() throws IOException {
        // PLAIN_DICTIONARY-tagged data pages are Parquet 1.x writer dialect; the DecodedPage's valuesEncoding must
        // be the normalized RLE_DICTIONARY so downstream dispatch is single-case.
        byte[] valueBytes = {0x05, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00};
        PageHeader header = new PageHeader(
                PageType.DATA_PAGE_V2,
                /*uncompressedPageSize*/ valueBytes.length,
                /*compressedPageSize*/ valueBytes.length,
                /*crc*/ OptionalInt.empty(),
                /*dataPageHeader*/ Optional.empty(),
                /*indexPageHeader*/ Optional.empty(),
                /*dictionaryPageHeader*/ Optional.empty(),
                Optional.of(new DataPageHeaderV2(
                        /*numValues*/ 2,
                        /*numNulls*/ 0,
                        /*numRows*/ 2,
                        Encoding.PLAIN_DICTIONARY,
                        /*defLevelsByteLength*/ 0,
                        /*repLevelsByteLength*/ 0,
                        /*isCompressed*/ false,
                        Optional.empty())));

        try (DecodedPage page =
                reader.read(header, IGNORED, MemorySegment.ofArray(valueBytes), uncompressed, Arena.ofConfined())) {
            assertThat(page.valuesEncoding()).isEqualTo(Encoding.RLE_DICTIONARY);
        }
    }

    @Test
    void closeReleasesArena() throws IOException {
        byte[] payload = encodeInt32sLittleEndian(new int[] {1});
        PageHeader header = newV2Header(1, 0, 0, payload.length, false);
        Arena arena = Arena.ofConfined();
        DecodedPage page = reader.read(header, IGNORED, MemorySegment.ofArray(payload), uncompressed, arena);
        page.close();
        // After close, the Arena is closed; any further allocation attempt must throw.
        assertThatThrownBy(() -> arena.allocate(1)).isInstanceOf(IllegalStateException.class);
    }

    // --- fixture builders ---

    private static PageHeader newV2Header(int numValues, int repLen, int defLen, int payloadLen, boolean compressed) {
        return new PageHeader(
                PageType.DATA_PAGE_V2,
                /*uncompressedPageSize*/ payloadLen,
                /*compressedPageSize*/ payloadLen,
                /*crc*/ OptionalInt.empty(),
                /*dataPageHeader*/ Optional.empty(),
                /*indexPageHeader*/ Optional.empty(),
                /*dictionaryPageHeader*/ Optional.empty(),
                Optional.of(new DataPageHeaderV2(
                        numValues,
                        /*numNulls*/ 0,
                        /*numRows*/ numValues,
                        Encoding.PLAIN,
                        /*defLevelsByteLength*/ defLen,
                        /*repLevelsByteLength*/ repLen,
                        compressed,
                        Optional.empty())));
    }

    private static byte[] encodeInt32sLittleEndian(int[] values) {
        ByteBuffer buf = ByteBuffer.allocate(values.length * 4).order(LITTLE_ENDIAN);
        for (int v : values) {
            buf.putInt(v);
        }
        return buf.array();
    }

    private static int[] decodeInt32sLittleEndian(ByteBuffer buf, int count) {
        ByteBuffer view = buf.duplicate().order(LITTLE_ENDIAN);
        int[] out = new int[count];
        for (int i = 0; i < count; i++) {
            out[i] = view.getInt();
        }
        return out;
    }

    private static byte[] concat(byte[]... arrays) {
        int total = 0;
        for (byte[] a : arrays) {
            total += a.length;
        }
        byte[] out = new byte[total];
        int off = 0;
        for (byte[] a : arrays) {
            System.arraycopy(a, 0, out, off, a.length);
            off += a.length;
        }
        return out;
    }

    private static byte[] toBytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }
}
