/*
 * Copyright (c) 2026 Multivers.io
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Asserts {@link BatchColumnReader} retains the full definition-level stream for a column with an optional ancestor,
 * rather than collapsing it to a present/absent bit per value.
 *
 * <p>The fixture models the element leaf of an optional list of optional ints (the shape of the {@code null_list}
 * corpus): {@code maxRep == 1}, {@code maxDef == 2}. The def levels then take intermediate values - {@code 0} for a
 * null outer list, {@code 1} for a present-but-empty list or a null element, {@code 2} for a present element. A value
 * strictly between {@code 0} and {@code maxDef} can only appear if the stream is retained whole; the collapsed
 * present/absent bitmap discards it.
 */
class DefLevelRetentionTest {

    private static final ColumnPath ELEMENT_PATH = ColumnPath.of("items", "list", "element");

    @Test
    void retainsIntermediateDefinitionLevels() throws IOException {
        // Four elements with def levels {2, 0, 1, 2}: two present (def==2), one null outer list (def==0),
        // one intermediate (def==1). Only the two present elements have value bytes.
        int[] defLevels = {2, 0, 1, 2};
        int[] repLevels = {0, 0, 0, 0};
        int[] presentValues = {100, 200};
        int maxDef = 2;
        int maxRep = 1;

        FetchedColumnChunk chunk = repeatedOptionalInt32Chunk(presentValues, repLevels, defLevels, maxRep, maxDef);
        SchemaNode.Primitive leaf = optionalInt32Element();

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf);

        int[] elementDefs = reader.currentPageDefLevels();

        assertThat(elementDefs)
                .as("def-level stream is retained, not collapsed to present/absent")
                .isNotNull()
                .containsExactly(2, 0, 1, 2);

        boolean hasIntermediate = false;
        for (int d : elementDefs) {
            if (d > 0 && d < maxDef) {
                hasIntermediate = true;
            }
        }
        assertThat(hasIntermediate)
                .as("def-level stream retains an intermediate level strictly between 0 and maxDef")
                .isTrue();
    }

    // --- fixture helpers ---

    /**
     * Builds a {@link FetchedColumnChunk} with a single repeated-optional INT32 V1 page. The decompressed payload is
     * {@code [int32 LE repLen][rep bytes][int32 LE defLen][def bytes][value bytes]}; value bytes cover only the present
     * (def == maxDef) elements.
     */
    private static FetchedColumnChunk repeatedOptionalInt32Chunk(
            int[] presentValues, int[] repLevels, int[] defLevels, int maxRep, int maxDef) throws IOException {
        byte[] repBytes = rleEncodeBits(repLevels, maxRep);
        byte[] defBytes = rleEncodeBits(defLevels, maxDef);
        byte[] valueBytes = encodeInt32sLittleEndian(presentValues);
        byte[] payload = buildV1Payload(repBytes, defBytes, valueBytes);
        byte[] chunkBuffer = encodeV1Page(defLevels.length, payload);
        return heapChunk(chunkBuffer, defLevels.length, maxRep, maxDef);
    }

    private static byte[] buildV1Payload(byte[] repBytes, byte[] defBytes, byte[] valueBytes) {
        ByteBuffer buf = ByteBuffer.allocate(4 + repBytes.length + 4 + defBytes.length + valueBytes.length)
                .order(LITTLE_ENDIAN);
        buf.putInt(repBytes.length);
        buf.put(repBytes);
        buf.putInt(defBytes.length);
        buf.put(defBytes);
        buf.put(valueBytes);
        return buf.array();
    }

    private static byte[] encodeV1Page(int numValues, byte[] payload) throws IOException {
        PageHeader header = new PageHeader(PageType.DATA_PAGE, payload.length, payload.length);
        DataPageHeader dataHeader = new DataPageHeader(
                numValues,
                org.apache.parquet.format.Encoding.PLAIN,
                org.apache.parquet.format.Encoding.RLE,
                org.apache.parquet.format.Encoding.RLE);
        header.setData_page_header(dataHeader);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writePageHeader(header, out);
        out.write(payload);
        return out.toByteArray();
    }

    /**
     * RLE-encodes a sequence of level values as single-element RLE runs (runLen=1 each), the simplest hand-rolled form.
     * Each run is {@code varint(1 << 1)} followed by the value in {@code ceil(bitWidth/8)} little-endian bytes.
     */
    private static byte[] rleEncodeBits(int[] levels, int maxLevel) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int bitWidth = computeBitWidth(maxLevel);
        int bytesPerValue = (bitWidth + 7) / 8;
        for (int level : levels) {
            writeVarint(out, 2L);
            for (int b = 0; b < bytesPerValue; b++) {
                out.write((level >>> (b * 8)) & 0xff);
            }
        }
        return out.toByteArray();
    }

    private static int computeBitWidth(int maxLevel) {
        if (maxLevel == 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxLevel);
    }

    private static void writeVarint(ByteArrayOutputStream out, long value) {
        long remaining = value;
        while (true) {
            if ((remaining & ~0x7fL) == 0) {
                out.write((int) remaining);
                return;
            }
            out.write(((int) remaining & 0x7f) | 0x80);
            remaining >>>= 7;
        }
    }

    private static List<String> pathSegments(ColumnPath path) {
        String[] segments = new String[path.numParts()];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = path.part(i);
        }
        return List.of(segments);
    }

    private static FetchedColumnChunk heapChunk(byte[] data, long numValues, int maxRep, int maxDef) {
        MemorySegment segment = MemorySegment.ofArray(data).asReadOnly();

        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(pathSegments(ELEMENT_PATH))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(numValues)
                .totalUncompressedSize((long) data.length)
                .totalCompressedSize((long) data.length)
                .dataPageOffset(0L)
                .build();

        return new FetchedColumnChunk(ELEMENT_PATH, meta, maxRep, maxDef, segment, Optional.empty());
    }

    private static SchemaNode.Primitive optionalInt32Element() {
        return new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static byte[] encodeInt32sLittleEndian(int[] values) {
        ByteBuffer buf = ByteBuffer.allocate(values.length * 4).order(LITTLE_ENDIAN);
        for (int v : values) {
            buf.putInt(v);
        }
        return buf.array();
    }
}
