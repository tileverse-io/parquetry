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
package io.tileverse.parquetry.data;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Function;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.EncodingStats;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.SchemaElement;

/**
 * Builds the bytes of a Parquet file with one row group of flat columns, each column chunk holding a single
 * uncompressed data page. The page encoding is given as a wire code: a code unknown to parquetry cannot be expressed
 * through the format records, and is patched into the serialized page header and footer in place of a stand-in.
 */
final class SinglePageParquetFile {

    enum PageVersion {
        V1,
        V2
    }

    /**
     * One flat column. An optional column writes a definition level per row, null where {@code nulls} is set;
     * {@code valueBytes} holds the encoded non-null values.
     */
    record Column(String name, PhysicalType type, boolean optional, BitSet nulls, int encodingCode, byte[] valueBytes) {

        static Column required(String name, PhysicalType type, int encodingCode, byte[] valueBytes) {
            return new Column(name, type, false, new BitSet(), encodingCode, valueBytes);
        }
    }

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
    private static final Encoding STAND_IN = Encoding.PLAIN;
    private static final Encoding SECOND_STAND_IN = Encoding.BYTE_STREAM_SPLIT;

    private SinglePageParquetFile() {}

    static byte[] build(PageVersion version, int rowCount, List<Column> columns) {
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.writeBytes(MAGIC);
        List<ColumnChunkPlacement> placements = new ArrayList<>();
        for (Column column : columns) {
            long pageOffset = file.size();
            byte[] page = page(version, rowCount, column);
            file.writeBytes(page);
            placements.add(new ColumnChunkPlacement(column, pageOffset, page.length));
        }
        byte[] footer =
                withEncodingCode(unknownCodeOf(columns), standIn -> footer(version, rowCount, placements, standIn));
        file.writeBytes(footer);
        file.writeBytes(littleEndianInt(footer.length));
        file.writeBytes(MAGIC);
        return file.toByteArray();
    }

    private record ColumnChunkPlacement(Column column, long pageOffset, int pageLength) {}

    private static byte[] page(PageVersion version, int rowCount, Column column) {
        byte[] levels = column.optional() ? definitionLevels(rowCount, column.nulls()) : new byte[0];
        byte[] payload = payload(version, levels, column);
        Function<Encoding, PageHeader> header = encoding -> switch (version) {
            case V1 -> v1Header(rowCount, encoding, payload.length);
            case V2 -> v2Header(rowCount, column.nulls().cardinality(), encoding, levels.length, payload.length);
        };
        byte[] headerBytes = withEncodingCode(column.encodingCode(), encoding -> serialize(header.apply(encoding)));
        ByteArrayOutputStream page = new ByteArrayOutputStream();
        page.writeBytes(headerBytes);
        page.writeBytes(payload);
        return page.toByteArray();
    }

    /** V1 prefixes the levels with their byte length; V2 keeps that length in the page header. */
    private static byte[] payload(PageVersion version, byte[] levels, Column column) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        if (version == PageVersion.V1 && column.optional()) {
            payload.writeBytes(littleEndianInt(levels.length));
        }
        payload.writeBytes(levels);
        payload.writeBytes(column.valueBytes());
        return payload.toByteArray();
    }

    /** One bit-packed run of the RLE/bit-packing hybrid encoding at bit width 1. */
    private static byte[] definitionLevels(int rowCount, BitSet nulls) {
        int groups = (rowCount + 7) / 8;
        ByteArrayOutputStream levels = new ByteArrayOutputStream();
        writeUnsignedVarint(levels, (groups << 1) | 1);
        byte[] bits = new byte[groups];
        for (int row = 0; row < rowCount; row++) {
            if (!nulls.get(row)) {
                bits[row / 8] |= (byte) (1 << (row % 8));
            }
        }
        levels.writeBytes(bits);
        return levels.toByteArray();
    }

    private static PageHeader v1Header(int rowCount, Encoding encoding, int payloadLength) {
        DataPageHeader dataPage = new DataPageHeader(rowCount, encoding, Encoding.RLE, Encoding.RLE, Optional.empty());
        return new PageHeader(
                PageType.DATA_PAGE,
                payloadLength,
                payloadLength,
                OptionalInt.empty(),
                Optional.of(dataPage),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static PageHeader v2Header(
            int rowCount, int nullCount, Encoding encoding, int levelsLength, int payloadLength) {
        DataPageHeaderV2 dataPage =
                new DataPageHeaderV2(rowCount, nullCount, rowCount, encoding, levelsLength, 0, false, Optional.empty());
        return new PageHeader(
                PageType.DATA_PAGE_V2,
                payloadLength,
                payloadLength,
                OptionalInt.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(dataPage));
    }

    private static byte[] footer(
            PageVersion version, int rowCount, List<ColumnChunkPlacement> placements, Encoding standIn) {
        List<SchemaElement> schema = new ArrayList<>();
        schema.add(root(placements.size()));
        List<ColumnChunk> chunks = new ArrayList<>();
        long totalBytes = 0;
        for (ColumnChunkPlacement placement : placements) {
            schema.add(leaf(placement.column()));
            chunks.add(chunk(version, rowCount, placement, standIn));
            totalBytes += placement.pageLength();
        }
        RowGroup rowGroup = RowGroup.builder()
                .columns(chunks)
                .totalByteSize(totalBytes)
                .numRows(rowCount)
                .fileOffset(OptionalLong.of(MAGIC.length))
                .totalCompressedSize(OptionalLong.of(totalBytes))
                .ordinal(OptionalInt.of(0))
                .build();
        FileMetaData footer = FileMetaData.builder()
                .version(1)
                .schema(schema)
                .numRows(rowCount)
                .rowGroups(List.of(rowGroup))
                .createdBy(Optional.of("parquetry tests"))
                .build();
        return ParquetFormat.toBytes(footer);
    }

    private static SchemaElement root(int childCount) {
        return new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                "schema",
                OptionalInt.of(childCount),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }

    private static SchemaElement leaf(Column column) {
        FieldRepetitionType repetition =
                column.optional() ? FieldRepetitionType.OPTIONAL : FieldRepetitionType.REQUIRED;
        return new SchemaElement(
                Optional.of(column.type()),
                OptionalInt.empty(),
                Optional.of(repetition),
                column.name(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }

    private static ColumnChunk chunk(
            PageVersion version, int rowCount, ColumnChunkPlacement placement, Encoding standIn) {
        Column column = placement.column();
        Encoding encoding = Encoding.fromCode(column.encodingCode()).orElse(standIn);
        PageType pageType = version == PageVersion.V1 ? PageType.DATA_PAGE : PageType.DATA_PAGE_V2;
        ColumnMetaData metaData = ColumnMetaData.builder()
                .type(column.type())
                .encodings(List.of(Encoding.RLE, encoding))
                .pathInSchema(List.of(column.name()))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(rowCount)
                .totalUncompressedSize(placement.pageLength())
                .totalCompressedSize(placement.pageLength())
                .dataPageOffset(placement.pageOffset())
                .encodingStats(List.of(new EncodingStats(pageType, encoding, 1)))
                .build();
        return ColumnChunk.builder()
                .fileOffset(placement.pageOffset())
                .metaData(Optional.of(metaData))
                .build();
    }

    /** The single wire code unknown to parquetry among the columns, or a known code when there is none. */
    private static int unknownCodeOf(List<Column> columns) {
        List<Integer> unknownCodes = columns.stream()
                .map(Column::encodingCode)
                .filter(code -> Encoding.fromCode(code).isEmpty())
                .distinct()
                .toList();
        if (unknownCodes.size() > 1) {
            throw new IllegalArgumentException("at most one unknown encoding code per file: " + unknownCodes);
        }
        return unknownCodes.isEmpty() ? STAND_IN.value() : unknownCodes.getFirst();
    }

    /**
     * Serializes with the encoding of {@code code}. For a code unknown to parquetry, serializes twice with two stand-in
     * encodings and writes the code at the bytes where the two differ: the encoding fields. Both stand-ins and the code
     * take a single zigzag varint byte, leaving the layout unchanged.
     */
    private static byte[] withEncodingCode(int code, Function<Encoding, byte[]> serializer) {
        Optional<Encoding> known = Encoding.fromCode(code);
        if (known.isPresent()) {
            return serializer.apply(known.get());
        }
        if (code < 0 || code > 63) {
            throw new IllegalArgumentException("unknown code " + code + " does not fit one varint byte");
        }
        byte[] patched = serializer.apply(STAND_IN);
        byte[] second = serializer.apply(SECOND_STAND_IN);
        for (int i = 0; i < patched.length; i++) {
            if (patched[i] != second[i]) {
                patched[i] = (byte) (code << 1);
            }
        }
        return patched;
    }

    private static byte[] serialize(PageHeader header) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(out, header);
        return out.toByteArray();
    }

    private static void writeUnsignedVarint(ByteArrayOutputStream out, int value) {
        int remaining = value;
        while ((remaining & ~0x7F) != 0) {
            out.write((remaining & 0x7F) | 0x80);
            remaining >>>= 7;
        }
        out.write(remaining);
    }

    private static byte[] littleEndianInt(int value) {
        return ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(value)
                .array();
    }
}
