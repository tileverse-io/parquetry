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
package io.tileverse.parquetry.data.read;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.read.page.Dictionary;
import io.tileverse.parquetry.data.read.page.DictionaryDecoder;
import io.tileverse.parquetry.data.read.page.MemorySegmentInputStream;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.DictionaryPageHeader;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Slices one column chunk out of a coalesced range segment and decodes its dictionary, producing a
 * {@link FetchedColumnChunk} view whose {@code compressedSegment} covers just the data-page region.
 */
final class ColumnChunkSlicer {

    private ColumnChunkSlicer() {}

    /**
     * @param chunkSegment a read-only view of the full compressed column chunk (dictionary page if present + all data
     *     pages), sliced out of a coalesced range buffer
     */
    static FetchedColumnChunk slice(
            MemorySegment chunkSegment, ColumnMetaData meta, ColumnPath path, ParquetSchema fileSchema)
            throws IOException {
        DictionaryAndOffset dict = maybeDecodeDictionary(chunkSegment, meta, path);
        MemorySegment dataPages = chunkSegment
                .asSlice(dict.dataPageOffset(), chunkSegment.byteSize() - dict.dataPageOffset())
                .asReadOnly();
        LevelMaxima maxima = fileSchema.maxLevels(path);
        return new FetchedColumnChunk(
                path, meta, maxima.maxRepetitionLevel(), maxima.maxDefinitionLevel(), dataPages, dict.dictionary());
    }

    /** The decoded dictionary (if any) plus the offset of the first data page within the chunk segment. */
    private record DictionaryAndOffset(Optional<Dictionary<?>> dictionary, int dataPageOffset) {}

    /**
     * Reads and decodes the dictionary page at the front of {@code chunkSegment} when {@code meta.dictionaryPageOffset}
     * is set, returning the dictionary and the offset of the byte right after the dictionary page (the first data
     * page). Returns an empty dictionary and offset {@code 0} when no dictionary page is declared.
     *
     * <p>The dictionary is decompressed into a transient Arena segment that is closed before the method returns; the
     * resulting {@link Dictionary} holds heap-resident Java values referenced from the {@link FetchedColumnChunk}.
     */
    private static DictionaryAndOffset maybeDecodeDictionary(
            MemorySegment chunkSegment, ColumnMetaData meta, ColumnPath path) throws IOException {
        if (meta.dictionaryPageOffset().isEmpty()) {
            return new DictionaryAndOffset(Optional.empty(), 0);
        }
        MemorySegmentInputStream stream = new MemorySegmentInputStream(chunkSegment, 0L, chunkSegment.byteSize());
        PageHeader header = readPageHeader(stream, path);
        DictionaryPageHeader dictHeader = header.dictionaryPageHeader()
                .orElseThrow(() -> new ParquetFormatException("Column " + path.dot()
                        + " declares dictionaryPageOffset but its first page is not a dictionary page (type="
                        + header.type() + ")"));
        if (header.type() != PageType.DICTIONARY_PAGE) {
            throw new MalformedFileException(
                    "Column " + path.dot() + " dictionary page header has unexpected type " + header.type());
        }
        long headerEnd = stream.position();
        if (headerEnd <= 0) {
            throw new MalformedFileException("Column " + path.dot() + " dictionary page advanced zero bytes");
        }
        int compressedSize = header.compressedPageSize();
        if (chunkSegment.byteSize() - headerEnd < compressedSize) {
            throw new MalformedFileException("Column " + path.dot() + " dictionary page compressed payload ("
                    + compressedSize + ") overruns the chunk buffer (remaining="
                    + (chunkSegment.byteSize() - headerEnd) + ")");
        }
        MemorySegment compressedPayload =
                chunkSegment.asSlice(headerEnd, compressedSize).asReadOnly();
        Dictionary<?> dictionary =
                decodeDictionary(meta, path, dictHeader, compressedPayload, header.uncompressedPageSize());
        int dataPageOffset = Math.toIntExact(headerEnd + compressedSize);
        return new DictionaryAndOffset(Optional.of(dictionary), dataPageOffset);
    }

    /**
     * Decompresses the dictionary page payload into a transient Arena segment and dispatches to
     * {@link DictionaryDecoder} using the column's {@link PrimitiveKind} and optional {@code typeLength}. The Arena is
     * closed before this method returns.
     */
    private static Dictionary<?> decodeDictionary(
            ColumnMetaData meta,
            ColumnPath path,
            DictionaryPageHeader dictHeader,
            MemorySegment compressedPayload,
            int uncompressedSize)
            throws IOException {
        Compression codec = Compression.forWireCodec(meta.codec());
        PrimitiveKind kind = primitiveKindOf(meta.type(), path);
        OptionalInt typeLength = OptionalInt.empty();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment dst = arena.allocate(uncompressedSize);
            codec.decompress(compressedPayload, dst);
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
     * Reads a {@link PageHeader} from {@code stream}. Wraps the chunk segment as an {@link java.io.InputStream} rather
     * than copying to a {@code byte[]}.
     */
    private static PageHeader readPageHeader(MemorySegmentInputStream stream, ColumnPath path) {
        try {
            return ParquetFormat.readPageHeader(stream);
        } catch (ParquetFormatException e) {
            throw e.withContext("Failed to read dictionary page header for column " + path.dot(), -1L, "PageHeader");
        }
    }
}
