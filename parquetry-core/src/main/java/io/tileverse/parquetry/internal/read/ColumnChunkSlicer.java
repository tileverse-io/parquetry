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
package io.tileverse.parquetry.internal.read;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.DictionaryPageHeader;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.internal.read.page.DataPageRun;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.internal.read.page.DictionaryDecoder;
import io.tileverse.parquetry.internal.read.page.MemorySegmentInputStream;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Slices one column chunk's fetched bytes out of coalesced range segments and decodes its dictionary, producing a
 * {@link FetchedColumnChunk} view whose runs cover just the data-page region.
 */
final class ColumnChunkSlicer {

    private ColumnChunkSlicer() {}

    /**
     * Builds the chunk view from the fetched pieces of one column chunk.
     *
     * <p>The dictionary comes from {@code dictionaryPrefix} when the fetch planned the chunk's leading bytes as their
     * own range; the prefix holds the chunk's true first bytes (the dictionary page, plus any writer padding around
     * it), which keeps the first-page-header sniffing intact. Bytes left in the prefix after the dictionary page are
     * ignored. With no prefix, the head of the first run is sniffed instead, which is the whole-chunk shape: one run
     * starting at the chunk's first byte.
     *
     * @param dictionaryPrefix view of the chunk's bytes up to its first data page, when fetched separately
     * @param runs views of the fetched stretches of the chunk's pages, in file order
     */
    static FetchedColumnChunk slice(
            Optional<MemorySegment> dictionaryPrefix,
            List<DataPageRun> runs,
            ColumnMetaData meta,
            ColumnPath path,
            ParquetSchema fileSchema)
            throws IOException {
        if (dictionaryPrefix.isPresent()) {
            DictionaryAndOffset dict = maybeDecodeDictionary(dictionaryPrefix.orElseThrow(), meta, path, fileSchema);
            return chunkOf(path, meta, fileSchema, runs, dict.dictionary());
        }
        if (runs.isEmpty()) {
            return chunkOf(path, meta, fileSchema, runs, Optional.empty());
        }
        MemorySegment head = runs.get(0).segment();
        DictionaryAndOffset dict = maybeDecodeDictionary(head, meta, path, fileSchema);
        if (dict.dataPageOffset() == 0) {
            return chunkOf(path, meta, fileSchema, runs, dict.dictionary());
        }
        return chunkOf(path, meta, fileSchema, stripDictionaryPage(runs, dict.dataPageOffset()), dict.dictionary());
    }

    /**
     * @param chunkSegment a read-only view of the full compressed column chunk (dictionary page if present + all data
     *     pages), sliced out of a coalesced range buffer
     */
    static FetchedColumnChunk slice(
            MemorySegment chunkSegment, ColumnMetaData meta, ColumnPath path, ParquetSchema fileSchema)
            throws IOException {
        return slice(Optional.empty(), List.of(new DataPageRun(chunkSegment, 0)), meta, path, fileSchema);
    }

    /** Drops the leading {@code dataPageOffset} bytes the dictionary page occupies at the head of the first run. */
    private static List<DataPageRun> stripDictionaryPage(List<DataPageRun> runs, int dataPageOffset) {
        MemorySegment head = runs.get(0).segment();
        MemorySegment dataPages = head.asSlice(dataPageOffset, head.byteSize() - dataPageOffset);
        List<DataPageRun> stripped = new ArrayList<>(runs.size());
        stripped.add(new DataPageRun(dataPages, runs.get(0).firstPageOrdinal()));
        stripped.addAll(runs.subList(1, runs.size()));
        return stripped;
    }

    private static FetchedColumnChunk chunkOf(
            ColumnPath path,
            ColumnMetaData meta,
            ParquetSchema fileSchema,
            List<DataPageRun> runs,
            Optional<Dictionary<?>> dictionary) {
        LevelMaxima maxima = fileSchema.maxLevels(path);
        return new FetchedColumnChunk(
                path, meta, maxima.maxRepetitionLevel(), maxima.maxDefinitionLevel(), readOnly(runs), dictionary);
    }

    /**
     * A fetched chunk must not be able to write through to the pooled range buffer it views, and callers hand over
     * plain slices of that buffer.
     */
    private static List<DataPageRun> readOnly(List<DataPageRun> runs) {
        List<DataPageRun> views = new ArrayList<>(runs.size());
        for (DataPageRun run : runs) {
            views.add(new DataPageRun(run.segment().asReadOnly(), run.firstPageOrdinal()));
        }
        return views;
    }

    /** The decoded dictionary (if any) plus the offset of the first data page within the chunk segment. */
    private record DictionaryAndOffset(Optional<Dictionary<?>> dictionary, int dataPageOffset) {}

    private static final DictionaryAndOffset NO_DICTIONARY = new DictionaryAndOffset(Optional.empty(), 0);

    /**
     * Decodes the dictionary page when one sits at the front of {@code chunkSegment}, returning the dictionary and the
     * offset of the byte right after it (the first data page).
     *
     * <p>A dictionary page is the first page in a column chunk whenever the column is dictionary-encoded. The
     * {@code dictionary_page_offset} field that would point at it is optional: writers such as Impala and the Hadoop
     * LZ4 fixtures omit it and instead point {@code data_page_offset} at the dictionary page itself. The chunk
     * therefore always begins at the dictionary page when one exists. This method inspects the first page header
     * directly rather than trusting {@code dictionary_page_offset}; when the first page is a data page, the chunk has
     * no dictionary and this returns {@link #NO_DICTIONARY}.
     *
     * <p>The dictionary is decompressed into a transient Arena segment that is closed before the method returns; the
     * resulting {@link Dictionary} holds heap-resident Java values referenced from the {@link FetchedColumnChunk}.
     */
    private static DictionaryAndOffset maybeDecodeDictionary(
            MemorySegment chunkSegment, ColumnMetaData meta, ColumnPath path, ParquetSchema fileSchema)
            throws IOException {
        if (chunkSegment.byteSize() == 0) {
            return NO_DICTIONARY;
        }
        MemorySegmentInputStream stream = new MemorySegmentInputStream(chunkSegment, 0L, chunkSegment.byteSize());
        PageHeader header = readPageHeader(stream, path);
        if (header.type() != PageType.DICTIONARY_PAGE) {
            if (declaresRealDictionaryPage(meta)) {
                throw new MalformedFileException("Column " + path.dot()
                        + " declares dictionaryPageOffset but its first page is not a dictionary page (type="
                        + header.type() + ")");
            }
            return NO_DICTIONARY;
        }
        return decodeDictionaryPage(chunkSegment, stream.position(), header, meta, path, fileSchema);
    }

    /**
     * Whether {@code dictionary_page_offset} points at a real dictionary page. Some writers store a literal {@code 0}
     * for columns that have no dictionary; that value would point at the file's magic header rather than a page, and
     * does not count as a declared dictionary page.
     */
    private static boolean declaresRealDictionaryPage(ColumnMetaData meta) {
        return meta.dictionaryPageOffset().orElse(0L) > 0;
    }

    /**
     * Decodes the dictionary page whose header has already been read, given {@code headerEnd}, the offset of the first
     * payload byte within {@code chunkSegment}.
     */
    private static DictionaryAndOffset decodeDictionaryPage(
            MemorySegment chunkSegment,
            long headerEnd,
            PageHeader header,
            ColumnMetaData meta,
            ColumnPath path,
            ParquetSchema fileSchema)
            throws IOException {
        DictionaryPageHeader dictHeader = header.dictionaryPageHeader()
                .orElseThrow(() -> new MalformedFileException(
                        "Column " + path.dot() + " dictionary page is missing its dictionary page header"));
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
                decodeDictionary(meta, path, fileSchema, dictHeader, compressedPayload, header.uncompressedPageSize());
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
            ParquetSchema fileSchema,
            DictionaryPageHeader dictHeader,
            MemorySegment compressedPayload,
            int uncompressedSize)
            throws IOException {
        Compression codec = Compression.forWireCodec(meta.codec());
        PrimitiveKind kind = primitiveKindOf(meta.type());
        OptionalInt typeLength = typeLengthFor(kind, path, fileSchema);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment dst = arena.allocate(uncompressedSize);
            codec.decompress(compressedPayload, dst);
            return DictionaryDecoder.read(dst, kind, dictHeader.numValues(), typeLength);
        }
    }

    private static PrimitiveKind primitiveKindOf(PhysicalType type) {
        return switch (type) {
            case BOOLEAN -> PrimitiveKind.BOOLEAN;
            case INT32 -> PrimitiveKind.INT32;
            case INT64 -> PrimitiveKind.INT64;
            case INT96 -> PrimitiveKind.INT96;
            case FLOAT -> PrimitiveKind.FLOAT;
            case DOUBLE -> PrimitiveKind.DOUBLE;
            case BYTE_ARRAY -> PrimitiveKind.BYTE_ARRAY;
            case FIXED_LEN_BYTE_ARRAY -> PrimitiveKind.FIXED_LEN_BYTE_ARRAY;
        };
    }

    /**
     * The fixed byte width a {@link PrimitiveKind#FIXED_LEN_BYTE_ARRAY} dictionary needs to split its payload into
     * values. The dictionary page header does not record it; it comes from the schema leaf instead. Every other kind
     * has a self-describing layout and needs no length.
     */
    private static OptionalInt typeLengthFor(PrimitiveKind kind, ColumnPath path, ParquetSchema fileSchema) {
        if (kind != PrimitiveKind.FIXED_LEN_BYTE_ARRAY) {
            return OptionalInt.empty();
        }
        SchemaNode node = fileSchema
                .find(path)
                .orElseThrow(() -> new MalformedFileException(
                        "Column " + path.dot() + " is dictionary-encoded but absent from the schema"));
        if (!(node instanceof SchemaNode.Primitive primitive)) {
            throw new MalformedFileException("Column " + path.dot() + " is not a primitive leaf in the schema");
        }
        return primitive.typeLength();
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
