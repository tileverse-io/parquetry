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
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.format.DictionaryPageHeader;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.read.page.DataPageRun;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.internal.read.page.DictionaryDecoder;
import io.tileverse.parquetry.internal.read.page.MemorySegmentInputStream;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * Slices one column chunk's fetched bytes out of the fetch buffer and decodes its dictionary, producing a
 * {@link FetchedColumnChunk} view whose runs cover just the data-page region. A dictionary page is decompressed into a
 * buffer borrowed from the read's {@link SegmentPool} and returned before the chunk is handed out; the decoded
 * {@link Dictionary} holds heap values, and never a view of that buffer. A native allocation per dictionary page would
 * map and unmap memory on the request thread once per dictionary-encoded column and row group.
 */
final class ColumnChunkSlicer {

    private final ParquetSchema fileSchema;
    private final SegmentPool pool;

    ColumnChunkSlicer(@NonNull ParquetSchema fileSchema, @NonNull SegmentPool pool) {
        this.fileSchema = fileSchema;
        this.pool = pool;
    }

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
     * @param runs views of the fetched stretches of the chunk's pages, in file order; never empty
     */
    FetchedColumnChunk slice(
            Optional<MemorySegment> dictionaryPrefix, List<DataPageRun> runs, ChunkMeta meta, ColumnPath path) {
        if (runs.isEmpty()) {
            throw new IllegalStateException("No data-page bytes were fetched for column " + path.dot()
                    + "; every fetched chunk holds at least one run of pages");
        }
        if (dictionaryPrefix.isPresent()) {
            Optional<Dictionary<?>> dictionary = decodeDictionaryPrefix(dictionaryPrefix.orElseThrow(), meta, path);
            return chunkOf(path, meta, runs, dictionary);
        }
        MemorySegment head = runs.get(0).segment();
        DictionaryAndOffset dict = maybeDecodeDictionary(head, meta, path);
        if (dict.dataPageOffset() == 0) {
            return chunkOf(path, meta, runs, dict.dictionary());
        }
        return chunkOf(path, meta, stripDictionaryPage(runs, dict.dataPageOffset()), dict.dictionary());
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

    private FetchedColumnChunk chunkOf(
            ColumnPath path, ChunkMeta meta, List<DataPageRun> runs, Optional<Dictionary<?>> dictionary) {
        LevelMaxima maxima = fileSchema.maxLevels(path);
        return new FetchedColumnChunk(
                path,
                meta.codec(),
                meta.numValues(),
                maxima.maxRepetitionLevel(),
                maxima.maxDefinitionLevel(),
                readOnly(runs),
                dictionary);
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
     * Decodes the dictionary page the fetch planned as its own range. That range spans the chunk's bytes up to its
     * first data page, which can only be a dictionary page (plus any writer padding around it) - a data page there
     * would mean the planner narrowed a chunk whose pages it had mislocated, and decoding on would drop a dictionary
     * the data pages reference.
     */
    private Optional<Dictionary<?>> decodeDictionaryPrefix(MemorySegment prefix, ChunkMeta meta, ColumnPath path) {
        MemorySegmentInputStream stream = new MemorySegmentInputStream(prefix, 0L, prefix.byteSize());
        PageHeader header = readPageHeader(stream, path);
        if (header.type() != PageType.DICTIONARY_PAGE) {
            throw new MalformedFileException(named(path)
                    + " was fetched with a dictionary prefix but the prefix begins with a page of type "
                    + header.type());
        }
        return decodeDictionaryPage(prefix, stream.position(), header, meta, path)
                .dictionary();
    }

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
     * <p>The dictionary is decompressed into a pooled buffer returned before the method returns; the resulting
     * {@link Dictionary} holds heap-resident Java values referenced from the {@link FetchedColumnChunk}.
     */
    private DictionaryAndOffset maybeDecodeDictionary(MemorySegment chunkSegment, ChunkMeta meta, ColumnPath path) {
        if (chunkSegment.byteSize() == 0) {
            return NO_DICTIONARY;
        }
        MemorySegmentInputStream stream = new MemorySegmentInputStream(chunkSegment, 0L, chunkSegment.byteSize());
        PageHeader header = readPageHeader(stream, path);
        if (header.type() != PageType.DICTIONARY_PAGE) {
            if (meta.dictionaryPageOffset().isPresent()) {
                throw new MalformedFileException(named(path)
                        + " declares dictionaryPageOffset but its first page is not a dictionary page (type="
                        + header.type() + ")");
            }
            return NO_DICTIONARY;
        }
        return decodeDictionaryPage(chunkSegment, stream.position(), header, meta, path);
    }

    /**
     * Decodes the dictionary page whose header has already been read, given {@code headerEnd}, the offset of the first
     * payload byte within {@code chunkSegment}.
     */
    private DictionaryAndOffset decodeDictionaryPage(
            MemorySegment chunkSegment, long headerEnd, PageHeader header, ChunkMeta meta, ColumnPath path) {
        DictionaryPageHeader dictHeader = header.dictionaryPageHeader()
                .orElseThrow(() -> new MalformedFileException(
                        named(path) + " dictionary page is missing its dictionary page header"));
        if (headerEnd <= 0) {
            throw new MalformedFileException(named(path) + " dictionary page advanced zero bytes");
        }
        requireNonNegativePageSizes(header, path);
        int compressedSize = header.compressedPageSize();
        if (chunkSegment.byteSize() - headerEnd < compressedSize) {
            throw new MalformedFileException(named(path) + " dictionary page compressed payload ("
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

    private static void requireNonNegativePageSizes(PageHeader header, ColumnPath path) {
        if (header.compressedPageSize() < 0 || header.uncompressedPageSize() < 0) {
            throw new MalformedFileException(named(path) + " dictionary page declares negative sizes:"
                    + " compressed=" + header.compressedPageSize() + ", uncompressed="
                    + header.uncompressedPageSize());
        }
    }

    /**
     * Decompresses the dictionary page payload into a buffer borrowed from the pool and dispatches to
     * {@link DictionaryDecoder} using the column's {@link PrimitiveKind} and optional {@code typeLength}. The buffer
     * returns to the pool before this method returns.
     */
    private Dictionary<?> decodeDictionary(
            ChunkMeta meta,
            ColumnPath path,
            DictionaryPageHeader dictHeader,
            MemorySegment compressedPayload,
            int uncompressedSize) {
        Compression codec = Compression.forWireCodec(meta.codec());
        PrimitiveKind kind = primitiveKindOf(meta.type());
        OptionalInt typeLength = typeLengthFor(kind, path);
        try (SegmentPool.Pooled buffer = pool.borrow(uncompressedSize)) {
            MemorySegment dst = buffer.segment();
            int written = codec.decompress(compressedPayload, dst);
            requireDeclaredSize(written, uncompressedSize);
            return DictionaryDecoder.read(dst, kind, dictHeader.numValues(), typeLength);
        } catch (ParquetFormatException e) {
            throw e.withContext(named(path) + " dictionary page: " + e.getMessage());
        } catch (IOException e) {
            // The bytes were fetched already: a codec failing on them means a corrupt page.
            throw new MalformedFileException(
                    named(path) + " dictionary page cannot be decompressed: " + e.getMessage(), e);
        }
    }

    /**
     * A pooled buffer holds the bytes of its earlier uses, and a page whose header overstates its uncompressed size
     * would be decoded from them. The decoder may read only what the codec wrote.
     */
    private static void requireDeclaredSize(int written, int uncompressedSize) {
        if (written != uncompressedSize) {
            throw new MalformedFileException(
                    "decompressed to " + written + " bytes, header declares " + uncompressedSize);
        }
    }

    /** The name of a column in a failure message. */
    private static String named(ColumnPath path) {
        return "Column " + path.dot();
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
    private OptionalInt typeLengthFor(PrimitiveKind kind, ColumnPath path) {
        if (kind != PrimitiveKind.FIXED_LEN_BYTE_ARRAY) {
            return OptionalInt.empty();
        }
        SchemaNode node = fileSchema
                .find(path)
                .orElseThrow(() ->
                        new MalformedFileException(named(path) + " is dictionary-encoded but absent from the schema"));
        if (!(node instanceof SchemaNode.Primitive primitive)) {
            throw new MalformedFileException(named(path) + " is not a primitive leaf in the schema");
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
            throw e.withContext(
                    "Failed to read dictionary page header for column " + path.dot() + ": " + e.getMessage(),
                    -1L,
                    "PageHeader");
        }
    }
}
