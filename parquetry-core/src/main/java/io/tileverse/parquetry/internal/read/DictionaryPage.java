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
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.internal.read.page.DictionaryDecoder;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

import lombok.NonNull;

/**
 * The dictionary page of a column chunk. It holds a view of the page's compressed payload and what decoding the page
 * takes. The view reads the fetch buffer of the row group, and that buffer outlives the readers of the row group. A
 * column reader can therefore decode the page for the first data page needing its values.
 *
 * <p>A reader asking for the dictionary records the column as used by the read, as does a fetch reading the page as its
 * own prefix. The fetch then decodes the pages of that column ahead, and {@link #decode} hands that dictionary back.
 * See {@link ColumnChunkSlicer}.
 */
final class DictionaryPage {

    private final MemorySegment compressedPayload;
    private final int uncompressedSize;
    private final int valueCount;
    private final Compression codec;
    private final PrimitiveKind kind;
    private final OptionalInt typeLength;
    private final ColumnPath path;
    private final Set<ColumnPath> columnsUsingDictionary;
    // A fetch thread sets this field and a decode thread reads it. A final field needs no lock for that.
    private final Optional<Dictionary<?>> decodedAhead;

    /**
     * @param compressedPayload a read-only view of the page's bytes after its header
     * @param uncompressedSize the decompressed size declared by the page header
     * @param valueCount the value count declared by the dictionary page header
     * @param codec the codec of the column chunk
     * @param kind the primitive kind of the column
     * @param typeLength the byte width of a {@link PrimitiveKind#FIXED_LEN_BYTE_ARRAY} column, otherwise empty
     * @param path the column, named by a failure
     * @param columnsUsingDictionary the columns with a dictionary used by the read, safe for concurrent use
     */
    @SuppressWarnings("java:S107") // the inputs of the decode plus the set of the read; a parameter object would only
    // relocate the arity
    DictionaryPage(
            @NonNull MemorySegment compressedPayload,
            int uncompressedSize,
            int valueCount,
            @NonNull Compression codec,
            @NonNull PrimitiveKind kind,
            @NonNull OptionalInt typeLength,
            @NonNull ColumnPath path,
            @NonNull Set<ColumnPath> columnsUsingDictionary) {
        this.compressedPayload = compressedPayload;
        this.uncompressedSize = uncompressedSize;
        this.valueCount = valueCount;
        this.codec = codec;
        this.kind = kind;
        this.typeLength = typeLength;
        this.path = path;
        this.columnsUsingDictionary = columnsUsingDictionary;
        this.decodedAhead = Optional.empty();
    }

    private DictionaryPage(DictionaryPage undecoded, Dictionary<?> dictionary) {
        this.compressedPayload = undecoded.compressedPayload;
        this.uncompressedSize = undecoded.uncompressedSize;
        this.valueCount = undecoded.valueCount;
        this.codec = undecoded.codec;
        this.kind = undecoded.kind;
        this.typeLength = undecoded.typeLength;
        this.path = undecoded.path;
        this.columnsUsingDictionary = undecoded.columnsUsingDictionary;
        this.decodedAhead = Optional.of(dictionary);
    }

    /**
     * The {@link Dictionary} of this page, of heap values, for a reader needing its values. This records the column as
     * used by the read. A page decoded ahead hands back its dictionary. Any other page decodes here. It decompresses
     * into a buffer of a page arena, and that buffer returns to the pool before this method returns. The dictionary
     * holds no view of it. A page arena borrows from the pool and never maps a file, because a mapping per dictionary
     * would cost more than the decode.
     *
     * @throws MalformedFileException when the codec rejects the payload, or when the decompressed bytes disagree with
     *     the page header
     */
    // The wildcard return is intentional: the concrete element type is chosen at runtime by PrimitiveKind.
    @SuppressWarnings("java:S1452")
    Dictionary<?> decode(DecodeBufferAllocator allocator) {
        columnsUsingDictionary.add(path);
        if (decodedAhead.isPresent()) {
            return decodedAhead.orElseThrow();
        }
        try (Arena scratch = allocator.newPageArena()) {
            return decodeInto(scratch.allocate(uncompressedSize));
        } catch (ParquetFormatException e) {
            throw e.withContext(named() + " dictionary page: " + e.getMessage());
        } catch (IOException e) {
            // The bytes were fetched already: a codec failing on them means a corrupt page.
            throw new MalformedFileException(named() + " dictionary page cannot be decompressed: " + e.getMessage(), e);
        }
    }

    /**
     * A page holding its dictionary, decoded on the fetch thread ahead of the readers. The page decompresses into a
     * buffer borrowed from {@code pool}, because a fetch holding a buffer never waits for memory and a borrow never
     * waits. That buffer returns to the pool before this method returns.
     *
     * <p>A decode ahead never fails the fetch, because a corrupt page fails only a read reaching a data page using it.
     * When the decode fails the result is this page, and {@link #decode} raises the failure for such a read.
     */
    DictionaryPage decodeAhead(SegmentPool pool) {
        try (SegmentPool.Pooled scratch = pool.borrow(uncompressedSize)) {
            return new DictionaryPage(this, decodeInto(scratch.segment()));
        } catch (RuntimeException | IOException _) {
            // The read may never reach a page using this dictionary, and a failure of any kind stays out of its fetch.
            // A reader needing the dictionary decodes the page again and raises the same failure.
            return this;
        }
    }

    /** Decompresses the page into {@code scratch} and reads its entries as heap values. */
    private Dictionary<?> decodeInto(MemorySegment scratch) throws IOException {
        int written = codec.decompress(compressedPayload, scratch);
        requireDeclaredSize(written);
        return DictionaryDecoder.read(scratch, kind, valueCount, typeLength);
    }

    /**
     * A pooled buffer holds the bytes of its earlier uses, and a page whose header overstates its uncompressed size
     * would be decoded from them. The decoder may read only what the codec wrote.
     */
    private void requireDeclaredSize(int written) {
        if (written != uncompressedSize) {
            throw new MalformedFileException(
                    "decompressed to " + written + " bytes, header declares " + uncompressedSize);
        }
    }

    /** The name of the column in a failure message. */
    private String named() {
        return "Column " + path.dot();
    }
}
