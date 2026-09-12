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
package io.tileverse.parquetry.internal.footer;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.PhysicalType;

/**
 * One column chunk of a {@link CompactFooter}: where its pages, page indexes, and bloom filter live in the file, how
 * many values it holds, how those values are typed and compressed, and the statistics used by a filter to prune.
 *
 * <p>Every accessor reads the blob at a fixed offset from the chunk's record. An instance is therefore a window rather
 * than a copy: creating one allocates nothing beyond the window itself, and the reported values stay valid for as long
 * as the {@link CompactFooter} lives. Obtain one from {@link CompactFooter#chunk(int, int)} or
 * {@link CompactFooter#chunkIfPresent(int, int)}; both hand out a window only for a row group that holds a chunk for
 * the leaf.
 */
public final class ChunkMeta {

    private final CompactFooter footer;
    private final int chunkIndex;

    ChunkMeta(CompactFooter footer, int chunkIndex) {
        this.footer = Objects.requireNonNull(footer, "footer");
        this.chunkIndex = chunkIndex;
    }

    /** File offset of the chunk's first data page. */
    public long dataPageOffset() {
        return footer.longAt(base() + CompactFooter.CHUNK_DATA_PAGE_OFFSET);
    }

    /** File offset of the chunk's dictionary page, empty when the chunk is not dictionary-encoded. */
    public OptionalLong dictionaryPageOffset() {
        long offset = footer.longAt(base() + CompactFooter.CHUNK_DICTIONARY_PAGE_OFFSET);
        return offset == CompactFooter.NO_DICTIONARY_PAGE ? OptionalLong.empty() : OptionalLong.of(offset);
    }

    /**
     * Compressed byte size of every page in the chunk, page headers included. A caller taking this as a fetch length
     * tests {@link #compressedSizeBeyondRange()} first: the lane holds a placeholder for a wire size beyond its range.
     */
    public int totalCompressedSize() {
        return footer.intAt(base() + CompactFooter.CHUNK_TOTAL_COMPRESSED_SIZE);
    }

    /**
     * Whether the footer records a compressed size beyond the range that this form holds, which leaves the chunk
     * unfetchable. A caller knowing the column's name tests this before using the size as a fetch length, which lets
     * the failure name the column.
     */
    public boolean compressedSizeBeyondRange() {
        return totalCompressedSize() == CompactFooter.SIZE_BEYOND_RANGE;
    }

    /** Number of values in the chunk, nulls included. */
    public long numValues() {
        return footer.longAt(base() + CompactFooter.CHUNK_NUM_VALUES);
    }

    /** Compression applied to the chunk's page payloads. */
    public CompressionCodec codec() {
        return CompressionCodec.valueOf(footer.byteAt(base() + CompactFooter.CHUNK_CODEC));
    }

    /** On-disk storage type of the chunk's values. */
    public PhysicalType type() {
        return PhysicalType.valueOf(footer.byteAt(base() + CompactFooter.CHUNK_TYPE));
    }

    /** File offset of the chunk's column index, empty when the footer does not locate one. */
    public OptionalLong columnIndexOffset() {
        return longOrAbsent(CompactFooter.CHUNK_COLUMN_INDEX_OFFSET);
    }

    /** Byte length of the chunk's column index; meaningful only when {@link #columnIndexOffset()} is present. */
    public int columnIndexLength() {
        return footer.intAt(base() + CompactFooter.CHUNK_COLUMN_INDEX_LENGTH);
    }

    /** File offset of the chunk's offset index, empty when the footer does not locate one. */
    public OptionalLong offsetIndexOffset() {
        return longOrAbsent(CompactFooter.CHUNK_OFFSET_INDEX_OFFSET);
    }

    /** Byte length of the chunk's offset index; meaningful only when {@link #offsetIndexOffset()} is present. */
    public int offsetIndexLength() {
        return footer.intAt(base() + CompactFooter.CHUNK_OFFSET_INDEX_LENGTH);
    }

    /** File offset of the chunk's bloom filter, empty when no bloom filter was written. */
    public OptionalLong bloomFilterOffset() {
        return longOrAbsent(CompactFooter.CHUNK_BLOOM_FILTER_OFFSET);
    }

    /** Byte length of the chunk's bloom filter, empty when the writer recorded the offset but no length. */
    public OptionalInt bloomFilterLength() {
        int length = footer.intAt(base() + CompactFooter.CHUNK_BLOOM_FILTER_LENGTH);
        return length == CompactFooter.ABSENT_INT ? OptionalInt.empty() : OptionalInt.of(length);
    }

    /** Number of null values in the chunk, empty when the writer recorded no statistics or no null count. */
    public OptionalLong nullCount() {
        return longOrAbsent(CompactFooter.CHUNK_NULL_COUNT);
    }

    /** PLAIN-encoded minimum value of the chunk, empty when the writer recorded none. */
    public Optional<MemorySegment> minValue() {
        return statisticValue(CompactFooter.CHUNK_MIN_OFFSET, CompactFooter.CHUNK_MIN_LENGTH);
    }

    /** PLAIN-encoded maximum value of the chunk, empty when the writer recorded none. */
    public Optional<MemorySegment> maxValue() {
        return statisticValue(CompactFooter.CHUNK_MAX_OFFSET, CompactFooter.CHUNK_MAX_LENGTH);
    }

    /**
     * The bounding box of every geometry in the chunk, empty when the column has no native geospatial statistics. Each
     * of the four optional halves stands on its own, as it does on the wire. The record is rebuilt from the blob's
     * lanes on every call rather than retained.
     */
    public Optional<BoundingBox> geoExtent() {
        byte flags = flags();
        if ((flags & CompactFooter.FLAG_HAS_GEO_BBOX) == 0) {
            return Optional.empty();
        }
        long entry = footer.geoEntryBase(footer.intAt(base() + CompactFooter.CHUNK_GEO_INDEX));
        return Optional.of(new BoundingBox(
                footer.doubleAt(entry + CompactFooter.GEO_XMIN),
                footer.doubleAt(entry + CompactFooter.GEO_XMAX),
                footer.doubleAt(entry + CompactFooter.GEO_YMIN),
                footer.doubleAt(entry + CompactFooter.GEO_YMAX),
                optionalExtent(flags, CompactFooter.FLAG_HAS_GEO_ZMIN, entry + CompactFooter.GEO_ZMIN),
                optionalExtent(flags, CompactFooter.FLAG_HAS_GEO_ZMAX, entry + CompactFooter.GEO_ZMAX),
                optionalExtent(flags, CompactFooter.FLAG_HAS_GEO_MMIN, entry + CompactFooter.GEO_MMIN),
                optionalExtent(flags, CompactFooter.FLAG_HAS_GEO_MMAX, entry + CompactFooter.GEO_MMAX)));
    }

    private OptionalDouble optionalExtent(byte flags, int flag, long lane) {
        if ((flags & flag) == 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(footer.doubleAt(lane));
    }

    private byte flags() {
        return footer.byteAt(base() + CompactFooter.CHUNK_FLAGS);
    }

    /**
     * The byte offset where the chunk begins. A dictionary page offset only points at a real dictionary page when it
     * precedes the first data page; some writers leave it unset or store a literal zero, which would otherwise point at
     * the file's magic header. The chunk then starts at its first data page. This mirrors parquet-mr's
     * {@code getStartingPos}.
     */
    public long chunkStart() {
        long dataPageOffset = dataPageOffset();
        long dictionaryPageOffset = footer.longAt(base() + CompactFooter.CHUNK_DICTIONARY_PAGE_OFFSET);
        boolean dictionaryPagePrecedesData = dictionaryPageOffset > 0 && dictionaryPageOffset < dataPageOffset;
        return dictionaryPagePrecedesData ? dictionaryPageOffset : dataPageOffset;
    }

    private long base() {
        return footer.chunkRecordBase(chunkIndex);
    }

    private OptionalLong longOrAbsent(int field) {
        long value = footer.longAt(base() + field);
        return value == CompactFooter.ABSENT_LONG ? OptionalLong.empty() : OptionalLong.of(value);
    }

    private Optional<MemorySegment> statisticValue(int offsetField, int lengthField) {
        int offset = footer.intAt(base() + offsetField);
        if (offset == CompactFooter.ABSENT_INT) {
            return Optional.empty();
        }
        return Optional.of(footer.arenaSlice(offset, footer.intAt(base() + lengthField)));
    }
}
