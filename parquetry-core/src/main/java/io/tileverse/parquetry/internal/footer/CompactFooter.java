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

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * A whole Parquet footer reduced to the values used in planning a read, packed into a single read-only
 * {@link MemorySegment} over a heap {@code byte[]}.
 *
 * <p>One blob replaces the tree of wire records otherwise retained by a parsed footer for as long as a file stays open:
 * a lane per row group, a fixed-width record per (row group, leaf column) pair, a sparse section for the chunks with
 * native geospatial bounds, and an arena holding the statistics min/max bytes verbatim. Every read is offset
 * arithmetic, and {@link #chunk(int, int)} hands out a {@link ChunkMeta} window rather than a rehydrated object.
 *
 * <p>Deliberately dropped: encodings, encoding statistics, size statistics, column key/value metadata, sorting columns,
 * the deprecated {@code file_offset}, crypto metadata, and the statistics distinct count and exactness flags. Nothing
 * on the planning or scan path reads them.
 *
 * <h2>Layout</h2>
 *
 * <p>Every numeric lane is little-endian and read through an unaligned layout, which a segment over a heap
 * {@code byte[]} requires. A 40-byte header names the section offsets, followed in order by the row-group lanes, the
 * chunk table (addressed by {@code rowGroup * leafCount + leaf}), the geo section, and the arena, which runs to the end
 * of the segment. Every record size below is a multiple of eight, which puts each eight-byte lane on an eight-byte
 * boundary.
 */
public final class CompactFooter {

    private static final int HEADER_BYTES = 40;
    private static final int HEADER_ROW_GROUP_COUNT = 0;
    private static final int HEADER_LEAF_COUNT = 4;
    private static final int HEADER_ROW_GROUP_SECTION = 8;
    private static final int HEADER_CHUNK_TABLE = 16;
    private static final int HEADER_GEO_SECTION = 24;
    private static final int HEADER_ARENA = 32;

    private static final int ROW_GROUP_RECORD_BYTES = 8;
    private static final int ROW_GROUP_NUM_ROWS = 0;

    static final int CHUNK_RECORD_BYTES = 96;
    static final int CHUNK_DATA_PAGE_OFFSET = 0;
    static final int CHUNK_DICTIONARY_PAGE_OFFSET = 8;
    static final int CHUNK_NUM_VALUES = 16;
    static final int CHUNK_NULL_COUNT = 24;
    static final int CHUNK_COLUMN_INDEX_OFFSET = 32;
    static final int CHUNK_OFFSET_INDEX_OFFSET = 40;
    static final int CHUNK_BLOOM_FILTER_OFFSET = 48;
    static final int CHUNK_TOTAL_COMPRESSED_SIZE = 56;
    static final int CHUNK_COLUMN_INDEX_LENGTH = 60;
    static final int CHUNK_OFFSET_INDEX_LENGTH = 64;
    static final int CHUNK_BLOOM_FILTER_LENGTH = 68;
    static final int CHUNK_MIN_OFFSET = 72;
    static final int CHUNK_MIN_LENGTH = 76;
    static final int CHUNK_MAX_OFFSET = 80;
    static final int CHUNK_MAX_LENGTH = 84;
    static final int CHUNK_CODEC = 88;
    static final int CHUNK_TYPE = 89;
    static final int CHUNK_FLAGS = 90;
    static final int CHUNK_GEO_INDEX = 92;

    static final int FLAG_CHUNK_PRESENT = 0x1;
    static final int FLAG_HAS_GEO_BBOX = 0x2;
    static final int FLAG_HAS_GEO_ZMIN = 0x4;
    static final int FLAG_HAS_GEO_ZMAX = 0x8;
    static final int FLAG_HAS_GEO_MMIN = 0x10;
    static final int FLAG_HAS_GEO_MMAX = 0x20;

    private static final int GEO_ENTRY_BYTES = 64;
    static final int GEO_XMIN = 0;
    static final int GEO_XMAX = 8;
    static final int GEO_YMIN = 16;
    static final int GEO_YMAX = 24;
    static final int GEO_ZMIN = 32;
    static final int GEO_ZMAX = 40;
    static final int GEO_MMIN = 48;
    static final int GEO_MMAX = 56;

    /** Written into a long lane whose value the footer does not record. */
    static final long ABSENT_LONG = -1L;

    /** Written into an int lane whose value the footer does not record. */
    static final int ABSENT_INT = -1;

    /** A dictionary page offset of zero means the chunk has none; no writer puts one at the file's magic header. */
    static final long NO_DICTIONARY_PAGE = 0L;

    /**
     * Written into the compressed-size lane for a wire size beyond the lane's range. A legal chunk spans at least one
     * byte, hence no real size collides with it. The chunk keeps its slot and every other column of the row group stays
     * readable; only a fetch of this one column fails, and only once it is planned.
     */
    static final int SIZE_BEYOND_RANGE = -1;

    private final MemorySegment blob;
    private final int rowGroupCount;
    private final int leafCount;
    private final long rowGroupSectionOffset;
    private final long chunkTableOffset;
    private final long geoSectionOffset;
    private final long arenaOffset;

    private CompactFooter(MemorySegment filled) {
        MemorySegment readOnly = filled.asReadOnly();
        this.blob = readOnly;
        this.rowGroupCount = readOnly.get(INT32, HEADER_ROW_GROUP_COUNT);
        this.leafCount = readOnly.get(INT32, HEADER_LEAF_COUNT);
        this.rowGroupSectionOffset = readOnly.get(INT64, HEADER_ROW_GROUP_SECTION);
        this.chunkTableOffset = readOnly.get(INT64, HEADER_CHUNK_TABLE);
        this.geoSectionOffset = readOnly.get(INT64, HEADER_GEO_SECTION);
        this.arenaOffset = readOnly.get(INT64, HEADER_ARENA);
    }

    /**
     * Packs {@code footer} into the compact planning form, placing each column chunk at the ordinal that {@code leaves}
     * gives its path.
     *
     * <p>Encoding fails only for a footer that no reader could plan against at all. A single unusable value - an
     * unaddressable chunk length, a bloom-filter length beyond its lane - is recorded as such and costs that one
     * column, never the whole file.
     *
     * @throws ParquetFormatException when a chunk names a path not declared by the schema, when one row group holds two
     *     chunks for the same leaf, when a codec or physical type has a wire code beyond its lane, or when the whole
     *     blob would exceed the addressable size
     */
    public static CompactFooter encode(FileMetaData footer, LeafIndex leaves) {
        List<RowGroup> rowGroups = footer.rowGroups();
        SectionSizes sizes = measureSections(rowGroups);
        return new CompactFooter(new BlobWriter(rowGroups.size(), leaves, sizes).write(rowGroups));
    }

    /** The number of row groups in the encoded footer. */
    public int rowGroupCount() {
        return rowGroupCount;
    }

    /** The number of leaf columns in the file schema; the width of the chunk table. */
    public int leafCount() {
        return leafCount;
    }

    /** The row count of row group {@code rowGroup}. */
    public long numRows(int rowGroup) {
        return blob.get(INT64, rowGroupLaneBase(rowGroup) + ROW_GROUP_NUM_ROWS);
    }

    /**
     * A window onto the chunk of leaf column {@code leaf} within row group {@code rowGroup}. The blob keeps no column
     * names; the failure below identifies the column by ordinal, and a caller holding the {@link LeafIndex} can name
     * it.
     *
     * @throws ParquetFormatException when the row group has no chunk for that leaf
     * @throws IndexOutOfBoundsException when either coordinate falls outside the encoded footer
     */
    public ChunkMeta chunk(int rowGroup, int leaf) {
        return chunkIfPresent(rowGroup, leaf)
                .orElseThrow(() -> new ParquetFormatException(
                        "Row group " + rowGroup + " does not contain the column at leaf ordinal " + leaf));
    }

    /**
     * The same window as {@link #chunk(int, int)}, empty rather than failing when the row group has no chunk for that
     * leaf. A caller that treats an absent column as a legitimate answer asks through this one.
     *
     * @throws IndexOutOfBoundsException when either coordinate falls outside the encoded footer
     */
    public Optional<ChunkMeta> chunkIfPresent(int rowGroup, int leaf) {
        int chunkIndex = requireChunkIndex(rowGroup, leaf);
        byte flags = blob.get(JAVA_BYTE, chunkRecordBase(chunkIndex) + CHUNK_FLAGS);
        if ((flags & FLAG_CHUNK_PRESENT) == 0) {
            return Optional.empty();
        }
        return Optional.of(new ChunkMeta(this, chunkIndex));
    }

    /**
     * How many column chunks of this footer record a native geospatial extent, across every row group and every leaf
     * column. Zero for a file whose writer recorded no geospatial statistics. Answered from the section offsets in the
     * header, which rules a whole file out of the native bounds tier without a walk of the chunk table.
     */
    public int geoExtentCount() {
        return (int) ((arenaOffset - geoSectionOffset) / GEO_ENTRY_BYTES);
    }

    /**
     * Whether the chunk of leaf column {@code leaf} in row group {@code rowGroup} records a native geospatial extent.
     * Answered from the chunk's flags alone, which lets a caller rule a column out before building anything per row
     * group. An absent chunk records none.
     *
     * @throws IndexOutOfBoundsException when either coordinate falls outside the encoded footer
     */
    public boolean recordsGeoExtent(int rowGroup, int leaf) {
        long base = chunkRecordBase(requireChunkIndex(rowGroup, leaf));
        return (blob.get(JAVA_BYTE, base + CHUNK_FLAGS) & FLAG_HAS_GEO_BBOX) != 0;
    }

    /** The blob's byte size, which is the weight to charge a cache holding this footer. */
    public long byteSize() {
        return blob.byteSize();
    }

    private long rowGroupLaneBase(int rowGroup) {
        return rowGroupSectionOffset + (long) requireRowGroup(rowGroup) * ROW_GROUP_RECORD_BYTES;
    }

    private int requireChunkIndex(int rowGroup, int leaf) {
        int validRowGroup = requireRowGroup(rowGroup);
        if (leaf < 0 || leaf >= leafCount) {
            throw new IndexOutOfBoundsException(
                    "Leaf ordinal " + leaf + " is outside the " + leafCount + " leaf columns of this footer");
        }
        return validRowGroup * leafCount + leaf;
    }

    private int requireRowGroup(int rowGroup) {
        if (rowGroup < 0 || rowGroup >= rowGroupCount) {
            throw new IndexOutOfBoundsException(
                    "Row group " + rowGroup + " is outside the " + rowGroupCount + " row groups of this footer");
        }
        return rowGroup;
    }

    // --- windows onto the blob, read by ChunkMeta ---

    long chunkRecordBase(int chunkIndex) {
        return chunkTableOffset + (long) chunkIndex * CHUNK_RECORD_BYTES;
    }

    long geoEntryBase(int geoIndex) {
        return geoSectionOffset + (long) geoIndex * GEO_ENTRY_BYTES;
    }

    long longAt(long offset) {
        return blob.get(INT64, offset);
    }

    int intAt(long offset) {
        return blob.get(INT32, offset);
    }

    byte byteAt(long offset) {
        return blob.get(JAVA_BYTE, offset);
    }

    double doubleAt(long offset) {
        return blob.get(DOUBLE, offset);
    }

    MemorySegment arenaSlice(int offset, int length) {
        return blob.asSlice(arenaOffset + offset, length);
    }

    // --- encoding ---

    /**
     * The variable-length sections needed by a footer, measured before the blob is allocated.
     *
     * @param arenaBytes total statistics min/max bytes across every chunk
     * @param geoEntries number of chunks with a native geospatial bounding box
     */
    private record SectionSizes(long arenaBytes, int geoEntries) {}

    private static SectionSizes measureSections(List<RowGroup> rowGroups) {
        long arenaBytes = 0L;
        int geoEntries = 0;
        for (RowGroup rowGroup : rowGroups) {
            for (ColumnChunk chunk : rowGroup.columns()) {
                Optional<ColumnMetaData> chunkMeta = chunk.metaData();
                if (chunkMeta.isEmpty()) {
                    continue;
                }
                ColumnMetaData meta = chunkMeta.orElseThrow();
                arenaBytes += statisticsBytes(meta);
                if (geoBoundingBox(meta).isPresent()) {
                    geoEntries++;
                }
            }
        }
        return new SectionSizes(arenaBytes, geoEntries);
    }

    private static long statisticsBytes(ColumnMetaData meta) {
        Optional<Statistics> statistics = meta.statistics();
        if (statistics.isEmpty()) {
            return 0L;
        }
        Statistics stats = statistics.orElseThrow();
        return stats.preferredMin().byteSize() + stats.preferredMax().byteSize();
    }

    private static Optional<BoundingBox> geoBoundingBox(ColumnMetaData meta) {
        return meta.geospatialStatistics().flatMap(GeospatialStatistics::bbox);
    }

    /**
     * Fills one freshly allocated blob. Both the geo section and the arena are sparse, and each grows through a cursor
     * as the chunks are visited in wire order.
     *
     * <p>A slot for a leaf with no chunk in its row group keeps the allocation's zero fill. Nothing stamps an absent
     * sentinel into it: its present flag stays clear, and that flag is what {@link CompactFooter#chunkIfPresent(int,
     * int)} tests before handing out a window.
     */
    private static final class BlobWriter {

        private final MemorySegment blob;
        private final LeafIndex leaves;
        private final int rowGroupCount;
        private final int leafCount;
        private final long rowGroupSectionOffset;
        private final long chunkTableOffset;
        private final long geoSectionOffset;
        private final long arenaOffset;
        private int nextGeoEntry;
        private int nextArenaByte;

        BlobWriter(int rowGroupCount, LeafIndex leaves, SectionSizes sizes) {
            this.leaves = leaves;
            this.rowGroupCount = rowGroupCount;
            this.leafCount = leaves.size();
            this.rowGroupSectionOffset = HEADER_BYTES;
            this.chunkTableOffset = rowGroupSectionOffset + (long) rowGroupCount * ROW_GROUP_RECORD_BYTES;
            this.geoSectionOffset = chunkTableOffset + chunkTableBytes(rowGroupCount, leafCount);
            this.arenaOffset = geoSectionOffset + (long) sizes.geoEntries() * GEO_ENTRY_BYTES;
            this.blob = allocate(arenaOffset + sizes.arenaBytes());
        }

        private static long chunkTableBytes(int rowGroupCount, int leafCount) {
            long chunkCount = (long) rowGroupCount * leafCount;
            if (chunkCount > Integer.MAX_VALUE / CHUNK_RECORD_BYTES) {
                throw new MalformedFileException("A footer with " + rowGroupCount + " row groups and " + leafCount
                        + " leaf columns has too many column chunks for the compact planning form");
            }
            return chunkCount * CHUNK_RECORD_BYTES;
        }

        private static MemorySegment allocate(long totalBytes) {
            if (totalBytes > Integer.MAX_VALUE) {
                throw new MalformedFileException(
                        "A footer needing " + totalBytes + " bytes exceeds the compact planning form's maximum size");
            }
            return MemorySegment.ofArray(new byte[(int) totalBytes]);
        }

        MemorySegment write(List<RowGroup> rowGroups) {
            writeHeader();
            for (int rowGroupIndex = 0; rowGroupIndex < rowGroups.size(); rowGroupIndex++) {
                RowGroup rowGroup = rowGroups.get(rowGroupIndex);
                writeRowGroupLane(rowGroupIndex, rowGroup);
                writeChunks(rowGroupIndex, rowGroup);
            }
            return blob;
        }

        private void writeHeader() {
            blob.set(INT32, HEADER_ROW_GROUP_COUNT, rowGroupCount);
            blob.set(INT32, HEADER_LEAF_COUNT, leafCount);
            blob.set(INT64, HEADER_ROW_GROUP_SECTION, rowGroupSectionOffset);
            blob.set(INT64, HEADER_CHUNK_TABLE, chunkTableOffset);
            blob.set(INT64, HEADER_GEO_SECTION, geoSectionOffset);
            blob.set(INT64, HEADER_ARENA, arenaOffset);
        }

        private void writeRowGroupLane(int rowGroupIndex, RowGroup rowGroup) {
            long base = rowGroupSectionOffset + (long) rowGroupIndex * ROW_GROUP_RECORD_BYTES;
            blob.set(INT64, base + ROW_GROUP_NUM_ROWS, rowGroup.numRows());
        }

        /**
         * A chunk without inline metadata cannot name its column, and its slot stays absent; the failure then lands on
         * the reader asking for that column, as it does when planning off the wire footer.
         */
        private void writeChunks(int rowGroupIndex, RowGroup rowGroup) {
            for (ColumnChunk chunk : rowGroup.columns()) {
                Optional<ColumnMetaData> meta = chunk.metaData();
                if (meta.isPresent()) {
                    writeChunk(rowGroupIndex, chunk, meta.orElseThrow());
                }
            }
        }

        private void writeChunk(int rowGroupIndex, ColumnChunk chunk, ColumnMetaData meta) {
            ColumnPath path = ColumnPath.of(meta.pathInSchema());
            int leaf = leaves.ordinalOf(path);
            if (leaf == LeafIndex.UNKNOWN) {
                throw new MalformedFileException("Column chunk " + path.dot() + " in row group " + rowGroupIndex
                        + " names a column not declared by the file schema");
            }
            ChunkSite site = new ChunkSite(rowGroupIndex, path);
            long base = chunkTableOffset + ((long) rowGroupIndex * leafCount + leaf) * CHUNK_RECORD_BYTES;
            requireLeafNotAlreadyWritten(base, site);
            writeChunkOffsets(base, chunk, meta);
            writeChunkCounts(base, meta);
            writeChunkTypes(base, meta, site);
            writeChunkStatistics(base, meta);
            int geoFlags = writeChunkGeoBbox(base, meta);
            blob.set(JAVA_BYTE, base + CHUNK_FLAGS, (byte) (FLAG_CHUNK_PRESENT | geoFlags));
        }

        /**
         * Two chunks of one row group resolving to the same leaf leave every fetch of that column ambiguous. Keeping
         * either one would plan reads off a corrupt footer, and the present flag of an already-filled slot is what
         * gives the duplicate away.
         */
        private void requireLeafNotAlreadyWritten(long base, ChunkSite site) {
            byte flags = blob.get(JAVA_BYTE, base + CHUNK_FLAGS);
            if ((flags & FLAG_CHUNK_PRESENT) != 0) {
                throw new MalformedFileException(site.describe("appears more than once"));
            }
        }

        private void writeChunkOffsets(long base, ColumnChunk chunk, ColumnMetaData meta) {
            blob.set(INT64, base + CHUNK_DATA_PAGE_OFFSET, meta.dataPageOffset());
            blob.set(INT64, base + CHUNK_DICTIONARY_PAGE_OFFSET, dictionaryPageOffset(meta));
            writeIndexLocator(
                    base + CHUNK_COLUMN_INDEX_OFFSET,
                    base + CHUNK_COLUMN_INDEX_LENGTH,
                    chunk.columnIndexOffset(),
                    chunk.columnIndexLength());
            writeIndexLocator(
                    base + CHUNK_OFFSET_INDEX_OFFSET,
                    base + CHUNK_OFFSET_INDEX_LENGTH,
                    chunk.offsetIndexOffset(),
                    chunk.offsetIndexLength());
            writeBloomLocator(base, meta);
        }

        /** A wire offset of zero or less points at no dictionary page; either spelling of absent is stored as zero. */
        private static long dictionaryPageOffset(ColumnMetaData meta) {
            long offset = meta.dictionaryPageOffset().orElse(NO_DICTIONARY_PAGE);
            return offset > 0 ? offset : NO_DICTIONARY_PAGE;
        }

        /**
         * A page index is usable only when the footer records both where it starts and how long it is; a locator
         * missing either half is stored as absent.
         */
        private void writeIndexLocator(long offsetLane, long lengthLane, OptionalLong offset, OptionalInt length) {
            boolean located = offset.isPresent() && length.isPresent();
            blob.set(INT64, offsetLane, located ? offset.getAsLong() : ABSENT_LONG);
            blob.set(INT32, lengthLane, located ? length.getAsInt() : 0);
        }

        /**
         * A bloom filter is reachable only when the footer says where it starts and states a length within the blob's
         * addressable range. Either half missing stores an absent locator, and the bloom tier then skips the column
         * rather than aiming a read at an unbounded stretch of bytes.
         */
        private void writeBloomLocator(long base, ColumnMetaData meta) {
            OptionalLong offset = meta.bloomFilterOffset();
            OptionalLong recordedLength = meta.bloomFilterLength();
            boolean reachable = offset.isPresent() && recordedLength.orElse(0L) <= Integer.MAX_VALUE;
            blob.set(INT64, base + CHUNK_BLOOM_FILTER_OFFSET, reachable ? offset.getAsLong() : ABSENT_LONG);
            blob.set(
                    INT32,
                    base + CHUNK_BLOOM_FILTER_LENGTH,
                    reachable ? bloomFilterLength(recordedLength) : ABSENT_INT);
        }

        /**
         * A length never recorded by the writer, or recorded as negative, is stored as absent; the reader then takes
         * the filter's size from the filter's own header.
         */
        private static int bloomFilterLength(OptionalLong recorded) {
            long length = recorded.orElse(ABSENT_INT);
            return length < 0 ? ABSENT_INT : (int) length;
        }

        private void writeChunkCounts(long base, ColumnMetaData meta) {
            blob.set(INT64, base + CHUNK_NUM_VALUES, meta.numValues());
            blob.set(INT64, base + CHUNK_NULL_COUNT, nullCount(meta));
            blob.set(INT32, base + CHUNK_TOTAL_COMPRESSED_SIZE, totalCompressedSize(meta));
        }

        private static long nullCount(ColumnMetaData meta) {
            Optional<Statistics> statistics = meta.statistics();
            if (statistics.isEmpty()) {
                return ABSENT_LONG;
            }
            return statistics.orElseThrow().nullCount().orElse(ABSENT_LONG);
        }

        private static int totalCompressedSize(ColumnMetaData meta) {
            long size = meta.totalCompressedSize();
            if (size < Integer.MIN_VALUE || size > Integer.MAX_VALUE) {
                return SIZE_BEYOND_RANGE;
            }
            return (int) size;
        }

        private void writeChunkTypes(long base, ColumnMetaData meta, ChunkSite site) {
            blob.set(JAVA_BYTE, base + CHUNK_CODEC, wireCode(meta.codec().value(), "compression codec", site));
            blob.set(JAVA_BYTE, base + CHUNK_TYPE, wireCode(meta.type().value(), "physical type", site));
        }

        private static byte wireCode(int code, String what, ChunkSite site) {
            if (code < 0 || code > Byte.MAX_VALUE) {
                throw new MalformedFileException(site.describe(
                        "has a " + what + " wire code of " + code + " outside the range stored by the blob"));
            }
            return (byte) code;
        }

        private void writeChunkStatistics(long base, ColumnMetaData meta) {
            Optional<Statistics> statistics = meta.statistics();
            if (statistics.isEmpty()) {
                blob.set(INT32, base + CHUNK_MIN_OFFSET, ABSENT_INT);
                blob.set(INT32, base + CHUNK_MAX_OFFSET, ABSENT_INT);
                return;
            }
            Statistics stats = statistics.orElseThrow();
            writeStatisticValue(base + CHUNK_MIN_OFFSET, base + CHUNK_MIN_LENGTH, stats.preferredMin());
            writeStatisticValue(base + CHUNK_MAX_OFFSET, base + CHUNK_MAX_LENGTH, stats.preferredMax());
        }

        private void writeStatisticValue(long offsetLane, long lengthLane, MemorySegment value) {
            if (value == MemorySegment.NULL) {
                blob.set(INT32, offsetLane, ABSENT_INT);
                return;
            }
            int length = (int) value.byteSize();
            MemorySegment.copy(value, 0L, blob, arenaOffset + nextArenaByte, length);
            blob.set(INT32, offsetLane, nextArenaByte);
            blob.set(INT32, lengthLane, length);
            nextArenaByte += length;
        }

        /**
         * Writes the chunk's geospatial bounding box into the geo section and returns the flag bits describing it. The
         * X and Y extents are mandatory on the wire; each Z and M half is independently optional, and a half not
         * recorded by the writer keeps the blob's zero fill, which a reader never reaches because its flag bit is
         * clear.
         */
        private int writeChunkGeoBbox(long base, ColumnMetaData meta) {
            Optional<BoundingBox> bbox = geoBoundingBox(meta);
            if (bbox.isEmpty()) {
                blob.set(INT32, base + CHUNK_GEO_INDEX, ABSENT_INT);
                return 0;
            }
            BoundingBox box = bbox.orElseThrow();
            long entry = geoSectionOffset + (long) nextGeoEntry * GEO_ENTRY_BYTES;
            blob.set(DOUBLE, entry + GEO_XMIN, box.xmin());
            blob.set(DOUBLE, entry + GEO_XMAX, box.xmax());
            blob.set(DOUBLE, entry + GEO_YMIN, box.ymin());
            blob.set(DOUBLE, entry + GEO_YMAX, box.ymax());
            int flags = FLAG_HAS_GEO_BBOX;
            flags |= writeOptionalExtent(entry + GEO_ZMIN, box.zmin(), FLAG_HAS_GEO_ZMIN);
            flags |= writeOptionalExtent(entry + GEO_ZMAX, box.zmax(), FLAG_HAS_GEO_ZMAX);
            flags |= writeOptionalExtent(entry + GEO_MMIN, box.mmin(), FLAG_HAS_GEO_MMIN);
            flags |= writeOptionalExtent(entry + GEO_MMAX, box.mmax(), FLAG_HAS_GEO_MMAX);
            blob.set(INT32, base + CHUNK_GEO_INDEX, nextGeoEntry);
            nextGeoEntry++;
            return flags;
        }

        private int writeOptionalExtent(long lane, OptionalDouble value, int flag) {
            if (value.isEmpty()) {
                return 0;
            }
            blob.set(DOUBLE, lane, value.getAsDouble());
            return flag;
        }
    }

    /** Names the chunk reported by an encoding failure. */
    private record ChunkSite(int rowGroupIndex, ColumnPath path) {

        String describe(String problem) {
            return "Column chunk " + path.dot() + " in row group " + rowGroupIndex + " " + problem;
        }
    }
}
