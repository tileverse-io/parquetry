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
package io.tileverse.parquetry.internal.filter;

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.PrimitiveKind;

import lombok.NonNull;

/**
 * The data pages of one column chunk, learned from their page headers by a read of the whole chunk. The entry lists the
 * pages in row order. For each page it holds the place of the page in the chunk, its first row, its null count, its
 * encoding and whether it holds only nulls. It keeps the minimum and the maximum of the pages holding a value, or no
 * bounds at all, as chosen by {@link LearnedBounds}.
 *
 * <p>An entry is immutable and packed into one read-only heap {@link MemorySegment}, whatever its page count. A read
 * holding an entry keeps it readable after a cache evicts it.
 */
public final class LearnedPages {

    /*
     * An entry starts with a 6-byte header holding the page count, the flags of the entry and the width of an inline
     * bound. An 18-byte record per page follows, with the offset, the size, the first row and the null count of the
     * page as ints, then its encoding and its flags as bytes. The bounds come last when the entry keeps them, in two
     * slots per page, the minimum then the maximum. A fixed-width kind keeps its bounds inline in their slots. A binary
     * kind keeps the position and the length of a bound in its slot, and the bytes of the bounds after the slots. Such
     * a position counts from the start of the segment. The slots of a page of nulls stay zero and are never read. The
     * lanes are little-endian, read through unaligned layouts as required by a segment over a heap byte[].
     */
    private static final int HEADER_BYTES = 6;
    private static final int HEADER_PAGE_COUNT = 0;
    private static final int HEADER_FLAGS = 4;
    private static final int HEADER_BOUND_WIDTH = 5;

    private static final int PAGE_RECORD_BYTES = 18;
    private static final int PAGE_OFFSET = 0;
    private static final int PAGE_SIZE = 4;
    private static final int PAGE_FIRST_ROW = 8;
    private static final int PAGE_NULL_COUNT = 12;
    private static final int PAGE_ENCODING = 16;
    private static final int PAGE_FLAGS = 17;

    private static final int SLOTS_PER_PAGE = 2;
    private static final int MIN_SLOT = 0;
    private static final int MAX_SLOT = 1;

    private static final int REFERENCE_BYTES = 8;
    private static final int REFERENCE_POSITION = 0;
    private static final int REFERENCE_LENGTH = 4;

    private static final byte NO_FLAGS = 0;
    private static final byte FLAG_HAS_BOUNDS = 0x1;
    private static final byte FLAG_NULL_PAGE = 0x1;

    /** Written into the null count lane when the header of a page records no null count within the lane's range. */
    private static final int UNKNOWN_NULL_COUNT = -1;

    /** The encodings indexed by their wire code, as stored in the encoding lane of a page record. */
    private static final Encoding[] ENCODINGS_BY_CODE = encodingsByCode();

    private final MemorySegment entry;
    private final int pageCount;
    private final boolean hasBounds;
    private final int boundWidth;

    private LearnedPages(MemorySegment packed) {
        MemorySegment readOnly = packed.asReadOnly();
        this.entry = readOnly;
        this.pageCount = readOnly.get(INT32, HEADER_PAGE_COUNT);
        this.hasBounds = (readOnly.get(JAVA_BYTE, HEADER_FLAGS) & FLAG_HAS_BOUNDS) != 0;
        this.boundWidth = readOnly.get(JAVA_BYTE, HEADER_BOUND_WIDTH);
    }

    /**
     * A builder of the entry of a chunk of {@code kind} values annotated {@code logicalType}. The type selects the
     * bounds kept by the entry, and the entry does not store it.
     *
     * @param expectedPages the number of pages expected in the chunk, used to size the builder
     */
    public static Builder builder(
            @NonNull PrimitiveKind kind, @NonNull Optional<LogicalType> logicalType, int expectedPages) {
        return new Builder(kind, logicalType, expectedPages);
    }

    /** The number of data pages of the chunk. */
    public int pageCount() {
        return pageCount;
    }

    /** The position of the header of page {@code page}, in bytes from the start of the chunk. */
    public long offset(int page) {
        return entry.get(INT32, pageLane(page, PAGE_OFFSET));
    }

    /** The bytes of page {@code page}, its header included. */
    public int size(int page) {
        return entry.get(INT32, pageLane(page, PAGE_SIZE));
    }

    /** The first row of page {@code page}, relative to the row group. */
    public long firstRow(int page) {
        return entry.get(INT32, pageLane(page, PAGE_FIRST_ROW));
    }

    /**
     * The null count of page {@code page}. It is empty when its header records none, or records a count below zero or
     * above {@link Integer#MAX_VALUE}.
     */
    public OptionalLong nullCount(int page) {
        int nullCount = entry.get(INT32, pageLane(page, PAGE_NULL_COUNT));
        if (nullCount == UNKNOWN_NULL_COUNT) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(nullCount);
    }

    /**
     * Whether page {@code page} holds only nulls. Its header then counts as many nulls as values and records no bound.
     */
    public boolean nullPage(int page) {
        byte flags = entry.get(JAVA_BYTE, pageLane(page, PAGE_FLAGS));
        return (flags & FLAG_NULL_PAGE) != 0;
    }

    /** The encoding of the values of page {@code page}. */
    public Encoding encoding(int page) {
        byte code = entry.get(JAVA_BYTE, pageLane(page, PAGE_ENCODING));
        return ENCODINGS_BY_CODE[code];
    }

    /** Whether the entry keeps the bounds of its pages. It keeps them for the pages holding a value, or for no page. */
    public boolean hasBounds() {
        return hasBounds;
    }

    /**
     * The minimum of page {@code page}, as a read-only slice of the entry. It is empty for a page of nulls and for an
     * entry without bounds. A binary minimum may be a prefix of the minimum recorded by the writer.
     */
    public Optional<MemorySegment> min(int page) {
        return bound(page, MIN_SLOT);
    }

    /**
     * The maximum of page {@code page}, as a read-only slice of the entry. It is empty for a page of nulls and for an
     * entry without bounds. A binary maximum may be cut and raised above the maximum recorded by the writer.
     */
    public Optional<MemorySegment> max(int page) {
        return bound(page, MAX_SLOT);
    }

    /** The bytes of the segment holding the entry, the weight charged by a cache. */
    public long byteSize() {
        return entry.byteSize();
    }

    /** The position in the entry of lane {@code lane} of the record of page {@code page}. */
    private long pageLane(int page, int lane) {
        Objects.checkIndex(page, pageCount);
        return recordOffset(page) + lane;
    }

    private Optional<MemorySegment> bound(int page, int slot) {
        boolean nullPage = nullPage(page);
        if (nullPage || !hasBounds) {
            return Optional.empty();
        }
        if (boundWidth == LearnedBounds.VARIABLE_WIDTH) {
            return Optional.of(binaryBound(page, slot));
        }
        return Optional.of(inlineBound(page, slot));
    }

    private MemorySegment inlineBound(int page, int slot) {
        long position = slotOffset(pageCount, page, slot, boundWidth);
        return entry.asSlice(position, boundWidth);
    }

    private MemorySegment binaryBound(int page, int slot) {
        long reference = slotOffset(pageCount, page, slot, REFERENCE_BYTES);
        int position = entry.get(INT32, reference + REFERENCE_POSITION);
        int length = entry.get(INT32, reference + REFERENCE_LENGTH);
        return entry.asSlice(position, length);
    }

    /** The position of the record of page {@code page}. */
    private static long recordOffset(int page) {
        return HEADER_BYTES + (long) page * PAGE_RECORD_BYTES;
    }

    /** The position of the bounds of an entry of {@code pageCount} pages. They follow the last page record. */
    private static long boundsOffset(int pageCount) {
        return recordOffset(pageCount);
    }

    /** The position of slot {@code slot} of page {@code page}, in slots of {@code slotBytes} each. */
    private static long slotOffset(int pageCount, int page, int slot, int slotBytes) {
        long index = (long) page * SLOTS_PER_PAGE + slot;
        return boundsOffset(pageCount) + index * slotBytes;
    }

    private static Encoding[] encodingsByCode() {
        Encoding[] encodings = Encoding.values();
        int highestCode = 0;
        for (Encoding encoding : encodings) {
            highestCode = Math.max(highestCode, encoding.value());
        }
        Encoding[] byCode = new Encoding[highestCode + 1];
        for (Encoding encoding : encodings) {
            byCode[encoding.value()] = encoding;
        }
        return byCode;
    }

    /** Collects the data pages of a chunk in row order, then packs them into an entry. */
    public static final class Builder {

        private final PrimitiveKind kind;
        private final Optional<LogicalType> logicalType;
        private final List<Page> pages;

        private Builder(PrimitiveKind kind, Optional<LogicalType> logicalType, int expectedPages) {
            this.kind = kind;
            this.logicalType = logicalType;
            this.pages = new ArrayList<>(expectedPages);
        }

        /**
         * Adds the next data page of the chunk. Its null count and its bounds come from {@code statistics}. The page is
         * marked as holding only nulls when its null count equals {@code numValues} and its statistics record no bound.
         * A marked page keeps no bound. A recorded bound proves a value, and such a page stays unmarked with its
         * trusted bounds and its null count.
         *
         * @param offset the position of the page header, in bytes from the start of the chunk
         * @param size the bytes of the page, its header included
         * @param firstRow the first row of the page, relative to the row group
         * @param numValues the values of the page, its nulls included
         * @param encoding the encoding of the values of the page
         * @param statistics the statistics of the page header, empty when the header records none
         * @throws IllegalArgumentException when the offset, the size or the first row is negative or above
         *     {@link Integer#MAX_VALUE}
         */
        public Builder add(
                long offset,
                int size,
                long firstRow,
                long numValues,
                @NonNull Encoding encoding,
                @NonNull Optional<Statistics> statistics) {
            int offsetLane = intLane(offset, "offset");
            int sizeLane = intLane(size, "size");
            int firstRowLane = intLane(firstRow, "first row");
            int nullCount = nullCountLane(statistics);
            boolean nullPage = holdsOnlyNulls(nullCount, numValues, statistics);
            Optional<Bounds> bounds = nullPage ? Optional.empty() : boundsOf(statistics);
            pages.add(new Page(offsetLane, sizeLane, firstRowLane, nullCount, nullPage, encoding, bounds));
            return this;
        }

        private static int intLane(long value, String name) {
            if (value < 0 || value > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "The " + name + " of a learned page lies outside [0, " + Integer.MAX_VALUE + "]: " + value);
            }
            return (int) value;
        }

        /** The null count of {@code statistics} as a lane, unknown when it has none within the lane's range. */
        private static int nullCountLane(Optional<Statistics> statistics) {
            OptionalLong recorded = statistics.map(Statistics::nullCount).orElse(OptionalLong.empty());
            long nullCount = recorded.orElse(UNKNOWN_NULL_COUNT);
            boolean fits = nullCount >= 0 && nullCount <= Integer.MAX_VALUE;
            return fits ? (int) nullCount : UNKNOWN_NULL_COUNT;
        }

        /**
         * Whether the statistics of a page show that it holds only nulls. Its null count must equal its values, and its
         * statistics must record no bound. A recorded bound proves a value, whatever the count says.
         */
        private static boolean holdsOnlyNulls(int nullCount, long numValues, Optional<Statistics> statistics) {
            boolean countsOnlyNulls = nullCount != UNKNOWN_NULL_COUNT && nullCount == numValues;
            return countsOnlyNulls && !recordsABound(statistics);
        }

        /** Whether {@code statistics} record a minimum or a maximum, in the modern or the deprecated fields. */
        private static boolean recordsABound(Optional<Statistics> statistics) {
            if (statistics.isEmpty()) {
                return false;
            }
            Statistics recorded = statistics.orElseThrow();
            return recorded.preferredMin() != MemorySegment.NULL || recorded.preferredMax() != MemorySegment.NULL;
        }

        /** The bounds of a page, present when the trust rule keeps both its minimum and its maximum. */
        private Optional<Bounds> boundsOf(Optional<Statistics> statistics) {
            if (statistics.isEmpty()) {
                return Optional.empty();
            }
            Statistics recorded = statistics.orElseThrow();
            Optional<MemorySegment> min = LearnedBounds.min(recorded, kind, logicalType);
            Optional<MemorySegment> max = LearnedBounds.max(recorded, kind, logicalType);
            if (min.isEmpty() || max.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new Bounds(min.orElseThrow(), max.orElseThrow()));
        }

        /**
         * The entry of the pages added so far. It keeps their bounds only when each page holding a value has a trusted
         * minimum and maximum.
         *
         * @throws IllegalStateException when no page was added, or when the pages do not fit in one heap array
         */
        public LearnedPages build() {
            if (pages.isEmpty()) {
                throw new IllegalStateException("A learned page index holds at least one page");
            }
            boolean keepsBounds = eachValuedPageIsBounded();
            int width = LearnedBounds.fixedWidth(kind);
            MemorySegment packed = allocate(entryBytes(keepsBounds, width));
            writeHeader(packed, keepsBounds, width);
            writeRecords(packed);
            if (keepsBounds) {
                writeBounds(packed, width);
            }
            return new LearnedPages(packed);
        }

        /** Whether each page holding a value has bounds. A page of nulls needs none. */
        private boolean eachValuedPageIsBounded() {
            for (Page page : pages) {
                if (!page.nullPage() && page.bounds().isEmpty()) {
                    return false;
                }
            }
            return true;
        }

        private long entryBytes(boolean keepsBounds, int width) {
            int pageCount = pages.size();
            if (!keepsBounds) {
                return boundsOffset(pageCount);
            }
            if (width == LearnedBounds.VARIABLE_WIDTH) {
                return binaryBytesOffset(pageCount) + binaryBoundBytes();
            }
            long slots = (long) pageCount * SLOTS_PER_PAGE;
            return boundsOffset(pageCount) + slots * width;
        }

        /** The position of the bytes of the binary bounds of an entry of {@code pageCount} pages, after the slots. */
        private static long binaryBytesOffset(int pageCount) {
            long slots = (long) pageCount * SLOTS_PER_PAGE;
            return boundsOffset(pageCount) + slots * REFERENCE_BYTES;
        }

        private long binaryBoundBytes() {
            long bytes = 0;
            for (Page page : pages) {
                bytes += page.bounds().map(Bounds::byteSize).orElse(0L);
            }
            return bytes;
        }

        private static MemorySegment allocate(long bytes) {
            if (bytes > Integer.MAX_VALUE) {
                throw new IllegalStateException(
                        "A learned page index of " + bytes + " bytes exceeds the size of one heap array");
            }
            return MemorySegment.ofArray(new byte[(int) bytes]);
        }

        private void writeHeader(MemorySegment packed, boolean keepsBounds, int width) {
            packed.set(INT32, HEADER_PAGE_COUNT, pages.size());
            packed.set(JAVA_BYTE, HEADER_FLAGS, keepsBounds ? FLAG_HAS_BOUNDS : NO_FLAGS);
            packed.set(JAVA_BYTE, HEADER_BOUND_WIDTH, (byte) width);
        }

        private void writeRecords(MemorySegment packed) {
            for (int index = 0; index < pages.size(); index++) {
                Page page = pages.get(index);
                long recordStart = recordOffset(index);
                byte code = (byte) page.encoding().value();
                byte flags = page.nullPage() ? FLAG_NULL_PAGE : NO_FLAGS;
                packed.set(INT32, recordStart + PAGE_OFFSET, page.offset());
                packed.set(INT32, recordStart + PAGE_SIZE, page.size());
                packed.set(INT32, recordStart + PAGE_FIRST_ROW, page.firstRow());
                packed.set(INT32, recordStart + PAGE_NULL_COUNT, page.nullCount());
                packed.set(JAVA_BYTE, recordStart + PAGE_ENCODING, code);
                packed.set(JAVA_BYTE, recordStart + PAGE_FLAGS, flags);
            }
        }

        private void writeBounds(MemorySegment packed, int width) {
            if (width == LearnedBounds.VARIABLE_WIDTH) {
                writeBinaryBounds(packed);
            } else {
                writeInlineBounds(packed, width);
            }
        }

        private void writeInlineBounds(MemorySegment packed, int width) {
            int pageCount = pages.size();
            for (int index = 0; index < pageCount; index++) {
                Optional<Bounds> bounds = pages.get(index).bounds();
                if (bounds.isPresent()) {
                    Bounds kept = bounds.orElseThrow();
                    long minSlot = slotOffset(pageCount, index, MIN_SLOT, width);
                    long maxSlot = slotOffset(pageCount, index, MAX_SLOT, width);
                    MemorySegment.copy(kept.min(), 0, packed, minSlot, width);
                    MemorySegment.copy(kept.max(), 0, packed, maxSlot, width);
                }
            }
        }

        private void writeBinaryBounds(MemorySegment packed) {
            int pageCount = pages.size();
            long position = binaryBytesOffset(pageCount);
            for (int index = 0; index < pageCount; index++) {
                Optional<Bounds> bounds = pages.get(index).bounds();
                if (bounds.isPresent()) {
                    Bounds kept = bounds.orElseThrow();
                    long minSlot = slotOffset(pageCount, index, MIN_SLOT, REFERENCE_BYTES);
                    long maxSlot = slotOffset(pageCount, index, MAX_SLOT, REFERENCE_BYTES);
                    position = writeBinaryBound(packed, minSlot, kept.min(), position);
                    position = writeBinaryBound(packed, maxSlot, kept.max(), position);
                }
            }
        }

        /**
         * Writes {@code bound} at {@code position} and its reference into {@code slot}. It returns the position after
         * the bound.
         */
        private static long writeBinaryBound(MemorySegment packed, long slot, MemorySegment bound, long position) {
            int length = (int) bound.byteSize();
            MemorySegment.copy(bound, 0, packed, position, length);
            packed.set(INT32, slot + REFERENCE_POSITION, (int) position);
            packed.set(INT32, slot + REFERENCE_LENGTH, length);
            return position + length;
        }

        /** A page added to the builder, its lanes checked and its bounds chosen. */
        private record Page(
                int offset,
                int size,
                int firstRow,
                int nullCount,
                boolean nullPage,
                @NonNull Encoding encoding,
                @NonNull Optional<Bounds> bounds) {}

        /** The minimum and the maximum kept for a page. */
        private record Bounds(
                @NonNull MemorySegment min, @NonNull MemorySegment max) {

            long byteSize() {
                return min.byteSize() + max.byteSize();
            }
        }
    }
}
