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
package io.tileverse.parquetry.materializer;

import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.ElementMeta;
import io.tileverse.parquetry.columnar.LeafLevels;
import io.tileverse.parquetry.columnar.LevelListVector;
import io.tileverse.parquetry.columnar.LevelSource;
import io.tileverse.parquetry.columnar.ListMeta;
import io.tileverse.parquetry.columnar.MapMeta;
import io.tileverse.parquetry.record.LevelElementRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Builds a Java {@link List} from a row of a {@link LevelListVector} by navigating its batch-owned level streams,
 * without the eager offset-and-child assembly the {@link ListMaterializer} path performs. The result is a lazy,
 * immutable, random-access view whose elements materialize on demand; it stays valid only while the owning batch is
 * open.
 *
 * <p>The navigation follows the level arithmetic of the streaming read path: a row's entries live in the structure
 * leaf's window {@code [rowStarts[r], rowStarts[r + 1])}; within a window, an entry whose repetition level is at most
 * the element repetition level opens an element, real when its definition level reaches the element definition level
 * and a skipped phantom otherwise; nested lists narrow the window and apply the same rules one level deeper.
 *
 * <p>A subtree that is scalar at every depth reads from the single structure leaf and allocates no per-leaf window
 * state. When the subtree holds a struct, Variant, or map at any depth ({@link ListMeta#multiLeaf()}), the view also
 * tracks every descendant leaf's window through a {@link MultiLeafCursor}; struct and Variant elements then materialize
 * through {@link LevelElementRecord}, which reads each descendant leaf at the per-element window the cursor resolves.
 */
public final class LevelListMaterializer {

    private LevelListMaterializer() {}

    /**
     * Builds a Java {@link List} from row {@code rowIndex} of {@code vec}. Returns {@code null} when the row's list is
     * null per the vector's validity; a present empty container materializes to {@link List#of()}. The returned view is
     * lazy and batch-lifetime.
     *
     * <p>Null is intentional - it distinguishes a null row from an empty list per the empty-vs-null contract.
     *
     * <p>A present row's window can open with a phantom entry (a null first element) yet still hold real elements after
     * it: in an unannotated structural list the element wrapper adds a definition level below the repeated child, which
     * leaves a definition gap an opener can sit in without terminating the list. Emptiness is therefore decided by the
     * view's element count over the whole window, not by the opening entry's definition level; an empty view equals
     * {@link List#of()}. The only short-circuit kept is the sound one: a window that starts past the structure leaf's
     * last entry holds nothing.
     */
    @SuppressWarnings({"java:S1452", "java:S1168"})
    public static List<?> materializeAt(LevelListVector vec, int rowIndex) {
        if (vec.isNull(rowIndex)) {
            return null;
        }
        ColumnPath structureLeaf = vec.structureLeaf();
        LeafLevels structureLevels = vec.levels(structureLeaf);
        int windowStart = structureLevels.rowStarts().get(rowIndex);
        int windowEnd = structureLevels.rowStarts().get(rowIndex + 1);
        ListMeta meta = vec.meta();
        if (windowStart >= structureLevels.entryCount()) {
            return List.of();
        }
        if (meta.multiLeaf()) {
            return new LevelListView(vec, rowIndex, meta, structureLeaf, LeafWindows.forRow(vec, rowIndex));
        }
        return new LevelListView(vec, rowIndex, meta, structureLeaf, windowStart, windowEnd);
    }

    /**
     * Builds the lazy view of a list-typed struct field whose subtree spans several descendant leaves. The
     * {@link LevelElementRecord} of a struct calls this for a nested list field, passing the per-leaf windows of the
     * element holding the field; the inner list's entries lie within that element's window in every descendant leaf.
     */
    @SuppressWarnings("java:S1452")
    public static List<?> nestedListView(
            LevelSource vec, int rowIndex, ListMeta meta, ColumnPath structureLeaf, LeafWindows windows) {
        return new LevelListView(vec, rowIndex, meta, structureLeaf, windows);
    }

    /**
     * Builds the lazy view of a list-typed struct field whose subtree is scalar at every depth and reads from its
     * single structure leaf alone.
     */
    @SuppressWarnings("java:S1452")
    public static List<?> nestedListView(
            LevelSource vec, int rowIndex, ListMeta meta, ColumnPath structureLeaf, int windowStart, int windowEnd) {
        return new LevelListView(vec, rowIndex, meta, structureLeaf, windowStart, windowEnd);
    }

    /**
     * An immutable, random-access {@link List} view over one list value, reading the value's elements from a structure
     * leaf's level streams on demand. A forward cursor caches the last visited element's entry position; sequential
     * iteration advances the cursor in amortized O(1), while an index behind the cursor rescans from the window start
     * in linear time.
     *
     * <p>On the multi-leaf path the view additionally drives a {@link MultiLeafCursor} that advances every descendant
     * leaf's position in lockstep, handing struct and Variant elements (and multi-leaf nested lists) their per-element
     * windows without rescanning the span per element.
     */
    static final class LevelListView extends AbstractList<Object> implements RandomAccess {

        private final LevelSource vec;
        private final int rowIndex;
        private final ColumnVector structureLeaf;
        private final ColumnPath structureLeafPath;
        private final LeafLevels levels;
        private final ListMeta meta;
        private final int windowStart;
        private final int windowEnd;

        /** The per-leaf spans of this view, present only on the multi-leaf path. */
        private final LeafWindows spanWindows;

        private int size = -1;
        private int cursorIndex = -1;
        private int cursorEntry = -1;

        /** Lazily created on the first access that needs per-element multi-leaf windows. */
        private MultiLeafCursor multiLeafCursor;

        /** The single-structure-leaf view of a subtree that is scalar at every depth. */
        LevelListView(
                LevelSource vec,
                int rowIndex,
                ListMeta meta,
                ColumnPath structureLeafPath,
                int windowStart,
                int windowEnd) {
            this.vec = vec;
            this.rowIndex = rowIndex;
            this.structureLeaf = vec.leaf(structureLeafPath);
            this.structureLeafPath = structureLeafPath;
            this.levels = vec.levels(structureLeafPath);
            this.meta = meta;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.spanWindows = null;
        }

        /** The multi-leaf view of a subtree holding a struct, Variant, or map at some depth. */
        LevelListView(LevelSource vec, int rowIndex, ListMeta meta, ColumnPath structureLeafPath, LeafWindows windows) {
            this.vec = vec;
            this.rowIndex = rowIndex;
            this.structureLeaf = vec.leaf(structureLeafPath);
            this.structureLeafPath = structureLeafPath;
            this.levels = vec.levels(structureLeafPath);
            this.meta = meta;
            this.windowStart = windows.start(structureLeafPath);
            this.windowEnd = windows.end(structureLeafPath);
            this.spanWindows = windows;
        }

        @Override
        public int size() {
            if (size < 0) {
                size = countElements();
            }
            return size;
        }

        @Override
        public Object get(int index) {
            Objects.checkIndex(index, size());
            int entry = entryOf(index);
            return valueAt(index, entry);
        }

        /** Counts the real elements in the window: openers that reach the element definition level. */
        private int countElements() {
            int count = 0;
            for (int entry = windowStart; entry < windowEnd; entry++) {
                if (opensRealElement(entry)) {
                    count++;
                }
            }
            return count;
        }

        /**
         * The entry position of the {@code index}-th real element. A request at or behind the cursor rescans from the
         * window start; a request ahead of it resumes from the entry after the cached one, which keeps sequential
         * iteration amortized O(1).
         */
        private int entryOf(int index) {
            if (index == cursorIndex) {
                return cursorEntry;
            }
            int seen;
            int fromEntry;
            if (index > cursorIndex && cursorIndex >= 0) {
                seen = cursorIndex;
                fromEntry = cursorEntry + 1;
            } else {
                seen = -1;
                fromEntry = windowStart;
            }
            for (int entry = fromEntry; entry < windowEnd; entry++) {
                if (opensRealElement(entry)) {
                    seen++;
                    if (seen == index) {
                        cursorIndex = index;
                        cursorEntry = entry;
                        return entry;
                    }
                }
            }
            throw new IndexOutOfBoundsException("element " + index + " not found in window");
        }

        private boolean opensRealElement(int entry) {
            return levels.repLevels().get(entry) <= meta.elementRepLevel()
                    && levels.defLevels().get(entry) >= meta.elementDefLevel();
        }

        private Object valueAt(int index, int structureEntry) {
            return switch (meta.element()) {
                case ElementMeta.Scalar _ -> structureLeaf.get(structureEntry);
                case ElementMeta.NestedList(ListMeta child) -> nestedListAt(child, index, structureEntry);
                case ElementMeta.Struct element -> structElementAt(element, index, structureEntry);
                case ElementMeta.VariantLeaves element -> variantElementAt(element, index);
                case ElementMeta.MapEntry(MapMeta map) -> mapElementAt(map, index);
                // A list whose element subtree was fully dropped has no structure leaf; the vector factory rejects
                // such a meta before any view exists to navigate it.
                case ElementMeta.Absent _ -> throw new IllegalStateException("list element is absent");
            };
        }

        /**
         * The map element at list index {@code index}: the map spans this element's window in every descendant leaf. A
         * null map yields a null element and an empty map yields an empty map, both read from the structure leaf at the
         * element's window start.
         */
        private Object mapElementAt(MapMeta map, int index) {
            return LevelMapMaterializer.materialize(vec, rowIndex, map, elementWindows(index));
        }

        /**
         * The struct element at list index {@code index}: null when the slot holds no struct, else a fresh record
         * reading each descendant leaf at this element's per-leaf window.
         */
        private Object structElementAt(ElementMeta.Struct element, int index, int structureEntry) {
            if (levels.defLevels().get(structureEntry) < element.presenceDef()) {
                return null;
            }
            return new LevelElementRecord(vec, rowIndex, element.struct(), elementWindows(index));
        }

        /**
         * The Variant element at list index {@code index}, built from its two binary leaves at the element position.
         */
        private Object variantElementAt(ElementMeta.VariantLeaves element, int index) {
            return LevelElementRecord.readVariant(vec, element, elementWindows(index));
        }

        /** The per-leaf windows of the element at list index {@code index}, resolved by the shared forward cursor. */
        private LeafWindows elementWindows(int index) {
            if (multiLeafCursor == null) {
                multiLeafCursor = new MultiLeafCursor(spanWindows, meta.elementRepLevel(), meta.elementDefLevel());
            }
            return multiLeafCursor.windowsAt(index);
        }

        /**
         * The nested list element opened at {@code entry}: its inner window runs to the next entry that opens an
         * element of this list (or the window end). A null inner container yields a null element. The inner window can
         * open with a phantom entry (a null first sub-element) yet still hold real elements after it; emptiness is
         * therefore decided by the inner view's element count over the whole window, not by the opening entry alone. A
         * scalar-at-every-depth inner subtree reads on through this view's structure leaf; an inner subtree with struct
         * elements below receives the element's windows in every descendant leaf.
         */
        private Object nestedListAt(ListMeta child, int index, int entry) {
            int innerDef = levels.defLevels().get(entry);
            if (innerDef < child.groupDefLevel()) {
                return null;
            }
            if (child.multiLeaf()) {
                return new LevelListView(vec, rowIndex, child, child.structureLeaf(), elementWindows(index));
            }
            return new LevelListView(vec, rowIndex, child, structureLeafPath, entry, nextElementBoundary(entry));
        }

        /** The next entry after {@code entry} that opens an element of this list, or the window end. */
        private int nextElementBoundary(int entry) {
            for (int next = entry + 1; next < windowEnd; next++) {
                if (levels.repLevels().get(next) <= meta.elementRepLevel()) {
                    return next;
                }
            }
            return windowEnd;
        }
    }
}
