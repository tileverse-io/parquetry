/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.columnar;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * A pure gather over {@link ColumnVector}s: rebuilds a vector keeping only the rows (or child elements) at
 * {@code keptIndices}, in that order. Recurses into nested {@link ListVector} / {@link MapVector} /
 * {@link StructVector} / {@link VariantVector} children, re-indexing container offsets and gathering the child elements
 * each kept row spans. It reads no leaf or level state.
 *
 * <p>Two callers: the Dremel assembler drops phantom elements of an enclosing repeated group during nested
 * reconstruction, and {@link FilteredRecordBatch#compacted()} rebuilds a predicate-filtered view as dense vectors for
 * the Arrow export boundary.
 *
 * <p>Two shapes are rejected deliberately. A {@link ShreddedVariantVector} is refused because in assembly reaching one
 * means the unsupported shredded-under-list/map read shape; {@code FilteredRecordBatch.compacted()} unshreds a legal
 * top-level shredded column before gathering. The level-backed {@link LevelListVector} / {@link LevelMapVector} are
 * refused because only the streaming scan produces them - they are never assembly children and never batch columns.
 */
public final class Compaction {

    private Compaction() {}

    public static ColumnVector compact(ColumnVector child, int[] keptIndices) {
        if (child == null) {
            return null;
        }
        if (keptIndices.length == child.size()) {
            return child;
        }
        return switch (child) {
            case IntVector v -> {
                int[] ints = gatherInts(v, keptIndices);
                Validity validity = gatherValidity(v, keptIndices);
                yield IntVector.materialized(ints, validity);
            }
            case LongVector v -> {
                long[] longs = gatherLongs(v, keptIndices);
                Validity validity = gatherValidity(v, keptIndices);
                yield LongVector.materialized(longs, validity);
            }
            case FloatVector v -> {
                float[] floats = gatherFloats(v, keptIndices);
                Validity validity = gatherValidity(v, keptIndices);
                yield FloatVector.materialized(floats, validity);
            }
            case DoubleVector v -> {
                double[] doubles = gatherDoubles(v, keptIndices);
                Validity validity = gatherValidity(v, keptIndices);
                yield DoubleVector.materialized(doubles, validity);
            }
            case BooleanVector v -> {
                boolean[] booleans = gatherBooleans(v, keptIndices);
                Validity validity = gatherValidity(v, keptIndices);
                yield BooleanVector.materialized(booleans, validity);
            }
            case BinaryVector v -> {
                MemorySegment[] segments = gatherSegments(v::get, keptIndices);
                Validity validity = gatherValidity(v, keptIndices);
                yield BinaryVector.materialized(segments, validity);
            }
            case FixedLenBinaryVector v -> {
                MemorySegment[] segments = gatherSegments(v::get, keptIndices);
                int byteWidth = v.byteWidth();
                Validity validity = gatherValidity(v, keptIndices);
                yield FixedLenBinaryVector.materialized(segments, byteWidth, validity);
            }
            case Int96Vector v -> {
                MemorySegment[] segments = gatherSegments(v::get, keptIndices);
                Validity validity = gatherValidity(v, keptIndices);
                yield Int96Vector.materialized(segments, validity);
            }
            case ListVector v -> compactList(v, keptIndices);
            case MapVector v -> compactMap(v, keptIndices);
            case StructVector v -> compactStruct(v, keptIndices);
            case VariantVector v -> compactVariant(v, keptIndices);
            case ShreddedVariantVector _ ->
                throw new ParquetFormatException("a shredded Variant vector must be unshredded before compaction; "
                        + "reading a shredded Variant nested under a list or map is not supported");
            case LevelListVector _, LevelMapVector _ ->
                throw new IllegalStateException("level-backed vectors are never assembly children");
        };
    }

    private static VariantVector compactVariant(VariantVector v, int[] keptIndices) {
        BinaryVector metadata = (BinaryVector) compact(v.metadataColumn(), keptIndices);
        BinaryVector value = (BinaryVector) compact(v.valueColumn(), keptIndices);
        return new VariantVector(metadata, value, gatherValidity(v, keptIndices), keptIndices.length);
    }

    private static ListVector compactList(ListVector v, int[] keptIndices) {
        ChildGather gather = gatherNestedRows(v::rowOffsetStart, v::rowOffsetEnd, keptIndices);
        ColumnVector compactedChild = compact(v.child(), gather.childIndices());
        return new ListVector(gather.offsets(), compactedChild, gatherValidity(v, keptIndices), keptIndices.length);
    }

    private static MapVector compactMap(MapVector v, int[] keptIndices) {
        ChildGather gather = gatherNestedRows(v::rowOffsetStart, v::rowOffsetEnd, keptIndices);
        ColumnVector compactedKeys = compact(v.keys(), gather.childIndices());
        ColumnVector compactedValues = compact(v.values(), gather.childIndices());
        return new MapVector(
                gather.offsets(), compactedKeys, compactedValues, gatherValidity(v, keptIndices), keptIndices.length);
    }

    private static StructVector compactStruct(StructVector v, int[] keptIndices) {
        Map<ColumnPath, ColumnVector> children = new LinkedHashMap<>();
        for (Map.Entry<ColumnPath, ColumnVector> entry : v.children().entrySet()) {
            children.put(entry.getKey(), compact(entry.getValue(), keptIndices));
        }
        return new StructVector(children, gatherValidity(v, keptIndices), keptIndices.length);
    }

    /**
     * Reindexes a nested container's offsets to the kept parent rows, collecting the child-element indices each kept
     * row points at into a flat, contiguous index list.
     */
    private static ChildGather gatherNestedRows(IntUnaryOperator startOf, IntUnaryOperator endOf, int[] keptIndices) {
        int[] offsets = new int[keptIndices.length + 1];
        List<Integer> childIndices = new ArrayList<>();
        int running = 0;
        for (int i = 0; i < keptIndices.length; i++) {
            offsets[i] = running;
            int start = startOf.applyAsInt(keptIndices[i]);
            int end = endOf.applyAsInt(keptIndices[i]);
            for (int e = start; e < end; e++) {
                childIndices.add(e);
            }
            running += end - start;
        }
        offsets[keptIndices.length] = running;
        int[] childIndexArray = new int[childIndices.size()];
        for (int i = 0; i < childIndexArray.length; i++) {
            childIndexArray[i] = childIndices.get(i);
        }
        return new ChildGather(offsets, childIndexArray);
    }

    @SuppressWarnings("java:S6218") // internal gather carrier, never compared by value
    private record ChildGather(int[] offsets, int[] childIndices) {}

    /**
     * Re-indexes a flat offsets array ({@code length = rows + 1}) to the kept rows, collecting the element indices each
     * kept row spans into a contiguous list. The companion to {@link #gatherNestedRows} for inputs that hold their
     * per-row ranges as a plain offsets array rather than a vector.
     */
    public static OffsetGather gatherOffsets(int[] offsets, int[] keptIndices) {
        ChildGather gather = gatherNestedRows(row -> offsets[row], row -> offsets[row + 1], keptIndices);
        return new OffsetGather(gather.offsets(), gather.childIndices());
    }

    @SuppressWarnings("java:S6218") // internal gather carrier, never compared by value
    public record OffsetGather(int[] offsets, int[] childIndices) {}

    // Compaction keeps a value for every kept index, null rows included; the gathered validity preserves the
    // null mask. Read the backing directly via valueAt to keep the parked value for kept null rows instead of
    // failing fast.
    private static int[] gatherInts(IntVector v, int[] keptIndices) {
        int[] out = new int[keptIndices.length];
        for (int i = 0; i < keptIndices.length; i++) {
            out[i] = v.valueAt(keptIndices[i]);
        }
        return out;
    }

    private static long[] gatherLongs(LongVector v, int[] keptIndices) {
        long[] out = new long[keptIndices.length];
        for (int i = 0; i < keptIndices.length; i++) {
            out[i] = v.valueAt(keptIndices[i]);
        }
        return out;
    }

    private static float[] gatherFloats(FloatVector v, int[] keptIndices) {
        float[] out = new float[keptIndices.length];
        for (int i = 0; i < keptIndices.length; i++) {
            out[i] = v.valueAt(keptIndices[i]);
        }
        return out;
    }

    private static double[] gatherDoubles(DoubleVector v, int[] keptIndices) {
        double[] out = new double[keptIndices.length];
        for (int i = 0; i < keptIndices.length; i++) {
            out[i] = v.valueAt(keptIndices[i]);
        }
        return out;
    }

    private static boolean[] gatherBooleans(BooleanVector v, int[] keptIndices) {
        boolean[] out = new boolean[keptIndices.length];
        for (int i = 0; i < keptIndices.length; i++) {
            out[i] = v.valueAt(keptIndices[i]);
        }
        return out;
    }

    private static MemorySegment[] gatherSegments(IntFunction<MemorySegment> getter, int[] keptIndices) {
        MemorySegment[] out = new MemorySegment[keptIndices.length];
        for (int i = 0; i < keptIndices.length; i++) {
            out[i] = getter.apply(keptIndices[i]);
        }
        return out;
    }

    private static Validity gatherValidity(ColumnVector v, int[] keptIndices) {
        return gatherValidity(v.validity(), keptIndices);
    }

    public static Validity gatherValidity(Validity source, int[] keptIndices) {
        BitSet out = new BitSet(keptIndices.length);
        for (int i = 0; i < keptIndices.length; i++) {
            if (source.isValid(keptIndices[i])) {
                out.set(i);
            }
        }
        return Validity.of(out, keptIndices.length);
    }
}
