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
package io.tileverse.parquetry.columnar;

import java.util.BitSet;
import java.util.function.IntConsumer;

/**
 * Which physical rows of a decoded column are exposed by a view, and in what order. A {@code Selection} is an index
 * map: logical row {@code j} maps to physical row {@code physical(j)}. One instance is shared across all of a batch's
 * columns, which is what makes a filtered batch a zero-copy view over the decoded source.
 *
 * <p><b>Selection is not Validity.</b> They answer different questions and must never be conflated:
 *
 * <ul>
 *   <li>{@code Selection} answers <i>which</i> physical rows are exposed and in <i>what order</i>; it is an index map,
 *       indexed by logical row {@code j}.
 *   <li>{@link Validity} answers whether the underlying physical row is <i>null</i>; it is data (the decoded null
 *       mask), indexed by physical row {@code p}.
 * </ul>
 *
 * They cannot merge, because a selected row can itself be null: {@code IsNull(col)} <i>selects</i> the null rows. A
 * vector stores exactly one {@code Selection}; its logical null mask is always derived through that selection
 * ({@code validity().select(selection)}), never stored as a second selection.
 *
 * <p>{@link #physical(int)} is <b>sequential-access-optimized</b>: a forward cursor gives amortized O(1) for
 * non-decreasing {@code j}; a backward or out-of-order {@code j} resets the cursor and rescans (correct, rare). Every
 * real consumer iterates {@code j = 0..length()-1}, which matches.
 *
 * <p>{@link #ALL} is the identity sentinel resolved by an {@code == ALL} fast-path guard at each call site (the vector
 * reads its backing directly rather than through the index map). Because {@code ALL} has no domain size, its
 * {@link #length()} and {@link #forEachPhysical(IntConsumer)} are unsupported; only {@code physical(j) == j} is
 * defined.
 */
public sealed interface Selection permits Selection.All, Selection.Range, Selection.Bits {

    /**
     * The identity selection: every physical row is exposed in order. Resolved by an {@code == ALL} guard at call
     * sites.
     */
    All ALL = new All();

    /** Number of logical rows this selection exposes. */
    int length();

    /** The physical row index for logical row {@code logical}. */
    int physical(int logical);

    /** Visits every physical row this selection exposes, in logical order; the scatter-iteration primitive. */
    void forEachPhysical(IntConsumer action);

    /**
     * A selection over the contiguous logical sub-window {@code [from, from + count)} of this selection: it exposes the
     * same physical rows this selection maps those logical positions to. Used to window a filtered batch for
     * offset/limit without re-selecting an already-selected vector.
     */
    Selection sub(int from, int count);

    /** A contiguous window {@code [start, start + length)}; used for offset/limit windowing of an unfiltered batch. */
    static Range range(int start, int length) {
        return new Range(start, length);
    }

    /** Predicate survivors: the {@code BitSet} from evaluation, reused directly (set bit == surviving row, no copy). */
    static Bits bits(BitSet survivors) {
        return new Bits(survivors);
    }

    /**
     * The identity selection. A sentinel, not a sized view: callers short-circuit on {@code == Selection.ALL} and read
     * the base vector directly. {@link #length()} and {@link #forEachPhysical(IntConsumer)} are therefore never reached
     * on it and throw to flag a missing guard. Only {@code physical(j) == j} is meaningful.
     */
    final class All implements Selection {

        private All() {}

        @Override
        public int physical(int logical) {
            return logical;
        }

        @Override
        public int length() {
            throw new UnsupportedOperationException(
                    "Selection.ALL has no length; resolve it through the base vector size via the == ALL guard");
        }

        @Override
        public void forEachPhysical(IntConsumer action) {
            throw new UnsupportedOperationException(
                    "Selection.ALL is not iterable; resolve it through the base vector via the == ALL guard");
        }

        @Override
        public Selection sub(int from, int count) {
            throw new UnsupportedOperationException(
                    "Selection.ALL has no logical window; window the base vector size directly");
        }
    }

    /** A contiguous window: logical {@code j} maps to physical {@code start + j}. */
    record Range(int start, int length) implements Selection {

        public Range {
            if (start < 0) {
                throw new IllegalArgumentException("start must be >= 0: " + start);
            }
            if (length < 0) {
                throw new IllegalArgumentException("length must be >= 0: " + length);
            }
        }

        @Override
        public int physical(int logical) {
            return start + logical;
        }

        @Override
        public void forEachPhysical(IntConsumer action) {
            for (int logical = 0; logical < length; logical++) {
                action.accept(start + logical);
            }
        }

        @Override
        public Selection sub(int from, int count) {
            return new Range(start + from, count);
        }
    }

    /**
     * Predicate survivors backed directly by the evaluation {@link BitSet} (bit {@code p} set == physical row {@code p}
     * survives). Takes ownership of the bit set; the caller must not mutate it afterward. Logical order is ascending
     * physical order; {@link #physical(int)} is cursor-optimized for non-decreasing logical access.
     */
    final class Bits implements Selection {

        private final BitSet survivors;
        private final int length;
        private final SetBitCursor cursor;

        private Bits(BitSet survivors) {
            this.survivors = survivors;
            this.length = survivors.cardinality();
            this.cursor = new SetBitCursor(survivors);
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public int physical(int logical) {
            return cursor.physical(logical);
        }

        @Override
        public void forEachPhysical(IntConsumer action) {
            for (int physical = survivors.nextSetBit(0); physical >= 0; physical = survivors.nextSetBit(physical + 1)) {
                action.accept(physical);
            }
        }

        @Override
        public Selection sub(int from, int count) {
            BitSet window = new BitSet();
            int end = from + count;
            int logical = 0;
            for (int physical = survivors.nextSetBit(0);
                    physical >= 0 && logical < end;
                    physical = survivors.nextSetBit(physical + 1), logical++) {
                if (logical >= from) {
                    window.set(physical);
                }
            }
            return new Bits(window);
        }
    }
}
