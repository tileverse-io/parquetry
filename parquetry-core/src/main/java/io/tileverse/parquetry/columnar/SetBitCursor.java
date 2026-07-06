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

import java.util.BitSet;

/**
 * Translates a logical position (the {@code n}-th set bit, zero-based) to its physical bit index in a {@link BitSet},
 * optimized for the access pattern this codebase actually uses: non-decreasing logical positions. A
 * {@link Selection.Bits} selection and a selection-projected {@link Validity} are both physical-row bit masks read in
 * logical order; they share this one translation machinery.
 *
 * <p>The cursor remembers the last {@code (logical, physical)} pair. A forward step walks the set bits from the cursor;
 * a backward or first request rescans from the start (correct, O(distance), and rare). Not thread-safe: a batch is
 * consumed by a single thread during iteration.
 */
final class SetBitCursor {

    private final BitSet bits;
    private int lastLogical = -1;
    private int lastPhysical = -1;

    SetBitCursor(BitSet bits) {
        this.bits = bits;
    }

    /**
     * The physical bit index of the {@code logical}-th set bit (zero-based), or {@code -1} when {@code logical} is at
     * or beyond the number of set bits. Sequential-optimized: amortized O(1) for non-decreasing {@code logical}.
     */
    int physical(int logical) {
        if (logical == lastLogical) {
            return lastPhysical;
        }
        int physical;
        if (logical > lastLogical && lastPhysical >= 0) {
            physical = advanceForward(logical - lastLogical);
        } else {
            physical = nthSetBit(bits, logical);
        }
        lastLogical = logical;
        lastPhysical = physical;
        return physical;
    }

    private int advanceForward(int steps) {
        int physical = lastPhysical;
        for (int step = 0; step < steps; step++) {
            physical = bits.nextSetBit(physical + 1);
            if (physical < 0) {
                return -1;
            }
        }
        return physical;
    }

    /** The physical index of the {@code n}-th set bit (zero-based) of {@code bits}, or {@code -1} when fewer exist. */
    static int nthSetBit(BitSet bits, int n) {
        int physical = bits.nextSetBit(0);
        for (int seen = 0; seen < n && physical >= 0; seen++) {
            physical = bits.nextSetBit(physical + 1);
        }
        return physical;
    }
}
