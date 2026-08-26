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
package io.tileverse.parquetry.internal.write;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Unsigned lexicographic order over raw bytes, matching Parquet's default {@code ColumnOrder} for {@code BYTE_ARRAY} /
 * {@code FIXED_LEN_BYTE_ARRAY}: bytes compare unsigned left to right, and on prefix equality the shorter value orders
 * first. The single ordering implementation shared by the write-side statistics accumulator and the column-index
 * builder.
 */
final class UnsignedLexOrder {

    private UnsignedLexOrder() {}

    /** Negative when {@code a} orders below {@code b}, positive above, {@code 0} on byte equality. */
    static int compare(MemorySegment a, MemorySegment b) {
        long mismatch = a.mismatch(b);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == a.byteSize()) {
            return -1;
        }
        if (mismatch == b.byteSize()) {
            return 1;
        }
        return Byte.compareUnsigned(a.get(ValueLayout.JAVA_BYTE, mismatch), b.get(ValueLayout.JAVA_BYTE, mismatch));
    }
}
