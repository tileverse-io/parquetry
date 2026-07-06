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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.sun.management.ThreadMXBean;

/**
 * {@link BinaryVector#read(int, BinaryView)} hands the value's backing, offset, and length to the view and reads in
 * place. Unlike {@link BinaryVector#get(int)} it must allocate no per-value {@link MemorySegment} slice: a streaming
 * consumer (the {@code readBinary}/getString attribute path) reads every binary cell this way. This pins the per-row
 * allocation to a bound the per-value slice would blow through.
 */
class BinaryReadAllocationTest {

    private static final int ROWS = 200_000;
    private static final byte[] VALUE = "a-binary-value".getBytes(StandardCharsets.UTF_8);

    @Test
    void readDoesNotAllocateAPerValueSlice() {
        BinaryVector vector = binaryVector();
        touch(vector); // warm up the JIT

        long before = allocatedBytes();
        long sink = touch(vector);
        long allocated = allocatedBytes() - before;

        assertThat(sink).isEqualTo((long) ROWS * VALUE.length); // guard against the work being optimised away
        // The callback reads from the backing in place. A per-value slice (the regression this guards against)
        // allocated a fresh MemorySegment per row, ~4.8 MB over this run; reading in place stays near zero.
        assertThat(allocated)
                .as("reading %d binary values allocated %d bytes", ROWS, allocated)
                .isLessThan(2L * 1024 * 1024);
    }

    private static long touch(BinaryVector vector) {
        long total = 0;
        for (int row = 0; row < ROWS; row++) {
            total += vector.read(row, BinaryReadAllocationTest::length);
        }
        return total;
    }

    private static long length(MemorySegment backing, long offset, long len) {
        return len;
    }

    private static long allocatedBytes() {
        ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        return threads.getCurrentThreadAllocatedBytes();
    }

    /** A consolidated {@link BinaryVector} of {@link #ROWS} non-null rows, each holding {@link #VALUE}. */
    private static BinaryVector binaryVector() {
        MemorySegment backing = MemorySegment.ofArray(repeat(VALUE, ROWS)).asReadOnly();
        int[] offsets = new int[ROWS + 1];
        for (int i = 0; i <= ROWS; i++) {
            offsets[i] = i * VALUE.length;
        }
        return BinaryVector.of(backing, IntSequence.of(offsets), Validity.allValid(ROWS));
    }

    private static byte[] repeat(byte[] value, int times) {
        byte[] out = new byte[value.length * times];
        MemorySegment target = MemorySegment.ofArray(out);
        for (int i = 0; i < times; i++) {
            MemorySegment.copy(value, 0, target, ValueLayout.JAVA_BYTE, (long) i * value.length, value.length);
        }
        return out;
    }
}
