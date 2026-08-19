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
package io.tileverse.parquetry.internal.write.page;

import java.util.Arrays;

/**
 * One growable byte[] backing plus per-value (offset, length). Aliases the caller's arrays - valid only for the
 * synchronous encode that follows, before the value store is cleared.
 */
public final class PackedBinaryPayload implements BinaryPayload {

    private final byte[] backing;
    private final int[] offsets;
    private final int[] lengths;
    private final int count;

    public PackedBinaryPayload(byte[] backing, int[] offsets, int[] lengths, int count) {
        this.backing = backing;
        this.offsets = offsets;
        this.lengths = lengths;
        this.count = count;
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public int length(int i) {
        return lengths[i];
    }

    @Override
    public void writeValueInto(int i, LittleEndianSink dst) {
        dst.write(backing, offsets[i], lengths[i]);
    }

    @Override
    public byte[] valueAt(int i) {
        int off = offsets[i];
        return Arrays.copyOfRange(backing, off, off + lengths[i]);
    }
}
