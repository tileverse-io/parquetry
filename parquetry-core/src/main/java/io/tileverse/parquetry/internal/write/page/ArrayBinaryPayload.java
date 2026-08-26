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

/** Wraps a byte[][] carrier (the dictionary sites) as a BinaryPayload with no copy. */
public final class ArrayBinaryPayload implements BinaryPayload {

    private final byte[][] values;
    private final int count;

    public ArrayBinaryPayload(byte[][] values, int count) {
        this.values = values;
        this.count = count;
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public int length(int i) {
        return values[i].length;
    }

    @Override
    public void writeValueInto(int i, LittleEndianSink dst) {
        dst.write(values[i]);
    }

    @Override
    public byte[] valueAt(int i) {
        return values[i];
    }
}
