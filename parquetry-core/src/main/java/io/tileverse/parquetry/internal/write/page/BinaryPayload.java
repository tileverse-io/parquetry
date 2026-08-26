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

/** A page's non-null binary values, read positionally by the binary encoders without a per-value byte[]. */
public sealed interface BinaryPayload permits PackedBinaryPayload, ArrayBinaryPayload {

    /** The number of values. */
    int count();

    /** The byte length of value {@code i}. */
    int length(int i);

    /** Appends value {@code i}'s bytes to {@code dst}. */
    void writeValueInto(int i, LittleEndianSink dst);

    /** Value {@code i} as a byte[]; the packed impl copies, the array impl returns its own array. */
    byte[] valueAt(int i);
}
