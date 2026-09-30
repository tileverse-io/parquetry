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
package io.tileverse.parquetry.io;

import java.lang.foreign.MemorySegment;

/** The argument checks shared by every {@link ByteRangeSource#read} implementation in this package. */
final class ReadArguments {

    private ReadArguments() {}

    /**
     * @throws IllegalArgumentException if {@code offset} is negative, {@code dst} is read-only, or {@code dst} is
     *     larger than {@link Integer#MAX_VALUE} bytes
     */
    static void requireReadable(long offset, MemorySegment dst) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, got " + offset);
        }
        if (dst.isReadOnly()) {
            throw new IllegalArgumentException("dst must be writable");
        }
        if (dst.byteSize() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("dst.byteSize() must be <= Integer.MAX_VALUE, got " + dst.byteSize());
        }
    }
}
