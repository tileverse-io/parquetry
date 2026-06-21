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
package io.tileverse.parquetry.io;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/** Small {@link MemorySegment} helpers shared across the reader. */
public final class Segments {

    private Segments() {}

    /**
     * Copies {@code source}'s bytes into a fresh heap-backed read-only segment, severing any aliasing with an off-heap
     * or transient backing. The returned segment owns its bytes and outlives {@code source}, which makes it safe to
     * retain a read value past the batch whose page buffers backed it.
     */
    public static MemorySegment toHeapReadOnly(MemorySegment source) {
        return MemorySegment.ofArray(source.toArray(ValueLayout.JAVA_BYTE)).asReadOnly();
    }
}
