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
package io.tileverse.parquetry.columnar;

import java.lang.foreign.MemorySegment;

/**
 * Reads a binary value in place from the segment that backs it. A vector passes the value's own backing segment plus
 * its {@code offset} and {@code length}; the view reads without slicing or copying, which lets a caller consume a
 * binary cell without allocating a per-value {@link MemorySegment} slice.
 *
 * <p>The backing segment is valid only for the duration of the call; a view that needs the bytes past the call copies
 * them out.
 *
 * @param <R> the value the view produces from the bytes
 */
@FunctionalInterface
public interface BinaryView<R> {

    /** Reads the value spanning {@code [offset, offset + length)} of {@code backing}. */
    R read(MemorySegment backing, long offset, long length);
}
