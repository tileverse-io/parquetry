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
package io.tileverse.parquetry.arrow.columnar;

import java.lang.foreign.MemorySegment;

/**
 * One Arrow-layout buffer of a column vector, tagged by the role it plays within its {@link EncodedNode}.
 *
 * <p>The {@code bytes} are read-only; the buffer owns no copy semantics of its own and is meant to be sliced from an
 * encoded payload and read back as-is.
 *
 * @param role what the bytes mean within the node (validity bitmap, value offsets, packed values, dictionary indices)
 * @param bytes the read-only buffer contents in Arrow layout
 */
public record EncodedBuffer(BufferRole role, MemorySegment bytes) {

    /** The meaning a buffer has within an Arrow field node. */
    public enum BufferRole {
        VALIDITY,
        OFFSETS,
        DATA,
        DICTIONARY_INDICES
    }
}
