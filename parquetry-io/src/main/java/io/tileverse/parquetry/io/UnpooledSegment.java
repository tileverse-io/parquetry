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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * A large borrowed segment that owns its arena. {@link #close()} closes the arena, freeing the native memory
 * immediately rather than returning the backing to {@link DefaultSegmentPool}. Close is idempotent for the owner.
 */
final class UnpooledSegment implements SegmentPool.Pooled {

    private final MemorySegment view;
    private Arena arena;

    UnpooledSegment(Arena arena, MemorySegment view) {
        this.arena = arena;
        this.view = view;
    }

    @Override
    public MemorySegment segment() {
        return view;
    }

    @Override
    public void close() {
        if (arena == null) {
            return;
        }
        arena.close();
        arena = null;
    }
}
