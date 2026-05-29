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

/** A borrowed segment from {@link DefaultSegmentPool}; returns its backing on first {@link #close()}. */
final class PooledSegment implements SegmentPool.Pooled {

    private final DefaultSegmentPool pool;
    private final MemorySegment view;
    private MemorySegment backing;

    PooledSegment(DefaultSegmentPool pool, MemorySegment backing, MemorySegment view) {
        this.pool = pool;
        this.backing = backing;
        this.view = view;
    }

    @Override
    public MemorySegment segment() {
        return view;
    }

    @Override
    public void close() {
        if (backing == null) {
            return;
        }
        pool.giveBack(backing);
        backing = null;
    }
}
