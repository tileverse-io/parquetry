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
package io.tileverse.parquetry.arrow.cdi;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.io.SegmentPool;

/**
 * The reclaim bookkeeping for one exported {@code ArrowArray} tree: every pooled buffer borrowed across the tree plus
 * the arena holding the array structs and pointer scaffolding. The consumer triggers {@link #release()} once on the top
 * array; releasing returns every borrowed buffer to the pool and frees the scaffolding arena. Release is idempotent and
 * thread-safe, since the C Data Interface lets a consumer release from an arbitrary thread.
 */
final class ExportedBuffers {

    private final Arena scaffolding;
    private final List<SegmentPool.Pooled> borrowed = new ArrayList<>();
    private boolean released = false;

    ExportedBuffers(Arena scaffolding) {
        this.scaffolding = scaffolding;
    }

    /** Records a borrowed buffer to return when the array is released. Called only during export, before handoff. */
    void track(SegmentPool.Pooled pooled) {
        borrowed.add(pooled);
    }

    /** Returns every borrowed buffer to the pool and frees the scaffolding arena. Idempotent. */
    synchronized void release() {
        if (released) {
            return;
        }
        released = true;
        for (SegmentPool.Pooled pooled : borrowed) {
            pooled.close();
        }
        scaffolding.close();
    }
}
