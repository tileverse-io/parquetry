/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.tileverse;

import java.util.Objects;

import io.tileverse.parquetry.io.SegmentPool;

import io.tileverse.io.ByteBufferPool;

/** Factories for {@link SegmentPool}s backed by tileverse-storage pools. */
public final class SegmentPools {

    private SegmentPools() {}

    /**
     * Returns a {@link SegmentPool} that borrows from {@code pool}. Wire parquetry's
     * {@code ReadOptions.segmentPool(...)} to a pool created over the same {@link ByteBufferPool} a co-resident reader
     * uses; one physical pool then serves both.
     */
    public static SegmentPool backedBy(ByteBufferPool pool) {
        return new ByteBufferPoolSegmentPool(Objects.requireNonNull(pool, "pool"));
    }
}
