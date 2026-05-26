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
package io.tileverse.parquetry.testsupport;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicLong;

import io.tileverse.parquetry.io.SegmentPool;

/** A {@link SegmentPool} that tracks how many borrows are still outstanding, for leak assertions in tests. */
public final class CountingSegmentPool implements SegmentPool {

    private final SegmentPool delegate = SegmentPool.getDefault();
    private final AtomicLong outstanding = new AtomicLong();

    /** Borrows still open (incremented on borrow, decremented on the handle's first close). */
    public long outstanding() {
        return outstanding.get();
    }

    @Override
    public Pooled borrow(long byteSize) {
        Pooled delegated = delegate.borrow(byteSize);
        outstanding.incrementAndGet();
        return new Pooled() {
            private boolean closed;

            @Override
            public MemorySegment segment() {
                return delegated.segment();
            }

            @Override
            public void close() {
                if (closed) {
                    return;
                }
                closed = true;
                delegated.close();
                outstanding.decrementAndGet();
            }
        };
    }
}
