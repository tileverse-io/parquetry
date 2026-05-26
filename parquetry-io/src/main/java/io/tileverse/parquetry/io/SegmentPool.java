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
package io.tileverse.parquetry.io;

import java.lang.foreign.MemorySegment;

/**
 * A pool of native {@link MemorySegment}s for column-chunk fetch and per-page decompression buffers. Borrowing reuses
 * native memory to keep the streaming memory budget bounded; closing the handle returns the backing segment.
 *
 * <p>Implementations support concurrent {@link #borrow} calls from multiple threads. A returned {@link Pooled} handle,
 * by contrast, is owned by a single borrower and is not safe to share across threads. The process-wide
 * {@link #getDefault() default} is a small bounded JDK-only pool. An adapter module can supply an implementation backed
 * by a shared pool that a co-resident reader already uses; a single physical pool then serves several readers.
 */
public interface SegmentPool {

    /** Borrows a native segment of exactly {@code byteSize} bytes; return it by closing the handle. */
    Pooled borrow(long byteSize);

    /** Process-wide shared default: a small bounded pool of native segments, JDK-only. */
    static SegmentPool getDefault() {
        return DefaultSegmentPool.INSTANCE;
    }

    /**
     * A borrowed segment owned by a single borrower. The owner closes it exactly once when done; repeated
     * {@link #close()} calls from that owner are a no-op. A {@code Pooled} handle is not designed to be closed
     * concurrently from multiple threads.
     */
    interface Pooled extends AutoCloseable {

        /**
         * The borrowed segment, sized to the requested byte length. Valid only until {@link #close()}; reading or
         * writing it afterwards is undefined, as the underlying native memory may have been handed to another borrow.
         */
        MemorySegment segment();

        /** Returns the segment to the pool. Idempotent for the owning borrower. */
        @Override
        void close();
    }
}
