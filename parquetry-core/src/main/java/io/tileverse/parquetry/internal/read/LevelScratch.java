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
package io.tileverse.parquetry.internal.read;

import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.columnar.Levels;
import io.tileverse.parquetry.internal.read.page.LevelDecoder;

/**
 * A column reader's reusable off-heap buffer for one level stream (rep or def). {@link #decode} fills the buffer from
 * the loaded decoder and returns a {@link Levels} view over it. The view is valid until the next {@link #decode} call
 * on the same scratch: the reader's next page decode. The buffer is a {@link SegmentScratch}, released at
 * {@link #close()}.
 */
final class LevelScratch implements AutoCloseable {

    private final SegmentScratch buffer;

    LevelScratch(DecodeBufferAllocator allocator) {
        this.buffer = new SegmentScratch(allocator);
    }

    /** Decodes {@code count} levels from the loaded {@code decoder} into this scratch; see the class contract. */
    Levels decode(LevelDecoder decoder, int count) {
        MemorySegment dst = buffer.acquire(4L * count);
        decoder.decodeInto(count, dst);
        return Levels.ofSegment(dst, count);
    }

    /** Returns the buffer to the pool; idempotent. */
    @Override
    public void close() {
        buffer.close();
    }
}
