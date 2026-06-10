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
package io.tileverse.parquetry.internal.read;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import io.tileverse.parquetry.io.SegmentPool;

/**
 * Builds {@link DecodeBufferAllocator}s for tests over the default {@link SegmentPool} with an ample off-heap decode
 * budget. With ample budget every mandatory acquire takes the RAM branch and reserves against the budget; the
 * disk-spill arm exists but is never reached.
 */
final class TestDecodeBuffers {

    private static final long AMPLE_DECODE_BUDGET = 1L << 30;
    private static final long AMPLE_DISK_BUDGET = 1L << 30;

    private TestDecodeBuffers() {}

    static DecodeBufferAllocator ample() {
        return ample(SegmentPool.getDefault());
    }

    /** Same ample budgets over a caller-supplied pool, letting a test assert on that pool's borrow accounting. */
    static DecodeBufferAllocator ample(SegmentPool pool) {
        return new DecodeBufferAllocator(
                pool,
                DecodeBudget.ofBytes(AMPLE_DECODE_BUDGET),
                new FetchSpillStore(spillDir(), DiskBudget.ofBytes(AMPLE_DISK_BUDGET)));
    }

    private static Path spillDir() {
        try {
            Path dir = Files.createTempDirectory("parquetry-test-decode-spill-");
            dir.toFile().deleteOnExit();
            return dir;
        } catch (IOException e) {
            throw new UncheckedIOException("Could not create a temp spill directory for a test decode allocator", e);
        }
    }
}
