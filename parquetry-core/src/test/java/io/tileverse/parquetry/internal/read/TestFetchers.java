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

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Builds {@link RowGroupFetcher}s for tests over a caller-supplied {@link SegmentPool}, wrapping it in a
 * {@link FetchBufferAllocator} backed by an ample fetch budget. With ample budget the mandatory path takes the RAM
 * branch and goes straight to {@code pool}, preserving each test's pool accounting; the disk-spill arm exists but is
 * never reached.
 */
final class TestFetchers {

    private static final long AMPLE_FETCH_BUDGET = 1L << 30;
    private static final long AMPLE_DISK_BUDGET = 1L << 30;

    private TestFetchers() {}

    static RowGroupFetcher over(
            ByteRangeSource source, ParquetSchema fileSchema, ParquetSchema projectedSchema, SegmentPool pool) {
        FetchBufferAllocator allocator = new FetchBufferAllocator(
                pool,
                FetchBudget.ofBytes(AMPLE_FETCH_BUDGET),
                new FetchSpillStore(spillDir(), DiskBudget.ofBytes(AMPLE_DISK_BUDGET)));
        return new RowGroupFetcher(source, fileSchema, projectedSchema, pool, allocator, 1 << 20, 8 << 20);
    }

    private static Path spillDir() {
        try {
            Path dir = Files.createTempDirectory("parquetry-test-fetch-spill-");
            dir.toFile().deleteOnExit();
            return dir;
        } catch (IOException e) {
            throw new UncheckedIOException("Could not create a temp spill directory for a test fetcher", e);
        }
    }
}
