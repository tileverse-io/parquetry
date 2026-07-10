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
package io.tileverse.parquetry.observe;

import java.util.concurrent.atomic.LongAdder;

/** Thread-safe active {@link FetchAccumulator}; shared across one query's fetch threads. */
final class ConcurrentFetchAccumulator implements FetchAccumulator {

    private final LongAdder pageBytes = new LongAdder();
    private final LongAdder dictionaryBytes = new LongAdder();
    private final LongAdder columnIndexBytes = new LongAdder();
    private final LongAdder offsetIndexBytes = new LongAdder();
    private final LongAdder bloomFilterBytes = new LongAdder();
    private final LongAdder fetchCount = new LongAdder();

    @Override
    public void add(FetchPurpose purpose, long bytes) {
        bucketFor(purpose).add(bytes);
        fetchCount.increment();
    }

    @Override
    public FetchStats snapshot() {
        return new FetchStats(
                pageBytes.sum(),
                dictionaryBytes.sum(),
                columnIndexBytes.sum(),
                offsetIndexBytes.sum(),
                bloomFilterBytes.sum(),
                (int) fetchCount.sum());
    }

    private LongAdder bucketFor(FetchPurpose purpose) {
        return switch (purpose) {
            case PAGES -> pageBytes;
            case DICTIONARY -> dictionaryBytes;
            case COLUMN_INDEX -> columnIndexBytes;
            case OFFSET_INDEX -> offsetIndexBytes;
            case BLOOM_FILTER -> bloomFilterBytes;
        };
    }
}
