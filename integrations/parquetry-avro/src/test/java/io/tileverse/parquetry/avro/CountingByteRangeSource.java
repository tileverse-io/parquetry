/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.avro;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicLong;

import io.tileverse.parquetry.io.ByteRangeSource;

/** Wraps a source and records the highest offset+length ever read, to assert lazy block reading. */
final class CountingByteRangeSource implements ByteRangeSource {

    private final ByteRangeSource delegate;
    private final AtomicLong maxReadEnd = new AtomicLong();

    CountingByteRangeSource(ByteRangeSource delegate) {
        this.delegate = delegate;
    }

    long maxReadEnd() {
        return maxReadEnd.get();
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public int read(long offset, MemorySegment dst) {
        int read = delegate.read(offset, dst);
        if (read > 0) {
            maxReadEnd.accumulateAndGet(offset + read, Math::max);
        }
        return read;
    }

    @Override
    public void close() {
        delegate.close();
    }
}
