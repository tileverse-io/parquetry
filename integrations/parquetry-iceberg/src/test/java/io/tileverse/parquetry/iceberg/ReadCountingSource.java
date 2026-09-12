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
package io.tileverse.parquetry.iceberg;

import java.lang.foreign.MemorySegment;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Counts how often a wrapped source is asked to read, leaving its identity and length untouched. Keeping the identity
 * is the point: a test that swallowed it would send every open past the footer-metadata cache and measure the wrong
 * thing.
 */
record ReadCountingSource(ByteRangeSource delegate, AtomicInteger readCount) implements ByteRangeSource {

    ReadCountingSource(ByteRangeSource delegate) {
        this(delegate, new AtomicInteger());
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public Optional<String> sourceIdentifier() {
        return delegate.sourceIdentifier();
    }

    @Override
    public int read(long offset, MemorySegment dst) {
        readCount.incrementAndGet();
        return delegate.read(offset, dst);
    }

    @Override
    public void close() {
        delegate.close();
    }

    int reads() {
        return readCount.get();
    }

    void resetReads() {
        readCount.set(0);
    }
}
