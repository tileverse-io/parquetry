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

/**
 * Per-purpose byte and fetch tally for one query. Call {@link #add} unconditionally at the I/O site; install
 * {@link #NONE} when no observer is attached and the calls become no-ops the JIT inlines away. The active form is
 * thread-safe and shared across the concurrent fetch threads.
 */
public sealed interface FetchAccumulator permits ConcurrentFetchAccumulator, FetchAccumulator.NoOp {

    FetchAccumulator NONE = new NoOp();

    /**
     * Records one call to the byte source: {@code requests} ranges totalling {@code bytes}, served below at a cost of
     * {@code backendFetches} requests, {@code bytesTransferred} bytes across the wire and {@code bytesFromCache} bytes
     * out of a cache.
     */
    void add(
            FetchPurpose purpose,
            long bytes,
            int requests,
            long backendFetches,
            long bytesTransferred,
            long bytesFromCache);

    /**
     * Records {@code requests} further ranges of a call already counted by {@link #add}, totalling {@code bytes} for
     * {@code purpose}. A call with ranges of several purposes reports one of them through {@link #add} and the rest
     * through this, splitting the bytes by purpose without counting the call more than once.
     */
    void addRangesOfSameCall(FetchPurpose purpose, long bytes, int requests);

    /** Records one call to the byte source for a single range, through a verb that reports no transport cost. */
    default void add(FetchPurpose purpose, long bytes) {
        add(purpose, bytes, 1, 0, 0, 0);
    }

    FetchStats snapshot();

    static FetchAccumulator active() {
        return new ConcurrentFetchAccumulator();
    }

    /** No-op null object: zero state, zero work, empty stats. */
    final class NoOp implements FetchAccumulator {

        private NoOp() {}

        @Override
        public void add(
                FetchPurpose purpose,
                long bytes,
                int requests,
                long backendFetches,
                long bytesTransferred,
                long bytesFromCache) {
            // intentional no-op: the null object discards every measurement
        }

        @Override
        public void addRangesOfSameCall(FetchPurpose purpose, long bytes, int requests) {
            // intentional no-op: the null object discards every measurement
        }

        @Override
        public FetchStats snapshot() {
            return FetchStats.EMPTY;
        }
    }
}
