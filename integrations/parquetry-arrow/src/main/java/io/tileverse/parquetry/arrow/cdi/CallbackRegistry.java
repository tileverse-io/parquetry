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
package io.tileverse.parquetry.arrow.cdi;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A registry of per-export callback state keyed by an integer an exporter stashes in a C struct's {@code private_data}.
 * The release upcall reads {@code private_data} back to find and run the state's reclaim. {@link #register} returns the
 * key as a pointer-typed segment to store; {@link #lookup} and {@link #remove} take the struct's {@code private_data}
 * segment. Removal is idempotent: a second release finds nothing.
 *
 * <p>{@link #outstanding()} reports the registered-but-not-released count. The C Data Interface puts the release
 * obligation on the consumer, and the Java side keeps no handle the consumer holds; this count is therefore the only
 * signal of a consumer that imported an export and never released it (which leaks the state's pooled buffers and
 * arena).
 *
 * @param <T> the per-export state type (the reclaim bookkeeping or the stream state)
 */
final class CallbackRegistry<T> {

    private final ConcurrentHashMap<Long, T> entries = new ConcurrentHashMap<>();
    private final AtomicLong keys = new AtomicLong(1);

    /** Registers {@code state} and returns the key segment to store in the struct's {@code private_data}. */
    MemorySegment register(T state) {
        long key = keys.getAndIncrement();
        entries.put(key, state);
        return MemorySegment.ofAddress(key);
    }

    /**
     * The state for the struct whose {@code private_data} is {@code privateData}, or {@code null} if not registered.
     */
    T lookup(MemorySegment privateData) {
        return entries.get(privateData.address());
    }

    /** Removes and returns the state for {@code privateData}, or {@code null} on a second (idempotent) release. */
    T remove(MemorySegment privateData) {
        return entries.remove(privateData.address());
    }

    /** The number of registered-but-not-yet-released exports, for leak observability. */
    int outstanding() {
        return entries.size();
    }
}
