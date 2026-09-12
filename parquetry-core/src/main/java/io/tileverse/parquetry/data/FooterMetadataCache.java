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
package io.tileverse.parquetry.data;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;

import io.tileverse.parquetry.internal.footer.CompactFooter;

import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CaffeineCache;

/**
 * The process-wide cache of {@link FooterMetadata}, keyed by a file's name and length. A server that reopens the same
 * files across requests - a dataset of several hundred Parquet files answering one map request after another - reads
 * and parses each footer once instead of once per open.
 *
 * <p>The cache is weighed by what an entry retains (the packed planning footer, the key/value metadata, the schema, and
 * the spatial bounds of the file's geometry columns) and bounded to a share of the heap, by default 10%, overridable
 * with the {@value #MAX_HEAP_PERCENT_PROPERTY} system property. It registers with the shared tileverse
 * {@link CacheManager}, alongside the storage layer's range cache.
 *
 * <p>An entry unused for ten minutes expires. The memory of an expired or evicted entry returns once no open reader
 * over that file still holds the forms derived from it. The per-file statistics kept by a catalog are copies and hold
 * none of that memory.
 *
 * <p>Two kinds of open never reach it: one over a source that does not name itself, which has no stable identity to key
 * on, and one that received a decryption key, whose metadata must not outlive the reader holding the key.
 */
public final class FooterMetadataCache {

    /** System property setting the maximum share of the heap available to the cache, as a whole percentage. */
    public static final String MAX_HEAP_PERCENT_PROPERTY = "io.tileverse.parquetry.footercache.maxheappercent";

    static final String CACHE_NAME = "parquetry-footer-metadata";

    private static final int DEFAULT_MAX_HEAP_PERCENT = 10;

    private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofMinutes(10);

    /**
     * Charged to every entry on top of the measured terms, covering the key, the record, and the object headers of
     * everything reachable from it. It also keeps a file with a tiny footer from looking free.
     */
    private static final int ENTRY_OVERHEAD_BYTES = 4096;

    /**
     * Charged per character of key/value metadata. Deliberately generous rather than exact: it covers the string's own
     * bytes, one per character for the Latin-1 text held by these entries, plus the map entry and headers around it.
     */
    private static final int BYTES_PER_METADATA_CHAR = 2;

    /** One leaf column's share of the retained schema tree: its node, its name, and its logical-type annotation. */
    private static final int BYTES_PER_LEAF = 256;

    /** One row group's {@link RowGroupSummary} in the retained public view. */
    private static final int BYTES_PER_ROW_GROUP = 48;

    /**
     * One bounding box as the spatial bounds source holds it: a record of four doubles and four optional axes, plus the
     * slot and the wrapper around it.
     */
    private static final int BYTES_PER_BOUNDING_BOX = 160;

    /** The entry size assumed when sizing the hash table, in the range of a many-column file's packed footer. */
    private static final int EXPECTED_ENTRY_BYTES = 64 * 1024;

    /**
     * Charged to the outcome of a load that threw, an entry dropped by its own thread before that outcome is published.
     */
    private static final int FAILED_LOAD_BYTES = 64;

    /** Ceiling on the size hint for the hash table, short of the largest table that the cache can allocate. */
    private static final int MAX_INITIAL_CAPACITY = 1_000_000_000;

    private static final int MAX_HEAP_PERCENT = configuredMaxHeapPercent();

    private FooterMetadataCache() {}

    /**
     * The metadata cached for the file named {@code sourceId} at {@code fileLength} bytes, obtained from {@code loader}
     * on a miss. Concurrent callers for one key share a single load; the rest wait for its result.
     *
     * <p>The cache's own lock covers no more than the installation of an empty slot. The caller that installs one reads
     * and parses the footer on its own thread outside that lock, which leaves every other file of the same hash bin
     * free to load meanwhile.
     *
     * <p>Every caller of a load that threw receives the throwable of the loading thread itself, stack trace of that
     * thread included, and the next open of the file loads it again.
     */
    static FooterMetadata get(String sourceId, long fileLength, Supplier<FooterMetadata> loader) {
        FooterKey key = new FooterKey(sourceId, fileLength);
        AsyncCache<FooterKey, LoadResult> entries = cache().asyncCache();
        CompletableFuture<LoadResult> claim = new CompletableFuture<>();
        CompletableFuture<LoadResult> entry = entries.get(key, (_, _) -> claim);
        boolean loadFallsToThisCaller = entry == claim;
        LoadResult result;
        if (loadFallsToThisCaller) {
            result = runLoad(entries, key, claim, loader);
        } else {
            result = entry.join();
        }
        return metadataOf(result);
    }

    /** Runs {@code loader} on the calling thread and publishes its outcome to the callers waiting on {@code claim}. */
    private static LoadResult runLoad(
            AsyncCache<FooterKey, LoadResult> entries,
            FooterKey key,
            CompletableFuture<LoadResult> claim,
            Supplier<FooterMetadata> loader) {
        LoadResult result = load(loader);
        publishOutcome(entries, key, claim, result);
        return result;
    }

    /** The outcome of one run of {@code loader}: what it returned, or the throwable thrown by it. */
    // java:S1181 - an error on the loading thread is an outcome of that load like any other; a claim that no outcome
    // reaches would strand every caller waiting on it.
    @SuppressWarnings("java:S1181")
    private static LoadResult load(Supplier<FooterMetadata> loader) {
        try {
            return new LoadResult.Loaded(loader.get());
        } catch (RuntimeException failure) {
            return new LoadResult.Failed(failure);
        } catch (Error fatal) {
            return new LoadResult.Fatal(fatal);
        }
    }

    /**
     * Hands {@code result} to the callers waiting on {@code claim}. The entry of a load that threw is dropped first and
     * the outcome published second: no caller arriving after the drop picks that failure up, and the next open of the
     * file loads it again. The publish stands even when the drop itself fails, which leaves nobody waiting on a claim
     * that no outcome reaches.
     */
    private static void publishOutcome(
            AsyncCache<FooterKey, LoadResult> entries,
            FooterKey key,
            CompletableFuture<LoadResult> claim,
            LoadResult result) {
        boolean loadCompleted = result instanceof LoadResult.Loaded;
        try {
            if (!loadCompleted) {
                dropEntry(entries, key, claim);
            }
        } finally {
            claim.complete(result);
        }
    }

    /** Drops the entry for {@code key} if it still holds {@code claim}, leaving a later claim on it untouched. */
    private static void dropEntry(
            AsyncCache<FooterKey, LoadResult> entries, FooterKey key, CompletableFuture<LoadResult> claim) {
        ConcurrentMap<FooterKey, CompletableFuture<LoadResult>> byKey = entries.asMap();
        byKey.remove(key, claim);
    }

    /** The metadata of a load that completed. A load that threw has the throwable of its loader rethrown here. */
    private static FooterMetadata metadataOf(LoadResult result) {
        return switch (result) {
            case LoadResult.Loaded(FooterMetadata metadata) -> metadata;
            case LoadResult.Failed(RuntimeException failure) -> throw failure;
            case LoadResult.Fatal(Error failure) -> throw failure;
        };
    }

    /**
     * Discards the entries present in the cache; a reader opened afterwards reads and parses its footer again. An entry
     * whose load is still in flight is outside what the underlying cache contract defines and may survive the call.
     * Exists for host lifecycle hooks that reset a running process without restarting it.
     */
    public static void clear() {
        cache().invalidateAll();
    }

    private static AsyncFooterCache cache() {
        return CacheManager.getDefault().getCache(CACHE_NAME, FooterMetadataCache::buildCache);
    }

    private static AsyncFooterCache buildCache() {
        Weigher<FooterKey, LoadResult> weigher = (_, result) -> weigh(result);
        AsyncCache<FooterKey, LoadResult> entries = Caffeine.newBuilder()
                .weigher(weigher)
                .maximumWeight(maximumWeightBytes())
                .initialCapacity(initialCapacity())
                .expireAfterAccess(EXPIRE_AFTER_ACCESS)
                // An idle process evicts expired entries only when something touches the cache; the scheduler returns
                // that memory on time instead.
                .scheduler(Scheduler.systemScheduler())
                .recordStats()
                .buildAsync();
        return new AsyncFooterCache(entries);
    }

    /** The share of the heap available to the cache, in bytes. */
    private static long maximumWeightBytes() {
        long heapBytes = Runtime.getRuntime().maxMemory();
        return (long) (heapBytes * (MAX_HEAP_PERCENT / 100d));
    }

    /**
     * The size hint for the cache's hash table: the entries admitted by the budget, at four fifths of that count. A
     * table sized well below its eventual population pays repeated resizing and the allocation churn behind it, and one
     * sized above it holds slots for entries that the budget never admits.
     */
    private static int initialCapacity() {
        long entriesWithinBudget = maximumWeightBytes() / EXPECTED_ENTRY_BYTES;
        long sizeHint = entriesWithinBudget * 4 / 5;
        return (int) Math.min(sizeHint, MAX_INITIAL_CAPACITY);
    }

    /**
     * What one entry costs the heap: what its metadata retains, or a token charge for the outcome of a load that threw,
     * an outcome about to be dropped.
     */
    private static int weigh(LoadResult result) {
        return switch (result) {
            case LoadResult.Loaded(FooterMetadata metadata) -> weigh(metadata);
            case LoadResult.Failed _ -> FAILED_LOAD_BYTES;
            case LoadResult.Fatal _ -> FAILED_LOAD_BYTES;
        };
    }

    /**
     * What one entry costs the heap: the packed footer plus the objects retained alongside it. The key/value metadata
     * is the term to watch - a Spark {@code org.apache.spark.sql.parquet.row.metadata}, an Iceberg schema, or a
     * GeoParquet PROJJSON definition each run to hundreds of kilobytes, and a footer with one outweighs its packed blob
     * many times over. The per-leaf, per-row-group, and per-box terms stand in for the schema tree, the public
     * row-group view, and the bounds decoded into the spatial bounds source, each of which grows with the file; the
     * fixed term covers the key, the record, and the rest.
     *
     * <p>The bounds source is asked how many boxes it holds rather than asking the packed footer how many geospatial
     * extents it records. The two agree only for a GeoParquet 2.0 file: a 1.1 file records no extent yet decodes a box
     * per row group out of its covering columns, and on a file of tens of thousands of row groups those boxes run to
     * megabytes.
     */
    static int weigh(FooterMetadata metadata) {
        CompactFooter compactFooter = metadata.compactFooter();
        long weight = compactFooter.byteSize()
                + BYTES_PER_METADATA_CHAR * metadataChars(metadata.keyValueMetadata())
                + (long) BYTES_PER_LEAF * compactFooter.leafCount()
                + (long) BYTES_PER_ROW_GROUP * compactFooter.rowGroupCount()
                + (long) BYTES_PER_BOUNDING_BOX * metadata.spatialBounds().retainedBoxCount()
                + ENTRY_OVERHEAD_BYTES;
        return (int) Math.min(weight, Integer.MAX_VALUE);
    }

    private static long metadataChars(Map<String, String> keyValueMetadata) {
        long chars = 0;
        for (Map.Entry<String, String> entry : keyValueMetadata.entrySet()) {
            chars += entry.getKey().length();
            chars += entry.getValue().length();
        }
        return chars;
    }

    /**
     * The heap share set by {@value #MAX_HEAP_PERCENT_PROPERTY}, falling back to the default of
     * {@value #DEFAULT_MAX_HEAP_PERCENT} for an absent value and for one that is not a whole percentage. Read once,
     * when this class is initialized; setting the property later changes nothing.
     */
    static int configuredMaxHeapPercent() {
        String configured = System.getProperty(MAX_HEAP_PERCENT_PROPERTY);
        if (configured == null) {
            return DEFAULT_MAX_HEAP_PERCENT;
        }
        try {
            int percent = Integer.parseInt(configured.trim());
            return (percent >= 0 && percent <= 100) ? percent : DEFAULT_MAX_HEAP_PERCENT;
        } catch (NumberFormatException _) {
            return DEFAULT_MAX_HEAP_PERCENT;
        }
    }

    /**
     * The identity of a file's footer: what the source calls itself and how long the file is. A file rewritten to a
     * different length under the same name misses, and the rewritten footer is read.
     */
    private record FooterKey(String sourceId, long fileLength) {}

    /**
     * The outcome of one footer load, held by the entry that the load was claimed under. It is the file's metadata when
     * the loader returned, and the throwable of the loading thread when the loader threw: an exception or an error,
     * since a {@link Supplier} declares no checked exception.
     */
    private sealed interface LoadResult {

        /** The metadata of a load that completed. */
        record Loaded(FooterMetadata metadata) implements LoadResult {}

        /** The exception thrown by a loader, held for every caller of that load. */
        record Failed(RuntimeException failure) implements LoadResult {}

        /** The error thrown by a loader, held for every caller of that load. */
        record Fatal(Error failure) implements LoadResult {}
    }

    /**
     * The registered form of the cache: the view of loaded entries held by the shared {@link CacheManager}, together
     * with the asynchronous cache behind it, where every entry is claimed before it is loaded.
     */
    private static final class AsyncFooterCache extends CaffeineCache<FooterKey, LoadResult> {

        private final AsyncCache<FooterKey, LoadResult> asyncCache;

        AsyncFooterCache(AsyncCache<FooterKey, LoadResult> asyncCache) {
            super(asyncCache.synchronous());
            this.asyncCache = asyncCache;
        }

        /** The cache that entries are loaded through, one claim per key. */
        AsyncCache<FooterKey, LoadResult> asyncCache() {
            return asyncCache;
        }
    }
}
