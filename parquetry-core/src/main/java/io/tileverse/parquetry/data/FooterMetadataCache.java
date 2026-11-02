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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;

import io.tileverse.parquetry.internal.filter.LearnedPages;
import io.tileverse.parquetry.internal.footer.CompactFooter;

import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CaffeineCache;
import lombok.NonNull;

/**
 * The process-wide cache of the metadata read from Parquet files. It holds two kinds of entry under one budget, both
 * keyed by the name and the length of a file.
 *
 * <p>A footer entry holds the {@link FooterMetadata} of a file. A server that reopens the same files across requests -
 * a dataset of several hundred Parquet files answering one map request after another - reads and parses each footer
 * once instead of once per open.
 *
 * <p>A learned entry holds the {@link LearnedPages} of one column chunk, and its key adds the row group and the column
 * of the chunk. A filtered read of a file without a page index fetches the chunks whole, and it may learn these entries
 * from their page headers. A later read of the same row group prunes pages with them.
 *
 * <p>The weight of a footer entry counts its packed planning footer, its key/value metadata, its schema and the spatial
 * bounds of the file's geometry columns. The weight of a learned entry counts the bytes of its packed pages. The two
 * kinds share a budget of 10% of the heap by default, set with the {@value #MAX_HEAP_PERCENT_PROPERTY} system property.
 * A share of 0 keeps nothing. The cache registers with the shared tileverse {@link CacheManager}, alongside the storage
 * layer's range cache.
 *
 * <p>The statistics of the cache count the hits, the misses and the entries of both kinds together. A filtered read of
 * a file without a page index looks up the learned entries of its column chunks, and these lookups dominate the hit
 * rate seen by an operator.
 *
 * <p>An entry unused for ten minutes expires. The memory of an expired or evicted entry returns once no open reader
 * over that file still holds the forms derived from it. The per-file statistics kept by a catalog are copies and hold
 * none of that memory.
 *
 * <p>An open over an unnamed source never reaches the cache, because such a source has no stable identity to key on.
 * Neither does an open given a decryption key, because its metadata must not outlive the reader holding the key.
 */
public final class FooterMetadataCache {

    /** System property setting the maximum share of the heap available to the cache, as a whole percentage. */
    public static final String MAX_HEAP_PERCENT_PROPERTY = "io.tileverse.parquetry.metadatacache.maxheappercent";

    static final String CACHE_NAME = "parquetry-footer-metadata";

    private static final int DEFAULT_MAX_HEAP_PERCENT = 10;

    private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofMinutes(10);

    /**
     * Charged to each footer entry on top of the measured terms, covering the key, the record, and the object headers
     * of everything reachable from it. It also keeps a file with a tiny footer from looking free.
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

    /**
     * Charged to each learned entry on top of its packed pages. It covers the key, the objects holding the pages, the
     * nodes of the cache and the completed future around the value.
     */
    private static final int LEARNED_ENTRY_OVERHEAD_BYTES = 288;

    /** The entry size assumed when sizing the hash table, in the range of a many-column file's packed footer. */
    private static final int EXPECTED_ENTRY_BYTES = 64 * 1024;

    /**
     * Charged to the outcome of a load that threw, an entry dropped by its own thread before that outcome is published.
     */
    private static final int FAILED_LOAD_BYTES = 64;

    /** Ceiling on the size hint for the hash table, short of the largest table that the cache can allocate. */
    private static final int MAX_INITIAL_CAPACITY = 1_000_000_000;

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
        AsyncCache<MetadataKey, CacheValue> entries = cache().asyncCache();
        CompletableFuture<CacheValue> claim = new CompletableFuture<>();
        CompletableFuture<CacheValue> entry = entries.get(key, (_, _) -> claim);
        boolean loadFallsToThisCaller = entry == claim;
        CacheValue result;
        if (loadFallsToThisCaller) {
            result = runLoad(entries, key, claim, loader);
        } else {
            result = entry.join();
        }
        return metadataOf(result);
    }

    /** Runs {@code loader} on the calling thread and publishes its outcome to the callers waiting on {@code claim}. */
    private static CacheValue runLoad(
            AsyncCache<MetadataKey, CacheValue> entries,
            FooterKey key,
            CompletableFuture<CacheValue> claim,
            Supplier<FooterMetadata> loader) {
        CacheValue result = load(loader);
        publishOutcome(entries, key, claim, result);
        return result;
    }

    /** The outcome of one run of {@code loader}: what it returned, or the throwable thrown by it. */
    // java:S1181 - an error on the loading thread is an outcome of that load like any other; a claim that no outcome
    // reaches would strand every caller waiting on it.
    @SuppressWarnings("java:S1181")
    private static CacheValue load(Supplier<FooterMetadata> loader) {
        try {
            return new CacheValue.Loaded(loader.get());
        } catch (RuntimeException failure) {
            return new CacheValue.Failed(failure);
        } catch (Error fatal) {
            return new CacheValue.Fatal(fatal);
        }
    }

    /**
     * Hands {@code result} to the callers waiting on {@code claim}. The entry of a load that threw is dropped first and
     * the outcome published second: no caller arriving after the drop picks that failure up, and the next open of the
     * file loads it again. The publish stands even when the drop itself fails, which leaves nobody waiting on a claim
     * that no outcome reaches.
     */
    private static void publishOutcome(
            AsyncCache<MetadataKey, CacheValue> entries,
            FooterKey key,
            CompletableFuture<CacheValue> claim,
            CacheValue result) {
        boolean loadCompleted = result instanceof CacheValue.Loaded;
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
            AsyncCache<MetadataKey, CacheValue> entries, FooterKey key, CompletableFuture<CacheValue> claim) {
        ConcurrentMap<MetadataKey, CompletableFuture<CacheValue>> byKey = entries.asMap();
        byKey.remove(key, claim);
    }

    /**
     * The metadata of a load that completed. A load that threw has the throwable of its loader rethrown here. A footer
     * key never holds a learned entry, because the two kinds of entry have keys of different types.
     */
    private static FooterMetadata metadataOf(CacheValue result) {
        return switch (result) {
            case CacheValue.Loaded(FooterMetadata metadata) -> metadata;
            case CacheValue.Failed(RuntimeException failure) -> throw failure;
            case CacheValue.Fatal(Error failure) -> throw failure;
            case CacheValue.Learned _ -> throw new IllegalStateException("A footer key holds a learned page index");
        };
    }

    /**
     * The page index learned for the chunk of column {@code column} in row group {@code rowGroup} of the file named
     * {@code sourceId} at {@code fileLength} bytes. The column is a leaf ordinal of the file schema. The result is
     * empty when no read has learned the chunk, or when its entry expired or was evicted.
     */
    static Optional<LearnedPages> learnedPages(String sourceId, long fileLength, int rowGroup, int column) {
        LearnedPagesKey key = new LearnedPagesKey(sourceId, fileLength, rowGroup, column);
        CacheValue value = synchronousEntries().getIfPresent(key);
        if (value instanceof CacheValue.Learned(LearnedPages pages)) {
            return Optional.of(pages);
        }
        return Optional.empty();
    }

    /**
     * Keeps {@code pages} as the page index learned for the chunk of column {@code column} in row group
     * {@code rowGroup} of the file named {@code sourceId} at {@code fileLength} bytes. An entry already kept for the
     * chunk stays, and {@code pages} is dropped. Reads learning one chunk at once build equal entries, and the first
     * one kept serves them all.
     */
    static void learnedPagesIfAbsent(String sourceId, long fileLength, int rowGroup, int column, LearnedPages pages) {
        LearnedPagesKey key = new LearnedPagesKey(sourceId, fileLength, rowGroup, column);
        ConcurrentMap<MetadataKey, CacheValue> entries = synchronousEntries().asMap();
        entries.putIfAbsent(key, new CacheValue.Learned(pages));
    }

    /** The number of learned entries held by the cache, over all files. */
    static long learnedEntryCount() {
        AsyncCache<MetadataKey, CacheValue> entries = cache().asyncCache();
        long count = 0;
        for (MetadataKey key : entries.asMap().keySet()) {
            if (key instanceof LearnedPagesKey) {
                count++;
            }
        }
        return count;
    }

    /**
     * The synchronous view of the cache, for storing and reading learned entries. A learned entry is complete when it
     * is stored, and no claim precedes it.
     */
    private static Cache<MetadataKey, CacheValue> synchronousEntries() {
        AsyncCache<MetadataKey, CacheValue> entries = cache().asyncCache();
        return entries.synchronous();
    }

    /**
     * Discards the footers and the learned page indexes present in the cache. A reader opened afterwards reads and
     * parses its footer again. An entry whose load is still in flight is outside what the underlying cache contract
     * defines and may survive the call. Exists for host lifecycle hooks that reset a running process without restarting
     * it.
     */
    public static void clear() {
        cache().invalidateAll();
    }

    private static AsyncMetadataCache cache() {
        return CacheManager.getDefault().getCache(CACHE_NAME, FooterMetadataCache::buildCache);
    }

    private static AsyncMetadataCache buildCache() {
        long maximumWeight = maximumWeightBytes(configuredMaxHeapPercent());
        Weigher<MetadataKey, CacheValue> weigher = (_, value) -> weigh(value);
        AsyncCache<MetadataKey, CacheValue> entries = Caffeine.newBuilder()
                .weigher(weigher)
                .maximumWeight(maximumWeight)
                .initialCapacity(initialCapacity(maximumWeight))
                .expireAfterAccess(EXPIRE_AFTER_ACCESS)
                // An idle process evicts expired entries only when something touches the cache; the scheduler returns
                // that memory on time instead.
                .scheduler(Scheduler.systemScheduler())
                .recordStats()
                .buildAsync();
        return new AsyncMetadataCache(entries);
    }

    /** The bytes of a share of {@code maxHeapPercent} percent of the heap. */
    private static long maximumWeightBytes(int maxHeapPercent) {
        long heapBytes = Runtime.getRuntime().maxMemory();
        return (long) (heapBytes * (maxHeapPercent / 100d));
    }

    /**
     * The size hint for the cache's hash table: the entries admitted by the budget, at four fifths of that count. A
     * table sized well below its eventual population pays repeated resizing and the allocation churn behind it, and one
     * sized above it holds slots for entries that the budget never admits.
     */
    private static int initialCapacity(long maximumWeightBytes) {
        long entriesWithinBudget = maximumWeightBytes / EXPECTED_ENTRY_BYTES;
        long sizeHint = entriesWithinBudget * 4 / 5;
        return (int) Math.min(sizeHint, MAX_INITIAL_CAPACITY);
    }

    /**
     * What one entry costs the heap. A footer entry costs what its metadata retains. A learned entry costs the bytes of
     * its pages. The outcome of a load that threw costs a token charge, because it is about to be dropped.
     */
    private static int weigh(CacheValue value) {
        return switch (value) {
            case CacheValue.Loaded(FooterMetadata metadata) -> weigh(metadata);
            case CacheValue.Failed _ -> FAILED_LOAD_BYTES;
            case CacheValue.Fatal _ -> FAILED_LOAD_BYTES;
            case CacheValue.Learned(LearnedPages pages) -> weigh(pages);
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
     * What one learned entry costs the heap. It is the bytes of its packed pages plus a fixed charge for the objects
     * around them.
     */
    private static int weigh(LearnedPages pages) {
        long weight = pages.byteSize() + LEARNED_ENTRY_OVERHEAD_BYTES;
        return (int) Math.min(weight, Integer.MAX_VALUE);
    }

    /**
     * The heap share set by {@value #MAX_HEAP_PERCENT_PROPERTY}, falling back to the default of
     * {@value #DEFAULT_MAX_HEAP_PERCENT} for an absent value and for one that is not a whole percentage. The cache
     * reads it once, when it is built at its first use. Setting the property later changes nothing until the default
     * {@link CacheManager} is replaced. A new manager builds a new cache, and that cache reads the property again.
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
     * The key of an entry. Each kind of entry has its own type of key, and a key of one kind never finds an entry of
     * the other.
     */
    private sealed interface MetadataKey permits FooterKey, LearnedPagesKey {}

    /**
     * The identity of a file's footer: what the source calls itself and how long the file is. A file rewritten to a
     * different length under the same name misses, and the rewritten footer is read.
     */
    private record FooterKey(@NonNull String sourceId, long fileLength) implements MetadataKey {}

    /**
     * The identity of one column chunk. It adds the ordinal of the row group and the leaf ordinal of the column to the
     * identity of the file's footer.
     */
    private record LearnedPagesKey(@NonNull String sourceId, long fileLength, int rowGroup, int column)
            implements MetadataKey {}

    /**
     * The value of an entry. A footer entry holds the outcome of one footer load, claimed under its key. That outcome
     * is the file's metadata when the loader returned, and the throwable of the loading thread when the loader threw.
     * The throwable is an exception or an error, since a {@link Supplier} declares no checked exception. A learned
     * entry holds the page index learned for one column chunk.
     */
    private sealed interface CacheValue {

        /** The metadata of a load that completed. */
        record Loaded(FooterMetadata metadata) implements CacheValue {}

        /** The exception thrown by a loader, held for every caller of that load. */
        record Failed(RuntimeException failure) implements CacheValue {}

        /** The error thrown by a loader, held for every caller of that load. */
        record Fatal(Error failure) implements CacheValue {}

        /** The page index learned for one column chunk. */
        record Learned(@NonNull LearnedPages pages) implements CacheValue {}
    }

    /**
     * The form of the cache registered with the shared {@link CacheManager}. The manager sees the synchronous view of
     * the asynchronous cache held here. A footer is claimed in the asynchronous cache before it is loaded, and a
     * learned entry is stored through the synchronous view.
     */
    private static final class AsyncMetadataCache extends CaffeineCache<MetadataKey, CacheValue> {

        private final AsyncCache<MetadataKey, CacheValue> asyncCache;

        AsyncMetadataCache(AsyncCache<MetadataKey, CacheValue> asyncCache) {
            super(asyncCache.synchronous());
            this.asyncCache = asyncCache;
        }

        /**
         * The asynchronous cache holding both kinds of entry. Footers are loaded through it, one claim per key. Learned
         * entries go through its synchronous view.
         */
        AsyncCache<MetadataKey, CacheValue> asyncCache() {
            return asyncCache;
        }
    }
}
