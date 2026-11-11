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
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import io.tileverse.parquetry.internal.filter.LearnedPages;
import io.tileverse.parquetry.internal.footer.CompactFooter;

import io.tileverse.cache.Cache;
import io.tileverse.cache.CacheBudgets;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CachePool;
import io.tileverse.cache.CaffeineCacheManager;
import io.tileverse.cache.PartitionSpec;
import lombok.NonNull;

/**
 * The process-wide cache of the metadata read from Parquet files. It is made of two partitions of the metadata pool of
 * the shared tileverse {@link CacheManager}. The keys of both hold the identity of a file: the name of its source and
 * its length.
 *
 * <p>The partition {@code parquetry-footer-metadata} holds the {@link FooterMetadata} of each file. A server that
 * reopens the same files across requests - a dataset of several hundred Parquet files answering one map request after
 * another - reads and parses each footer once instead of once per open.
 *
 * <p>The partition {@code parquetry-learned-pages} holds the {@link LearnedPages} of column chunks, and its key adds
 * the row group and the column of the chunk. A filtered read of a file without a page index fetches the chunks whole,
 * and it may learn these entries from their page headers. A later read of the same row group prunes pages with them.
 *
 * <p>The weight of a footer entry counts its packed planning footer, its key/value metadata, its schema and the spatial
 * bounds of the file's geometry columns. The weight of a learned entry counts the bytes of its packed pages. Both
 * partitions draw on the budget of the metadata pool, and they share it with the PMTiles directories and the VersaTiles
 * tile indexes of the process. The budget is set with {@code io.tileverse.cache.metadata.max-size}, as a share of the
 * heap or as a size such as {@code 512M}. It is 10% of the heap by default, and a budget of 0 keeps nothing.
 *
 * <p>The statistics of a {@link CaffeineCacheManager}, the default of tileverse, report the hits, the misses and the
 * entries of each partition apart, under the name of the partition. Another manager registers only the pool, and its
 * statistics report the totals of the pool.
 *
 * <p>An entry unused for ten minutes expires. The memory of an expired or evicted entry returns once no open reader
 * over that file still holds the forms derived from it. The per-file statistics kept by a catalog are copies and hold
 * none of that memory.
 *
 * <p>An open over an unnamed source never reaches the cache, because such a source has no stable identity to key on.
 * Neither does an open given a decryption key, because its metadata must not outlive the reader holding the key.
 */
public final class FooterMetadataCache {

    static final String CACHE_NAME = "parquetry-footer-metadata";

    static final String LEARNED_PAGES_NAME = "parquetry-learned-pages";

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

    /**
     * How the footer partition weighs and expires its entries. It is a constant because the pool compares the spec of a
     * partition registered again by equality, and its weigher by identity.
     */
    private static final PartitionSpec<FooterKey, FooterMetadata> FOOTERS =
            new PartitionSpec<>((_, metadata) -> weigh(metadata), EXPIRE_AFTER_ACCESS);

    /** How the learned partition weighs and expires its entries. It is a constant, as the spec of the footers is. */
    private static final PartitionSpec<LearnedPagesKey, LearnedPages> LEARNED_PAGES =
            new PartitionSpec<>((_, pages) -> weigh(pages), EXPIRE_AFTER_ACCESS);

    private FooterMetadataCache() {}

    /**
     * The metadata cached for the file named {@code sourceId} at {@code fileLength} bytes, obtained from {@code loader}
     * on a miss. Concurrent callers for one key share a single load; the rest wait for its result.
     *
     * <p>The lock of the pool covers no more than the claim of the key. The caller holding the claim reads and parses
     * the footer on its own thread outside that lock, and the other files of the same hash bin load meanwhile.
     *
     * <p>The callers of a load that threw receive the throwable of the loading thread itself, stack trace of that
     * thread included, and the next open of the file loads it again.
     *
     * @throws IllegalArgumentException if this call creates the metadata pool and a budget setting holds an invalid
     *     size
     */
    static FooterMetadata get(String sourceId, long fileLength, Supplier<FooterMetadata> loader) {
        FooterKey key = new FooterKey(sourceId, fileLength);
        return footerPartition().get(key, _ -> loader.get());
    }

    /**
     * The page index learned for the chunk of column {@code column} in row group {@code rowGroup} of the file named
     * {@code sourceId} at {@code fileLength} bytes. The column is a leaf ordinal of the file schema. The result is
     * empty when no read has learned the chunk, or when its entry expired or was evicted.
     */
    static Optional<LearnedPages> learnedPages(String sourceId, long fileLength, int rowGroup, int column) {
        LearnedPagesKey key = new LearnedPagesKey(sourceId, fileLength, rowGroup, column);
        LearnedPages pages = learnedPagesPartition().getIfPresent(key);
        return Optional.ofNullable(pages);
    }

    /**
     * Keeps {@code pages} as the page index learned for the chunk of column {@code column} in row group
     * {@code rowGroup} of the file named {@code sourceId} at {@code fileLength} bytes. An entry already kept for the
     * chunk stays, and {@code pages} is dropped. Reads learning one chunk at once build equal entries, and the first
     * one kept serves them all.
     *
     * <p>The partition stores the entry through a load. Its statistics count that store as a miss and a load.
     */
    static void learnedPagesIfAbsent(
            String sourceId, long fileLength, int rowGroup, int column, @NonNull LearnedPages pages) {
        LearnedPagesKey key = new LearnedPagesKey(sourceId, fileLength, rowGroup, column);
        learnedPagesPartition().get(key, _ -> pages);
    }

    /**
     * The number of footer entries held by the cache, over all files. It counts the keys of the partition, because the
     * entry count in the statistics of a partition follows removals through the executor of the pool. That count lags
     * behind a clear for a moment.
     */
    static long footerEntryCount() {
        Collection<FooterKey> keys = footerPartition().keys();
        return keys.size();
    }

    /** The number of learned entries held by the cache, over all files, counted as in {@link #footerEntryCount()}. */
    static long learnedEntryCount() {
        Collection<LearnedPagesKey> keys = learnedPagesPartition().keys();
        return keys.size();
    }

    /**
     * Whether the metadata pool keeps nothing, because the settings of the process give it a budget of 0. The default
     * manager reads the same settings when it creates the pool.
     *
     * @throws IllegalArgumentException if a budget setting holds an invalid size
     */
    static boolean keepsNothing() {
        CacheBudgets budgets = CacheBudgets.fromSettings();
        return budgets.of(CachePool.METADATA) == 0;
    }

    /**
     * Discards the footers and the learned page indexes present in the cache. A reader opened afterwards reads and
     * parses its footer again. An entry whose load is still in flight is outside what the underlying cache contract
     * defines and may survive the call. Exists for host lifecycle hooks that reset a running process without restarting
     * it.
     */
    public static void clear() {
        footerPartition().invalidateAll();
        learnedPagesPartition().invalidateAll();
    }

    /**
     * The footer partition in the metadata pool of the shared manager. Each call looks the partition up again, and a
     * default manager replaced in the meantime takes effect at the next call. The lookup of a known partition takes no
     * lock.
     */
    private static Cache<FooterKey, FooterMetadata> footerPartition() {
        CacheManager manager = CacheManager.getDefault();
        return manager.partition(CachePool.METADATA, CACHE_NAME, FOOTERS);
    }

    /** The learned partition in the metadata pool of the shared manager, looked up as in {@link #footerPartition()}. */
    private static Cache<LearnedPagesKey, LearnedPages> learnedPagesPartition() {
        CacheManager manager = CacheManager.getDefault();
        return manager.partition(CachePool.METADATA, LEARNED_PAGES_NAME, LEARNED_PAGES);
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
     * The identity of a file's footer: what the source calls itself and how long the file is. A file rewritten to a
     * different length under the same name misses, and the rewritten footer is read.
     */
    private record FooterKey(@NonNull String sourceId, long fileLength) {}

    /**
     * The identity of one column chunk. It adds the ordinal of the row group and the leaf ordinal of the column to the
     * identity of the file's footer.
     */
    private record LearnedPagesKey(@NonNull String sourceId, long fileLength, int rowGroup, int column) {}
}
