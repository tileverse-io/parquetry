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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.internal.filter.spatial.EmptyBoundsSource;
import io.tileverse.parquetry.internal.read.DecryptionKeyRetriever;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.testkit.TestCorpus;

import io.tileverse.cache.CacheManager;

/**
 * Covers the identity and the bypass rules of the shared footer cache: what is reused by a repeat open of the same
 * file, what is dropped by a changed length or a {@link FooterMetadataCache#clear()}, and which opens never touch the
 * cache at all. Concurrent opens are covered too: one load per file, a load in flight that holds up no other file, and
 * a failed load that leaves nothing behind.
 */
class FooterMetadataCacheTest {

    /** A GeoParquet 1.1 file, which also puts the parsed {@code "geo"} metadata through the cache. */
    private static final String FIXTURE = "parquetry/geo/buildings-gp110-bbox-covering.parquet";

    private static final DecryptionKeyRetriever ANY_KEY = _ -> new byte[16];

    /** The length keyed on by the concurrency checks below, which name files that never exist. */
    private static final long FILE_LENGTH = 4096L;

    /**
     * How long a check waits for a load on another thread before failing. Generous: nothing here is timing-sensitive.
     */
    private static final int BOUNDED_WAIT_SECONDS = 20;

    /** Home of the loggers of the cache library, the ancestor of every logger that its caches publish to. */
    private static final String CACHE_LOGGER = "com.github.benmanes.caffeine.cache";

    /** The loader of a caller expected to join a load already in flight; running it fails the check. */
    private static final Supplier<FooterMetadata> MUST_NOT_LOAD = () -> {
        throw new AssertionError("A caller joining a load in flight must not begin a load of its own");
    };

    @TempDir
    Path tempDir;

    private MemorySegment fixtureBytes;

    @BeforeEach
    void loadFixtureIntoAnEmptyCache() {
        FooterMetadataCache.clear();
        Path file = TestCorpus.extractFile(FIXTURE, tempDir);
        fixtureBytes = MemorySegment.ofArray(readAllBytes(file)).asReadOnly();
    }

    @Test
    void aSecondGetUnderTheSameKeyReusesTheLoadedMetadata() {
        ParseCountingLoader loader = new ParseCountingLoader();

        FooterMetadata loaded = FooterMetadataCache.get("mem://reuse.parquet", fixtureBytes.byteSize(), loader);
        FooterMetadata reused = FooterMetadataCache.get("mem://reuse.parquet", fixtureBytes.byteSize(), loader);

        assertThat(reused).isSameAs(loaded);
        assertThat(loader.invocations()).isOne();
    }

    @Test
    void aChangedLengthUnderOneIdentifierMisses() {
        ParseCountingLoader loader = new ParseCountingLoader();

        FooterMetadata atOneLength = FooterMetadataCache.get("mem://rewritten.parquet", 1024L, loader);
        FooterMetadata atAnother = FooterMetadataCache.get("mem://rewritten.parquet", 2048L, loader);

        assertThat(atAnother).isNotSameAs(atOneLength);
        assertThat(loader.invocations()).isEqualTo(2);
    }

    @Test
    void clearDropsEveryEntry() {
        ParseCountingLoader loader = new ParseCountingLoader();
        FooterMetadata beforeClear = FooterMetadataCache.get("mem://cleared.parquet", 1024L, loader);

        FooterMetadataCache.clear();
        FooterMetadata afterClear = FooterMetadataCache.get("mem://cleared.parquet", 1024L, loader);

        assertThat(afterClear).isNotSameAs(beforeClear);
        assertThat(loader.invocations()).isEqualTo(2);
    }

    /**
     * A cold load takes a storage round trip and a parse. Two files whose keys land in the same bin of the cache's hash
     * table are unrelated, and the read of one goes ahead while the other is still loading.
     */
    @Test
    void aLoadInFlightHoldsUpNoOtherFileOfItsHashBin() throws Exception {
        String slowFile = "mem://Aa.parquet";
        String otherFile = "mem://BB.parquet";
        assertThat(slowFile)
                .as("the two names must hash alike for their keys to share one bin")
                .hasSameHashCodeAs(otherFile);
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        FooterMetadata slowMetadata = parseFixture();
        FooterMetadata otherMetadata = parseFixture();

        try (ExecutorService opens = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<FooterMetadata> slowLoad = opens.submit(() -> FooterMetadataCache.get(slowFile, FILE_LENGTH, () -> {
                loadStarted.countDown();
                awaitOpening(releaseLoad);
                return slowMetadata;
            }));
            try {
                assertThat(loadStarted.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS))
                        .as("the slow load must reach its loader before the other file is opened")
                        .isTrue();
                Future<FooterMetadata> otherLoad =
                        opens.submit(() -> FooterMetadataCache.get(otherFile, FILE_LENGTH, () -> otherMetadata));

                assertThat(otherLoad.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS))
                        .as("the other file of the bin, loaded while the slow load is still in flight")
                        .isSameAs(otherMetadata);
                assertThat(slowLoad.isDone())
                        .as("the slow load is still in flight")
                        .isFalse();
            } finally {
                releaseLoad.countDown();
            }
            assertThat(slowLoad.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS))
                    .as("the metadata of the released load")
                    .isSameAs(slowMetadata);
        }
    }

    /** Every reader of one file waits on a single load, and the footer behind it is read and parsed once. */
    @Test
    void concurrentOpensOfOneFileShareASingleLoad() throws Exception {
        int readers = 8;
        ParseCountingLoader loader = new ParseCountingLoader();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<FooterMetadata>> loads = new ArrayList<>(readers);

        try (ExecutorService opens = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int reader = 0; reader < readers; reader++) {
                loads.add(opens.submit(() -> {
                    awaitOpening(start);
                    return FooterMetadataCache.get("mem://contended.parquet", FILE_LENGTH, loader);
                }));
            }
            start.countDown();

            FooterMetadata shared = loads.getFirst().get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
            for (Future<FooterMetadata> load : loads) {
                assertThat(load.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS))
                        .as("the metadata received by each of the concurrent readers")
                        .isSameAs(shared);
            }
        }

        assertThat(loader.invocations())
                .as("footer parses performed for the file")
                .isOne();
    }

    /**
     * A load that fails leaves the cache as it found it, and says nothing of its own about it: the caller of a file
     * that cannot be read receives one exception, not an exception plus a warning from the cache underneath.
     */
    @Test
    void aFailedLoadReachesItsCallerAndLeavesNothingBehind() {
        MalformedFileException truncated = new MalformedFileException("the footer of mem://broken.parquet is cut off");
        Supplier<FooterMetadata> failing = () -> {
            throw truncated;
        };
        WarningRecorder cacheWarnings = WarningRecorder.attachedTo(CACHE_LOGGER);

        try {
            assertThat(cacheWarnings.recordsWhatItsLoggerPublishes())
                    .as("the recorder must catch what the cache library publishes for the check below to mean anything")
                    .isTrue();

            assertThatThrownBy(() -> FooterMetadataCache.get("mem://broken.parquet", FILE_LENGTH, failing))
                    .as("the failure thrown by the loader, reaching the caller unwrapped")
                    .isSameAs(truncated);

            ParseCountingLoader afterFailure = new ParseCountingLoader();
            FooterMetadata reloaded = FooterMetadataCache.get("mem://broken.parquet", FILE_LENGTH, afterFailure);
            assertThat(reloaded)
                    .as("the metadata of the open following the failure")
                    .isNotNull();
            assertThat(afterFailure.invocations())
                    .as("the failed load left no entry for the next open to find")
                    .isOne();
            assertThat(cacheWarnings.recorded())
                    .as("warnings logged by the cache while a load failed")
                    .isEmpty();
        } finally {
            cacheWarnings.detach();
        }
    }

    /**
     * Every caller of a load that threw receives the throwable of the loading thread itself, the caller that ran the
     * loader and the callers that waited on it alike, and the file is left for the next open to load again. An error is
     * an outcome like any other here: a heap exhausted while one file's footer is parsed must strand no reader of it.
     */
    @ParameterizedTest
    @MethodSource
    void aThrowingLoadReachesEveryCallerAndLeavesNothingBehind(String file, Throwable thrown) throws Exception {
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        CompletableFuture<Thread> secondCaller = new CompletableFuture<>();
        Supplier<FooterMetadata> throwsWhenReleased = () -> {
            loadStarted.countDown();
            awaitOpening(releaseLoad);
            throw throwUnchecked(thrown);
        };

        try (ExecutorService opens = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Throwable> loading = opens.submit(
                    () -> catchThrowable(() -> FooterMetadataCache.get(file, FILE_LENGTH, throwsWhenReleased)));
            try {
                assertThat(loadStarted.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS))
                        .as("the load must be in flight before a second caller joins it")
                        .isTrue();
                Future<Throwable> waiting = opens.submit(() -> {
                    secondCaller.complete(Thread.currentThread());
                    return catchThrowable(() -> FooterMetadataCache.get(file, FILE_LENGTH, MUST_NOT_LOAD));
                });
                awaitJoiningTheLoad(secondCaller.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS));
                releaseLoad.countDown();

                assertThat(waiting.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS))
                        .as("the throwable received by the caller that waited on the load")
                        .isSameAs(thrown);
            } finally {
                releaseLoad.countDown();
            }
            assertThat(loading.get(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS))
                    .as("the throwable received by the caller that ran the load")
                    .isSameAs(thrown);
        }

        ParseCountingLoader afterFailure = new ParseCountingLoader();
        FooterMetadata reloaded = FooterMetadataCache.get(file, FILE_LENGTH, afterFailure);
        assertThat(reloaded)
                .as("the metadata of the open following the failure")
                .isNotNull();
        assertThat(afterFailure.invocations())
                .as("the load that threw left no entry for the next open to find")
                .isOne();
    }

    static Stream<Arguments> aThrowingLoadReachesEveryCallerAndLeavesNothingBehind() {
        return Stream.of(
                Arguments.of("mem://unreadable.parquet", new MalformedFileException("the footer is cut off")),
                Arguments.of("mem://larger-than-the-heap.parquet", new OutOfMemoryError("Java heap space")));
    }

    /** The cache is one of those registered with the shared manager, hence the process-wide reset drains it. */
    @Test
    void theResetOfTheSharedManagerEmptiesTheCache() {
        ParseCountingLoader loader = new ParseCountingLoader();
        FooterMetadata beforeReset = FooterMetadataCache.get("mem://managed.parquet", FILE_LENGTH, loader);

        CacheManager.getDefault().invalidateAll();

        FooterMetadata afterReset = FooterMetadataCache.get("mem://managed.parquet", FILE_LENGTH, loader);
        assertThat(afterReset).isNotSameAs(beforeReset);
        assertThat(loader.invocations()).isEqualTo(2);
    }

    @Test
    void reopeningANamedSourceSkipsTheFooterRead() {
        CountingSource source = CountingSource.named("mem://named.parquet", fixtureBytes);

        ParquetFileReader first = ParquetFileReader.open(source);
        int readsAfterFirstOpen = source.reads();
        ParquetFileReader second = ParquetFileReader.open(source);

        assertThat(source.reads())
                .as("reads performed by the second open of a named source")
                .isEqualTo(readsAfterFirstOpen);
        assertThat(second.compactFooter()).isSameAs(first.compactFooter());
    }

    @Test
    void reopeningANamedSourceSharesTheFormsDerivedFromTheFooter() {
        CountingSource source = CountingSource.named("mem://shared-forms.parquet", fixtureBytes);

        ParquetFileReader first = ParquetFileReader.open(source);
        ParquetFileReader second = ParquetFileReader.open(source);

        assertThat(second.leafIndex()).as("the leaf index of the cached footer").isSameAs(first.leafIndex());
        assertThat(second.spatialBounds())
                .as("the spatial bounds source of the cached footer")
                .isSameAs(first.spatialBounds());
    }

    @Test
    void anAnonymousSourceNeverConsultsTheCache() {
        CountingSource source = CountingSource.anonymous(fixtureBytes);

        ParquetFileReader first = ParquetFileReader.open(source);
        int readsAfterFirstOpen = source.reads();
        ParquetFileReader second = ParquetFileReader.open(source);

        assertThat(source.reads())
                .as("an unnamed source has no cache identity, hence every open reads the footer")
                .isGreaterThan(readsAfterFirstOpen);
        assertThat(second.compactFooter()).isNotSameAs(first.compactFooter());
    }

    @Test
    void anEncryptedOpenIgnoresACachedFooter() {
        CountingSource source = CountingSource.named("mem://encrypted-read.parquet", fixtureBytes);
        ParquetFileReader plain = ParquetFileReader.open(source);
        int readsAfterPlainOpen = source.reads();

        ParquetFileReader encrypted = openWithKey(source);

        assertThat(source.reads())
                .as("reads performed by an open that received a decryption key")
                .isGreaterThan(readsAfterPlainOpen);
        assertThat(encrypted.compactFooter()).isNotSameAs(plain.compactFooter());
    }

    @Test
    void anEncryptedOpenLeavesTheCacheEmpty() {
        CountingSource source = CountingSource.named("mem://encrypted-fill.parquet", fixtureBytes);
        openWithKey(source);
        int readsAfterEncryptedOpen = source.reads();

        ParquetFileReader.open(source);

        assertThat(source.reads())
                .as("a plain open finds nothing left behind by an encrypted open")
                .isGreaterThan(readsAfterEncryptedOpen);
    }

    @Test
    void bulkyKeyValueMetadataWeighsMoreThanTheFooterAlone() {
        FooterMetadata parsed = parseFixture();
        String projjson = "x".repeat(200_000);
        FooterMetadata bare = withKeyValueMetadata(parsed, Map.of());
        FooterMetadata bulky = withKeyValueMetadata(parsed, Map.of("projjson", projjson));

        int charged = FooterMetadataCache.weigh(bulky) - FooterMetadataCache.weigh(bare);

        assertThat(charged)
                .as("two bytes charged per character of key and value")
                .isEqualTo(2 * ("projjson".length() + projjson.length()));
    }

    /**
     * The fixture's bounds come from GeoParquet 1.1 covering columns, not from native geospatial statistics: its boxes
     * live only in the bounds source, and a weigher blind to them would price the entry as if the file had none.
     */
    @Test
    void everyRetainedBoundingBoxIsCharged() {
        FooterMetadata parsed = parseFixture();
        int retainedBoxes = parsed.spatialBounds().retainedBoxCount();
        assertThat(parsed.compactFooter().geoExtentCount())
                .as("a covering file records no native geospatial extent in its footer")
                .isZero();
        assertThat(retainedBoxes)
                .as("one box per row group, plus the file-level union")
                .isEqualTo(parsed.compactFooter().rowGroupCount() + 1);

        int charged = FooterMetadataCache.weigh(parsed) - FooterMetadataCache.weigh(withoutSpatialBounds(parsed));

        assertThat(charged).as("160 bytes charged per retained bounding box").isEqualTo(160 * retainedBoxes);
    }

    @Test
    void everyEntryWeighsAtLeastItsPackedFooterAndTheFixedOverhead() {
        FooterMetadata parsed = parseFixture();
        FooterMetadata bare = withKeyValueMetadata(parsed, Map.of());

        long floor = bare.compactFooter().byteSize() + 4096;

        assertThat((long) FooterMetadataCache.weigh(bare))
                .as("the packed blob and the fixed per-entry overhead are always charged")
                .isGreaterThan(floor);
    }

    @Test
    void anUnsetHeapShareFallsBackToTheDefault() {
        assertThat(System.getProperty(FooterMetadataCache.MAX_HEAP_PERCENT_PROPERTY))
                .as("the heap share property must start unset for this check")
                .isNull();

        assertThat(FooterMetadataCache.configuredMaxHeapPercent()).isEqualTo(10);
    }

    @ParameterizedTest
    @MethodSource
    void theHeapSharePropertyIsHonoredWithinBounds(String configured, int expected) {
        System.setProperty(FooterMetadataCache.MAX_HEAP_PERCENT_PROPERTY, configured);
        try {
            assertThat(FooterMetadataCache.configuredMaxHeapPercent()).isEqualTo(expected);
        } finally {
            System.clearProperty(FooterMetadataCache.MAX_HEAP_PERCENT_PROPERTY);
        }
    }

    static Stream<Arguments> theHeapSharePropertyIsHonoredWithinBounds() {
        return Stream.of(
                Arguments.of("25", 25),
                Arguments.of("  7  ", 7),
                Arguments.of("0", 0),
                Arguments.of("100", 100),
                Arguments.of("101", 10),
                Arguments.of("-1", 10),
                Arguments.of("", 10),
                Arguments.of("a tenth", 10));
    }

    private static FooterMetadata withKeyValueMetadata(FooterMetadata metadata, Map<String, String> keyValueMetadata) {
        return new FooterMetadata(
                metadata.compactFooter(),
                metadata.fileSchema(),
                metadata.leafIndex(),
                metadata.geoMetadata(),
                metadata.spatialBounds(),
                keyValueMetadata,
                metadata.rowGroups());
    }

    /** The same entry with a bounds source that holds no box, which isolates what the boxes themselves cost. */
    private static FooterMetadata withoutSpatialBounds(FooterMetadata metadata) {
        return new FooterMetadata(
                metadata.compactFooter(),
                metadata.fileSchema(),
                metadata.leafIndex(),
                metadata.geoMetadata(),
                EmptyBoundsSource.INSTANCE,
                metadata.keyValueMetadata(),
                metadata.rowGroups());
    }

    private static ParquetFileReader openWithKey(ByteRangeSource source) {
        return ParquetFileReader.open(source, ParquetRuntime.defaultRuntime(), Optional.of(ANY_KEY));
    }

    /** The fixture's footer metadata, derived afresh: two calls give two instances of equal content. */
    private FooterMetadata parseFixture() {
        return FooterMetadata.parse(CountingSource.anonymous(fixtureBytes));
    }

    /**
     * A log handler collecting what a logger and its descendants publish at {@link Level#WARNING} or above, attached
     * for the length of one check. It holds its logger: a logger reachable from nowhere else is collected, and its
     * handlers go with it. While it is attached, the logger publishes to it alone, which keeps what a check logs out of
     * the build output.
     */
    private static final class WarningRecorder extends Handler {

        /** What {@link #recordsWhatItsLoggerPublishes()} publishes to see whether the route reaches this handler. */
        private static final String PROBE = "the recorder attached to this logger is listening";

        private final Logger logger;
        private final boolean publishedToParentHandlers;
        private final List<String> recorded = new CopyOnWriteArrayList<>();

        private WarningRecorder(Logger logger) {
            this.logger = logger;
            this.publishedToParentHandlers = logger.getUseParentHandlers();
        }

        static WarningRecorder attachedTo(String loggerName) {
            WarningRecorder recorder = new WarningRecorder(Logger.getLogger(loggerName));
            recorder.setLevel(Level.ALL);
            recorder.logger.addHandler(recorder);
            recorder.logger.setUseParentHandlers(false);
            return recorder;
        }

        void detach() {
            logger.setUseParentHandlers(publishedToParentHandlers);
            logger.removeHandler(this);
        }

        /**
         * Publishes one warning the way the cache library publishes its own and reports whether it arrived, leaving
         * nothing recorded. A check that trusts an empty record needs this first: a recorder that the library's route
         * bypasses reports no warning whatever the library logged.
         */
        boolean recordsWhatItsLoggerPublishes() {
            System.Logger asTheLibraryPublishes = System.getLogger(logger.getName());
            asTheLibraryPublishes.log(System.Logger.Level.WARNING, PROBE);
            boolean observed = recorded.contains(PROBE);
            recorded.clear();
            return observed;
        }

        @Override
        public void publish(LogRecord entry) {
            if (entry.getLevel().intValue() >= Level.WARNING.intValue()) {
                recorded.add(entry.getMessage());
            }
        }

        @Override
        public void flush() {
            // nothing is buffered
        }

        @Override
        public void close() {
            // nothing is held open
        }

        List<String> recorded() {
            return List.copyOf(recorded);
        }
    }

    /** Waits for {@code latch} to open, failing the check rather than waiting for good. */
    private static void awaitOpening(CountDownLatch latch) {
        try {
            boolean opened = latch.await(BOUNDED_WAIT_SECONDS, TimeUnit.SECONDS);
            if (!opened) {
                throw new AssertionError("Timed out waiting for the latch to open");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for the latch to open", e);
        }
    }

    /**
     * Waits for {@code caller} to park, where a caller joining a load in flight comes to rest until the outcome of that
     * load is published. Bounded: a caller that never parks fails the check rather than holding it up for good.
     */
    private static void awaitJoiningTheLoad(Thread caller) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(BOUNDED_WAIT_SECONDS);
        while (caller.getState() != Thread.State.WAITING) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("Timed out waiting for " + caller + " to join the load in flight");
            }
            Thread.onSpinWait();
        }
    }

    /** Throws {@code thrown}, which every check above keeps to what a footer parse can throw: nothing checked. */
    private static RuntimeException throwUnchecked(Throwable thrown) {
        if (thrown instanceof RuntimeException unchecked) {
            throw unchecked;
        }
        throw (Error) thrown;
    }

    private static byte[] readAllBytes(Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + file, e);
        }
    }

    /** Derives the fixture's footer metadata afresh on every call, counting how often the cache asked for it. */
    private final class ParseCountingLoader implements Supplier<FooterMetadata> {

        private final AtomicInteger invocations = new AtomicInteger();

        @Override
        public FooterMetadata get() {
            invocations.incrementAndGet();
            return parseFixture();
        }

        int invocations() {
            return invocations.get();
        }
    }

    /** An in-memory source counting its reads; a named one offers the cache identity withheld by an anonymous one. */
    private static final class CountingSource implements ByteRangeSource {

        private final MemorySegment bytes;
        private final Optional<String> identifier;
        private final AtomicInteger reads = new AtomicInteger();

        private CountingSource(MemorySegment bytes, Optional<String> identifier) {
            this.bytes = bytes;
            this.identifier = identifier;
        }

        static CountingSource named(String identifier, MemorySegment bytes) {
            return new CountingSource(bytes, Optional.of(identifier));
        }

        static CountingSource anonymous(MemorySegment bytes) {
            return new CountingSource(bytes, Optional.empty());
        }

        @Override
        public long size() {
            return bytes.byteSize();
        }

        @Override
        public Optional<String> sourceIdentifier() {
            return identifier;
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            reads.incrementAndGet();
            long available = bytes.byteSize() - offset;
            if (available <= 0) {
                return -1;
            }
            int length = (int) Math.min(dst.byteSize(), available);
            MemorySegment.copy(bytes, offset, dst, 0L, length);
            return length;
        }

        @Override
        public void close() {
            // the backing segment is heap memory owned by the test
        }

        int reads() {
            return reads.get();
        }
    }
}
