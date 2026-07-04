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
package io.tileverse.parquetry.probes;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.FilesetReader;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

/**
 * Measures the wall-clock cost of reading N remote Parquet files, either one file at a time or through one multi-file
 * {@link ParquetSource}. Runs against a latency-injecting HTTP server standing in for object storage; the files are
 * expected at {@code <baseUrl>/f000.parquet} ... {@code f{N-1}.parquet}. Every file is fetched through
 * tileverse-storage exactly as {@link ParquetryReadEngine} opens a remote source, with the same Parquet-tuned cache
 * configuration (caching on, block alignment off).
 *
 * <h2>Pipelines</h2>
 *
 * <ul>
 *   <li>{@code perFile}: N independent single-file reads, each opened, driven, and closed before the next begins, over
 *       a fresh storage handle per file. The client-side baseline: what a caller that does not use the multi-file
 *       source pays. Inherently width-independent; setting {@code probe.maxConcurrentFiles} with this pipeline is
 *       rejected to keep results unambiguous.
 *   <li>{@code fileset}: the per-file sources are assembled into one {@link FilesetReader} and opened as one
 *       {@link ParquetSource} whose runtime overrides only {@code maxConcurrentFiles}. Footer opens overlap up to K and
 *       the drain fans out through the concurrent merge. Timing is decomposed into build (probe-side storage client
 *       construction, serial), open (footer reads), read, and close, exposing where the wall goes. A run at {@code K=1}
 *       is the engine path with the fan-out width collapsed - its parity with {@code perFile} is the no-overhead
 *       regression signal for the merge machinery.
 * </ul>
 *
 * <h2>Modes</h2>
 *
 * <ul>
 *   <li>{@code count}: the footer-only path; no page data is touched. Under {@code fileset} this exercises the
 *       multi-file count fan-out.
 *   <li>{@code drain}: every batch of every column via {@code readBatches}, paying footer, data fetches, and decode.
 * </ul>
 *
 * <h2>Running</h2>
 *
 * <pre>{@code
 * java [JVM flags] -Dprobe.baseUrl=http://localhost:8080 -Dprobe.fileCount=64 \
 *   -Dprobe.pipeline=fileset -Dprobe.mode=drain -Dprobe.maxConcurrentFiles=8 \
 *   -cp target/probes.jar io.tileverse.parquetry.probes.MultiFileReadProbe
 * }</pre>
 *
 * <p>One untimed warmup precedes the timed measurement (a single file read for {@code perFile}, a whole run for
 * {@code fileset}); it warms the JIT and the HTTP connection. The probe prints one parseable line per run.
 */
public final class MultiFileReadProbe {

    private MultiFileReadProbe() {}

    /** How the N files are combined: independent single-file reads, or one multi-file source. */
    private enum Pipeline {
        PER_FILE("perFile"),
        FILESET("fileset");

        private final String label;

        Pipeline(String label) {
            this.label = label;
        }

        static Pipeline from(String value) {
            for (Pipeline pipeline : values()) {
                if (pipeline.label.equalsIgnoreCase(value)) {
                    return pipeline;
                }
            }
            throw new IllegalArgumentException("probe.pipeline must be 'perFile' or 'fileset', got: " + value);
        }
    }

    /** The read shape driven per run: footer-only counting, or a full column drain. */
    private enum Mode {
        COUNT,
        DRAIN;

        static Mode from(String value) {
            return switch (value.toLowerCase()) {
                case "count" -> COUNT;
                case "drain" -> DRAIN;
                default -> throw new IllegalArgumentException("probe.mode must be 'count' or 'drain', got: " + value);
            };
        }

        String label() {
            return name().toLowerCase();
        }
    }

    /** The measured phases of one fileset run; all timings in nanoseconds. */
    private record RunResult(long rows, long buildNanos, long openNanos, long readNanos, long closeNanos) {

        long totalNanos() {
            return buildNanos + openNanos + readNanos + closeNanos;
        }
    }

    public static void main(String[] args) {
        String baseUrl = requiredProperty("probe.baseUrl");
        int fileCount = Integer.parseInt(requiredProperty("probe.fileCount"));
        Pipeline pipeline = Pipeline.from(requiredProperty("probe.pipeline"));
        Mode mode = Mode.from(requiredProperty("probe.mode"));

        switch (pipeline) {
            case PER_FILE -> runPerFile(baseUrl, fileCount, mode);
            case FILESET -> runFileset(baseUrl, fileCount, mode);
        }
    }

    // --- the perFile pipeline ---

    private static void runPerFile(String baseUrl, int fileCount, Mode mode) {
        rejectWidthProperty();
        readOneFile(fileUri(baseUrl, 0), mode);

        long start = System.nanoTime();
        long totalRows = 0L;
        for (int i = 0; i < fileCount; i++) {
            totalRows += readOneFile(fileUri(baseUrl, i), mode);
        }
        long elapsedNanos = System.nanoTime() - start;

        double totalMillis = elapsedNanos / 1_000_000.0;
        IO.println("pipeline=perFile mode=%s N=%d totalMs=%.1f msPerFile=%.2f rows=%d"
                .formatted(mode.label(), fileCount, totalMillis, totalMillis / fileCount, totalRows));
    }

    /** A width setting on the width-independent pipeline reads as if it applied; refuse it outright. */
    private static void rejectWidthProperty() {
        if (System.getProperty("probe.maxConcurrentFiles") != null) {
            throw new IllegalArgumentException(
                    "probe.maxConcurrentFiles does not apply to pipeline=perFile; drop it or use pipeline=fileset");
        }
    }

    /**
     * Opens one remote file, drives the mode's read to completion, closes every handle, and returns the row count
     * observed (a sanity value, not a timing input). A fresh storage handle per file models independent opens with no
     * cross-file reuse.
     */
    private static long readOneFile(URI fileUri, Mode mode) {
        URI container = fileUri.resolve(".");
        try (Storage storage = StorageFactory.open(container, storageProperties());
                RangeReader rangeReader = storage.openRangeReader(fileUri);
                ByteRangeSource source = ByteRangeSources.from(rangeReader)) {
            ParquetFileReader reader =
                    ParquetFileReader.open(source, ParquetRuntime.defaultRuntime(), Optional.empty());
            return switch (mode) {
                case COUNT -> reader.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
                case DRAIN ->
                    drainBatches(reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS));
            };
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // --- the fileset pipeline ---

    private static void runFileset(String baseUrl, int fileCount, Mode mode) {
        int maxConcurrentFiles = Integer.parseInt(requiredProperty("probe.maxConcurrentFiles"));

        runFilesetOnce(baseUrl, fileCount, mode, maxConcurrentFiles);

        RunResult result = runFilesetOnce(baseUrl, fileCount, mode, maxConcurrentFiles);

        double totalMillis = result.totalNanos() / 1_000_000.0;
        IO.println(
                "pipeline=fileset mode=%s K=%d N=%d totalMs=%.1f buildMs=%.1f openMs=%.1f readMs=%.1f closeMs=%.1f msPerFile=%.2f rows=%d"
                        .formatted(
                                mode.label(),
                                maxConcurrentFiles,
                                fileCount,
                                totalMillis,
                                result.buildNanos() / 1_000_000.0,
                                result.openNanos() / 1_000_000.0,
                                result.readNanos() / 1_000_000.0,
                                result.closeNanos() / 1_000_000.0,
                                totalMillis / fileCount,
                                result.rows()));
    }

    /**
     * Builds the N per-file sources, opens them as one dataset bound to a runtime that overrides only
     * {@code maxConcurrentFiles}, drives the mode's read, then closes every handle. Each phase is timed separately; the
     * sum is the end-to-end wall a caller pays, comparable to the perFile pipeline's total.
     */
    private static RunResult runFilesetOnce(String baseUrl, int fileCount, Mode mode, int maxConcurrentFiles) {
        List<Storage> storages = new ArrayList<>(fileCount);
        List<RangeReader> rangeReaders = new ArrayList<>(fileCount);
        List<ByteRangeSource> sources = new ArrayList<>(fileCount);
        try {
            long buildStart = System.nanoTime();
            buildSources(baseUrl, fileCount, storages, rangeReaders, sources);
            long buildNanos = System.nanoTime() - buildStart;

            long openStart = System.nanoTime();
            ParquetRuntime runtime = ParquetRuntime.defaultRuntime().withMaxConcurrentFiles(maxConcurrentFiles);
            OpenOptions options = OpenOptions.builder().runtime(runtime).build();
            ParquetSource source = ParquetSource.open(filesetOf(sources), options);
            long openNanos = System.nanoTime() - openStart;

            long readStart = System.nanoTime();
            long rows =
                    switch (mode) {
                        case COUNT -> source.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
                        case DRAIN ->
                            drainBatches(
                                    source.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS));
                    };
            long readNanos = System.nanoTime() - readStart;

            long closeStart = System.nanoTime();
            closeAll(sources, rangeReaders, storages);
            long closeNanos = System.nanoTime() - closeStart;

            return new RunResult(rows, buildNanos, openNanos, readNanos, closeNanos);
        } catch (RuntimeException e) {
            closeAll(sources, rangeReaders, storages);
            throw e;
        }
    }

    private static void buildSources(
            String baseUrl,
            int fileCount,
            List<Storage> storages,
            List<RangeReader> rangeReaders,
            List<ByteRangeSource> sources) {
        for (int index = 0; index < fileCount; index++) {
            URI fileUri = fileUri(baseUrl, index);
            URI container = fileUri.resolve(".");
            Storage storage = StorageFactory.open(container, storageProperties());
            RangeReader rangeReader = storage.openRangeReader(fileUri);
            storages.add(storage);
            rangeReaders.add(rangeReader);
            sources.add(ByteRangeSources.from(rangeReader));
        }
    }

    private static FilesetReader filesetOf(List<ByteRangeSource> sources) {
        return new FilesetReader() {
            @Override
            public ByteRangeSource openFile(int index) {
                return sources.get(index);
            }

            @Override
            public int fileCount() {
                return sources.size();
            }
        };
    }

    private static void closeAll(
            List<ByteRangeSource> sources, List<RangeReader> rangeReaders, List<Storage> storages) {
        for (ByteRangeSource source : sources) {
            source.close();
        }
        try {
            for (RangeReader rangeReader : rangeReaders) {
                rangeReader.close();
            }
            for (Storage storage : storages) {
                storage.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // --- shared plumbing ---

    /** Streams every batch, summing row counts and releasing each batch's buffers as it goes. */
    private static long drainBatches(Stream<ParquetRecordBatch> batches) {
        long rows = 0L;
        try (batches) {
            for (ParquetRecordBatch batch : (Iterable<ParquetRecordBatch>) batches::iterator) {
                rows += batch.rowCount();
                batch.close();
            }
        }
        return rows;
    }

    /**
     * The Parquet-tuned storage cache configuration, matching {@link ParquetryReadEngine}: caching on, block alignment
     * off, caching one entry per coalesced Parquet range rather than shattering each range into fixed 64 KB blocks.
     */
    private static Properties storageProperties() {
        Properties props = new Properties();
        props.setProperty("storage.caching.enabled", "true");
        props.setProperty("storage.caching.blockaligned", "false");
        return props;
    }

    private static URI fileUri(String baseUrl, int index) {
        String base = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        return URI.create("%s/f%03d.parquet".formatted(base, index));
    }

    private static String requiredProperty(String key) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("required system property not set: -D" + key);
        }
        return value;
    }
}
