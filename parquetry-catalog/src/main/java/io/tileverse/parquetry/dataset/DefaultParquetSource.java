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
package io.tileverse.parquetry.dataset;

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Semaphore;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.tileverse.parquetry.columnar.BatchMaterializer;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.RowGroupSummary;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Default {@link ParquetSource} implementation: a collection of 1..N {@link ParquetFileReader} instances over files
 * that share the same schema. A multi-reader read with no spatial probe fans its files out, draining up to
 * {@code maxConcurrentFiles} of them at once on virtual threads and merging their elements in arrival order (survivor
 * order when a windowed read needs a deterministic slice); a single-file read, or one bearing a spatial probe, keeps
 * the sequential path that opens one reader's stream at a time and closes it before the next opens. The record
 * overloads transport whole batches across the fan-out and flatten them to rows on the consuming thread, keeping each
 * row's flyweight view on one thread. {@code rowGroups()} aggregates all row groups and re-assigns sequential indices
 * across readers. {@code count} over more than one file fans the per-file counts out across virtual threads, bounded by
 * the shared fetch and decode budgets in {@link ReadOptions}. {@code explain} and {@code explainAnalyze} remain
 * single-reader only and throw {@link UnsupportedOperationException} when more than one reader is present.
 *
 * <p>Schema check happens in the constructor: every reader must agree on {@link ParquetSchema} by equality.
 */
final class DefaultParquetSource implements ParquetSource {

    private final List<ParquetFileReader> readers;
    private final int maxConcurrentFiles;
    private final ParquetSchema schema;
    private final List<RowGroupSummary> rowGroups;

    DefaultParquetSource(List<ParquetFileReader> readers, int maxConcurrentFiles) {
        if (readers == null || readers.isEmpty()) {
            throw new IllegalArgumentException("readers must contain at least one ParquetReader");
        }
        if (maxConcurrentFiles <= 0) {
            throw new IllegalArgumentException("maxConcurrentFiles must be > 0, got " + maxConcurrentFiles);
        }
        ParquetSchema first = readers.get(0).schema();
        for (int i = 1; i < readers.size(); i++) {
            if (!first.equals(readers.get(i).schema())) {
                throw new IllegalArgumentException("all files in a dataset must share one schema; the file at index "
                        + i + " has a schema that differs from the file at index 0");
            }
        }
        this.readers = List.copyOf(readers);
        this.maxConcurrentFiles = maxConcurrentFiles;
        this.schema = first;
        this.rowGroups = buildRowGroups(this.readers);
    }

    private static List<RowGroupSummary> buildRowGroups(List<ParquetFileReader> readers) {
        List<RowGroupSummary> aggregated = new ArrayList<>();
        int index = 0;
        for (ParquetFileReader reader : readers) {
            for (RowGroupSummary group : reader.rowGroups()) {
                aggregated.add(new RowGroupSummary(
                        index++, group.rowCount(), group.totalByteSize(), group.totalCompressedSize()));
            }
        }
        return List.copyOf(aggregated);
    }

    /**
     * Opens one reader per source, overlapping the footer reads of up to {@code maxConcurrentFiles} files on virtual
     * threads. An open is a handful of positional reads; against remote storage those round-trips dominate a sequential
     * loop over many files. The returned readers keep {@code sources}' index order. The sources stay borrowed: a
     * failure propagates without closing them, and the caller owns their lifecycle. Lives on the implementation class,
     * not the {@link ParquetSource} interface, keeping the interface's class file free of preview API references for
     * consumers compiled without preview features.
     */
    static List<ParquetFileReader> openReaders(List<ByteRangeSource> sources, OpenOptions options) {
        if (sources.size() == 1) {
            ParquetFileReader single =
                    ParquetFileReader.open(sources.get(0), options.runtime(), options.decryptionKeyRetriever());
            return List.of(single);
        }
        Semaphore openSlots = new Semaphore(options.runtime().maxConcurrentFiles());
        try (StructuredTaskScope<ParquetFileReader, Void> scope = StructuredTaskScope.open()) {
            List<Subtask<ParquetFileReader>> opens = new ArrayList<>(sources.size());
            for (ByteRangeSource source : sources) {
                opens.add(scope.fork(() -> openOneReader(source, options, openSlots)));
            }
            scope.join();
            return collectInOrder(opens);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException("Interrupted while opening dataset files");
            interrupted.initCause(e);
            throw new UncheckedIOException(interrupted);
        } catch (StructuredTaskScope.FailedException e) {
            throw asUnchecked(e.getCause());
        }
    }

    private static ParquetFileReader openOneReader(ByteRangeSource source, OpenOptions options, Semaphore openSlots)
            throws InterruptedException {
        openSlots.acquire();
        try {
            return ParquetFileReader.open(source, options.runtime(), options.decryptionKeyRetriever());
        } finally {
            openSlots.release();
        }
    }

    private static List<ParquetFileReader> collectInOrder(List<Subtask<ParquetFileReader>> opens) {
        List<ParquetFileReader> readers = new ArrayList<>(opens.size());
        for (Subtask<ParquetFileReader> open : opens) {
            readers.add(open.get());
        }
        return readers;
    }

    @Override
    public ParquetSchema schema() {
        return schema;
    }

    @Override
    public Map<String, String> keyValueMetadata() {
        return readers.get(0).keyValueMetadata();
    }

    @Override
    public List<RowGroupSummary> rowGroups() {
        return rowGroups;
    }

    @Override
    public FileStats fileStats() {
        ensureSingleReader("fileStats");
        return readers.get(0).fileStats();
    }

    @Override
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        return read(predicate, projection, Materializer.defaultRecord(), options);
    }

    @Override
    public <T> Stream<T> read(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        return read(predicate, projection, materializer, options, ConcurrentFileMerge.Emission.UNORDERED);
    }

    /**
     * The materializer read with a caller-chosen fan-out emission order. The windowed entry points pass
     * {@link ConcurrentFileMerge.Emission#SURVIVOR_ORDER}; a following offset/limit then slices a deterministic row
     * sequence. Every other caller keeps {@link ConcurrentFileMerge.Emission#UNORDERED}, the maximum-overlap default.
     */
    <T> Stream<T> read(
            Predicate predicate,
            Projection projection,
            Materializer<T> materializer,
            ReadOptions options,
            ConcurrentFileMerge.Emission emission) {
        if (mustReadSequentially(options)) {
            return concatSequentially(options, reader -> reader.read(predicate, projection, materializer, options));
        }
        Stream<ParquetRecordBatch> batches = mergeConcurrently(
                reader -> reader.readBatches(predicate, projection, options), ParquetRecordBatch::close, emission);
        return batches.flatMap(batch -> BatchRows.flatten(batch, materializer));
    }

    @Override
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        return readBatches(predicate, projection, options, ConcurrentFileMerge.Emission.UNORDERED);
    }

    /**
     * The columnar read with a caller-chosen fan-out emission order, the batch analogue of {@link #read(Predicate,
     * Projection, Materializer, ReadOptions, ConcurrentFileMerge.Emission)}.
     */
    Stream<ParquetRecordBatch> readBatches(
            Predicate predicate, Projection projection, ReadOptions options, ConcurrentFileMerge.Emission emission) {
        return concatLazily(
                options,
                reader -> reader.readBatches(predicate, projection, options),
                ParquetRecordBatch::close,
                emission);
    }

    /**
     * Kept sequential: a {@link BatchMaterializer}'s output ownership is materializer-specific and outside the
     * fan-out's element-owns-its-data transfer contract.
     */
    @Override
    public <T> Stream<T> readBatches(
            Predicate predicate, Projection projection, BatchMaterializer<T> materializer, ReadOptions options) {
        return concatSequentially(options, reader -> reader.readBatches(predicate, projection, materializer, options));
    }

    /**
     * Combines the per-reader streams for one read: a fan-out when it is safe, sequential otherwise. Only elements that
     * own their data may combine concurrently; {@code discardElement} releases an element the consumer never receives
     * when the fan-out is cancelled early. The {@code emission} order shapes a fan-out's interleave; a sequential
     * combine ignores it, being ordered already.
     */
    private <R> Stream<R> concatLazily(
            ReadOptions options,
            Function<ParquetFileReader, Stream<R>> open,
            Consumer<R> discardElement,
            ConcurrentFileMerge.Emission emission) {
        if (mustReadSequentially(options)) {
            return concatSequentially(options, open);
        }
        return mergeConcurrently(open, discardElement, emission);
    }

    /**
     * Whether this read must combine its readers sequentially. A single reader has nothing to overlap. A spatial probe
     * pins the visit order and accumulates its paint in that order on a single thread. Fanning the files out would
     * break both, which is why a probe read stays sequential.
     */
    private boolean mustReadSequentially(ReadOptions options) {
        return readers.size() == 1 || options.spatialReadProbe().isPresent();
    }

    /**
     * Drains up to {@link #maxConcurrentFiles} readers at once, each on its own virtual thread, and merges their
     * elements into one pull-based stream in the given {@code emission} order. Closing the returned stream cancels the
     * producers and releases every undelivered element through {@code discardElement}.
     */
    private <R> Stream<R> mergeConcurrently(
            Function<ParquetFileReader, Stream<R>> open,
            Consumer<R> discardElement,
            ConcurrentFileMerge.Emission emission) {
        return ConcurrentFileMerge.stream(
                readers.size(), index -> open.apply(readers.get(index)), discardElement, maxConcurrentFiles, emission);
    }

    /**
     * Concatenates each reader's stream lazily, holding exactly one reader's stream open at a time and closing it
     * before the next is opened. Closing the returned stream closes the open per-reader stream. This avoids the
     * {@code Stream.flatMap} behaviour that, when pulled lazily through {@code iterator()}, buffers the inner stream's
     * emitted elements (and the decoded batches each one pins) into a {@code SpinedBuffer}.
     *
     * <p>A {@link SpatialReadProbe} on {@code options} turns on the spatial-decimation file tier: the readers are
     * visited in spatial order and each is consulted through the probe before its stream opens. Without a probe the
     * readers keep their relative-path order and no file is skipped, leaving the no-probe read unchanged.
     */
    private <R> Stream<R> concatSequentially(ReadOptions options, Function<ParquetFileReader, Stream<R>> open) {
        SpatialReadPlan plan = spatialReadPlanFor(options);
        ReaderStreamIterator<R> iterator = new ReaderStreamIterator<>(plan.visitOrder(), plan.fileGate(), open);
        Spliterator<R> spliterator =
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, /*parallel*/ false).onClose(iterator::close);
    }

    /**
     * Resolves the per-read spatial plan from {@code options}. Without a probe the plan keeps the readers in their
     * relative-path order and skips nothing, the identity that leaves the no-probe read untouched. With a probe the
     * readers are ordered by their geometry bounds and a paint-aware file gate is built over that order.
     */
    private SpatialReadPlan spatialReadPlanFor(ReadOptions options) {
        Optional<SpatialReadProbe> probe = options.spatialReadProbe();
        if (probe.isEmpty()) {
            return SpatialReadPlan.noProbe(readers);
        }
        Map<ParquetFileReader, Optional<Bbox>> boundsByReader = fileBoundsByReader();
        List<ParquetFileReader> visitOrder = readersInSpatialOrder(boundsByReader);
        FileGate fileGate = paintAwareFileGate(probe.orElseThrow(), boundsByReader);
        return new SpatialReadPlan(visitOrder, fileGate);
    }

    /**
     * The primary geometry file bounds of every reader, read once per probe-bearing read. Both the spatial ordering and
     * the file gate read from this map, which keeps each file's footer parsed once rather than once per sort
     * comparison.
     */
    private Map<ParquetFileReader, Optional<Bbox>> fileBoundsByReader() {
        Map<ParquetFileReader, Optional<Bbox>> bounds = new IdentityHashMap<>(readers.size());
        for (ParquetFileReader reader : readers) {
            bounds.put(reader, fileBounds(reader));
        }
        return bounds;
    }

    /**
     * Orders the readers ascending by their geometry bounds' minimum corner {@code (minX, minY)}. A reader whose file
     * has no geometry bounds sorts last, keeping its relative order among other bound-less readers (the sort is
     * stable). Visiting in this order lets a probe accumulate paint coherently from one neighbouring file to the next.
     */
    private List<ParquetFileReader> readersInSpatialOrder(Map<ParquetFileReader, Optional<Bbox>> boundsByReader) {
        List<ParquetFileReader> ordered = new ArrayList<>(readers);
        ordered.sort(Comparator.comparing(boundsByReader::get, SpatialReadPlan.MIN_CORNER_LAST_IF_ABSENT));
        return ordered;
    }

    /**
     * Builds the file gate for {@code probe}. The gate, consulted lazily before a reader's stream opens, drops a reader
     * whose geometry bounds the probe reports as already covered. A reader whose file has no geometry bounds is never
     * dropped: an unknown extent might cover space the probe has not painted, and skipping it could lose rows.
     *
     * <p>The consultation uses the read-only coarse hook {@link SpatialReadProbe#probeRegion}, which records no
     * coverage. The file's footer was already read when this source was built; this tier skips the file's data read
     * (the column-chunk I/O is what matters) before the per-file read even opens. A fully covered file's row groups
     * would also be dropped by the per-file row-group tier; dropping the whole file here short-circuits it earlier.
     */
    private static FileGate paintAwareFileGate(
            SpatialReadProbe probe, Map<ParquetFileReader, Optional<Bbox>> boundsByReader) {
        return reader -> {
            Optional<Bbox> bounds = boundsByReader.get(reader);
            if (bounds.isEmpty()) {
                return false;
            }
            Bbox box = bounds.orElseThrow();
            Decision decision = probe.probeRegion(box.minX(), box.minY(), box.maxX(), box.maxY());
            return decision instanceof Decision.Skip;
        };
    }

    /**
     * The primary geometry column's file bounds, as a {@link Bbox}, or empty when the file exposes no geometry bounds.
     * The primary column is the first geometry entry of {@link FileStats#geometryBounds()}, matching how the
     * single-file reader picks its primary geometry. Reads only the cached footer; no column data is fetched.
     */
    private static Optional<Bbox> fileBounds(ParquetFileReader reader) {
        return reader.fileStats().geometryBounds().values().stream().findFirst().map(DefaultParquetSource::toBbox);
    }

    private static Bbox toBbox(BoundingBox box) {
        return Bbox.of2d(box.xmin(), box.ymin(), box.xmax(), box.ymax());
    }

    @Override
    public ExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options) {
        ensureSingleReader("explain");
        return readers.get(0).explain(predicate, projection, options);
    }

    @Override
    public ExplainPlan explainAnalyze(Predicate predicate, Projection projection, ReadOptions options) {
        ensureSingleReader("explainAnalyze");
        return readers.get(0).explainAnalyze(predicate, projection, options);
    }

    @Override
    public long count(Predicate predicate, ReadOptions options) {
        if (readers.size() == 1) {
            return readers.get(0).count(predicate, options);
        }
        return countConcurrently(predicate, options);
    }

    /**
     * Counts each file on its own virtual thread, overlapping the independent footer reads and residual decodes. The
     * shared fetch budget and decode pool in {@code options} bound total I/O and CPU. The fan-out therefore overlaps
     * latency without oversubscribing those resources, and a failure in any file cancels the rest.
     */
    private long countConcurrently(Predicate predicate, ReadOptions options) {
        try (StructuredTaskScope<Long, Void> scope = StructuredTaskScope.open()) {
            List<Subtask<Long>> counts = new ArrayList<>(readers.size());
            for (ParquetFileReader reader : readers) {
                counts.add(scope.fork(() -> reader.count(predicate, options)));
            }
            scope.join();
            return sum(counts);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException("Interrupted while counting dataset rows");
            interrupted.initCause(e);
            throw new UncheckedIOException(interrupted);
        } catch (StructuredTaskScope.FailedException e) {
            throw asUnchecked(e.getCause());
        }
    }

    private static long sum(List<Subtask<Long>> counts) {
        long total = 0L;
        for (Subtask<Long> count : counts) {
            total += count.get();
        }
        return total;
    }

    /**
     * Rethrows a per-file open or count failure with the type the sequential path would have thrown. The scope reports
     * the first failed subtask's exception, and neither {@link ParquetFileReader#open} nor
     * {@link ParquetFileReader#count} declares checked exceptions: the cause is a {@link RuntimeException} or an
     * {@link Error}, with anything else falling back to {@link IllegalStateException}.
     */
    private static RuntimeException asUnchecked(Throwable cause) {
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("Counting a dataset file failed", cause);
    }

    private void ensureSingleReader(String operation) {
        if (readers.size() > 1) {
            throw new UnsupportedOperationException(operation + " over multi-file datasets is not implemented yet");
        }
    }

    /**
     * A per-read decision on whether to skip a reader before its stream opens. {@code true} drops the reader: its
     * stream is never opened and its data is never read. The gate is consulted lazily, in visitation order; a probe
     * therefore sees the paint accumulated by every earlier-read file before it judges the next.
     */
    @FunctionalInterface
    private interface FileGate {
        boolean skip(ParquetFileReader reader);
    }

    /**
     * The visitation order and file gate for one read. The no-probe plan keeps the readers in relative-path order and
     * skips none of them, the identity that leaves a no-probe read untouched.
     */
    private record SpatialReadPlan(List<ParquetFileReader> visitOrder, FileGate fileGate) {

        /** Sorts present bounds ascending by {@code (minX, minY)} and sinks an absent bound to the end. */
        private static final Comparator<Optional<Bbox>> MIN_CORNER_LAST_IF_ABSENT = Comparator.comparingDouble(
                        (Optional<Bbox> bounds) -> bounds.map(Bbox::minX).orElse(Double.POSITIVE_INFINITY))
                .thenComparingDouble(bounds -> bounds.map(Bbox::minY).orElse(Double.POSITIVE_INFINITY));

        private static final FileGate OPEN_EVERY_READER = reader -> false;

        static SpatialReadPlan noProbe(List<ParquetFileReader> readers) {
            return new SpatialReadPlan(readers, OPEN_EVERY_READER);
        }
    }

    /**
     * Walks the readers in visitation order, opening each one's stream only when the previous is drained and closing
     * the drained one first. At most one reader's pipeline is resident at a time. A reader the {@link FileGate} skips
     * is never opened. {@link #close()} closes the open per-reader stream.
     */
    private static final class ReaderStreamIterator<R> implements Iterator<R>, AutoCloseable {

        private final Iterator<ParquetFileReader> readers;
        private final FileGate fileGate;
        private final Function<ParquetFileReader, Stream<R>> open;

        @SuppressWarnings("java:S2095") // the stream is closed when drained, by close(), or when its iterator advances
        private Stream<R> currentStream;

        private Iterator<R> currentRows;

        ReaderStreamIterator(
                List<ParquetFileReader> readers, FileGate fileGate, Function<ParquetFileReader, Stream<R>> open) {
            this.readers = readers.iterator();
            this.fileGate = fileGate;
            this.open = open;
        }

        @Override
        public boolean hasNext() {
            while (currentRows == null || !currentRows.hasNext()) {
                closeCurrentStream();
                ParquetFileReader next = nextReaderToOpen();
                if (next == null) {
                    return false;
                }
                currentStream = open.apply(next);
                currentRows = currentStream.iterator();
            }
            return true;
        }

        /**
         * The next reader whose stream should open, passing over every reader the gate skips. The gate is consulted
         * here, after the previous reader is fully drained, which is where a probe has seen that reader's paint.
         */
        private ParquetFileReader nextReaderToOpen() {
            while (readers.hasNext()) {
                ParquetFileReader candidate = readers.next();
                if (!fileGate.skip(candidate)) {
                    return candidate;
                }
            }
            return null;
        }

        @Override
        public R next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentRows.next();
        }

        @Override
        public void close() {
            closeCurrentStream();
        }

        private void closeCurrentStream() {
            Stream<R> stream = currentStream;
            currentStream = null;
            currentRows = null;
            if (stream != null) {
                stream.close();
            }
        }
    }
}
