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
package io.tileverse.parquetry.internal.read;

import java.util.Optional;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.observe.FetchStats;
import io.tileverse.parquetry.observe.PhaseTimings;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.RowGroupRead;

import lombok.NonNull;

/**
 * One row group's batches, emitted in file order from a {@link BatchSource}. An inline source decodes on the consumer
 * thread; a streaming source drains a worker-fed hand-off. {@link #close()} releases undelivered batches and the
 * underlying fetch.
 *
 * <p>{@code recordEvalRequired} mirrors the owning survivor's flag: {@code true} when the read still has to test each
 * decoded row against the predicate, {@code false} when statistics already proved every row matches (the MATCHED
 * outcome) and per-row evaluation can be skipped.
 *
 * <p>When an {@code observation} is attached, {@link #close()} emits one {@link RowGroupRead} after the source is
 * drained (joining a streaming producer, hence the decode counters are final) - but only for a row group the
 * coordinator actually delivered to the consumer. A speculatively decoded group the consumer never reached is closed
 * via the coordinator's window cleanup and fires no event. A {@code null} observation leaves the decode path
 * byte-identical: no per-row accounting runs and {@link #close()} only releases the source.
 */
final class DecodedRowGroup implements AutoCloseable {

    private final BatchSource source;
    private final boolean recordEvalRequired;
    private final ReadObservation observation;

    private long matchedRows;
    private long recordFilterNanos;
    private boolean delivered;

    DecodedRowGroup(@NonNull BatchSource source, boolean recordEvalRequired) {
        this(source, recordEvalRequired, null);
    }

    DecodedRowGroup(@NonNull BatchSource source, boolean recordEvalRequired, ReadObservation observation) {
        this.source = source;
        this.recordEvalRequired = recordEvalRequired;
        this.observation = observation;
    }

    boolean recordEvalRequired() {
        return recordEvalRequired;
    }

    boolean hasNext() {
        return source.hasNext();
    }

    ParquetRecordBatch next() {
        return source.next();
    }

    /** Adds {@code n} matched rows to this row group's tally; the read and count paths call it only when observing. */
    void addMatchedRows(long n) {
        matchedRows += n;
    }

    /** Adds {@code n} record-filter nanoseconds to this row group's tally; called only when the observer opted in. */
    void addRecordFilterNanos(long n) {
        recordFilterNanos += n;
    }

    /** Marks this row group as handed to the consumer; the coordinator calls it as it returns the group. */
    void markDelivered() {
        this.delivered = true;
    }

    @Override
    public void close() {
        source.close();
        if (observation != null && delivered) {
            fireRowGroupRead();
        }
    }

    /**
     * Builds and fires the row group's read event. Per-row-group fetch bytes are reported elsewhere, hence
     * {@link FetchStats#EMPTY} here; the page and row tallies come from the now-drained source, counting what decode
     * actually produced rather than the row group's metadata row count.
     */
    private void fireRowGroupRead() {
        BatchRowGroupReader.PageCounts pages = source.pageCounts();
        long rowsDecoded = source.rowsProduced();
        long rowsMatched = observation.matchedEqualsDecoded() ? rowsDecoded : matchedRows;
        // PageCounts.skipped is the column-index page-skip the RowGroupRead calls pagesPruned: the same pages, two
        // names.
        RowGroupRead event = new RowGroupRead(
                observation.rowGroupIndex(),
                rowsDecoded,
                rowsMatched,
                pages.decoded(),
                pages.skipped(),
                FetchStats.EMPTY,
                timings());
        observation.observer().onRowGroupRead(event);
    }

    /**
     * The row group's phase timings when the observer opted in, empty otherwise. Pipeline time is query-level and reads
     * as zero per group; fetch time rode in on the observation, decode time comes from the drained source, and
     * record-filter time accumulated through {@link #addRecordFilterNanos(long)}.
     */
    private Optional<PhaseTimings> timings() {
        if (!observation.wantsTimings()) {
            return Optional.empty();
        }
        return Optional.of(new PhaseTimings(0L, observation.fetchNanos(), source.decodeNanos(), recordFilterNanos));
    }

    /**
     * The per-row-group context the read paths attach when an observer is present. {@code matchedEqualsDecoded} is
     * {@code true} for the pushdown-only batches path (every decoded row is reported matched, no per-row work) and
     * {@code false} for the record-filtering read and count paths (matches accumulate through
     * {@link #addMatchedRows(long)}). {@code wantsTimings} mirrors the observer's opt-in; {@code fetchNanos} is the row
     * group's already-measured fetch time, zero when timings are off.
     */
    record ReadObservation(
            QueryObserver observer,
            int rowGroupIndex,
            boolean matchedEqualsDecoded,
            boolean wantsTimings,
            long fetchNanos) {}
}
