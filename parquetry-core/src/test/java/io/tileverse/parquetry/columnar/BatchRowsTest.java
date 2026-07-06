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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * Pins the close-ordering contract of the lazy batch-to-row flatten: a delivered row must come from a batch that is
 * still open, and a batch closes only when the stream advances past its last row (or the stream closes). The JDK's
 * {@code Stream.flatMap} violates this under {@code Stream.iterator()} pulls, which drain a whole inner stream into a
 * buffer and close the batch before its buffered row views are read.
 */
class BatchRowsTest {

    /** A stand-in for a decoded batch: rows are materialized as (batch, row) pairs that check openness. */
    private static final class FakeBatch {
        final int id;
        final int rows;
        boolean closed;

        FakeBatch(int id, int rows) {
            this.id = id;
            this.rows = rows;
        }
    }

    private record Row(FakeBatch source, int index) {}

    private static Stream<Row> flatten(Stream<FakeBatch> batches) {
        return BatchRows.flatten(
                batches,
                batch -> batch.rows,
                (batch, row) -> {
                    assertThat(batch.closed)
                            .as("materialized row %s of batch %s from a closed batch", row, batch.id)
                            .isFalse();
                    return new Row(batch, row);
                },
                batch -> batch.closed = true);
    }

    @Test
    void iteratorPullDeliversEveryRowFromAnOpenBatch() {
        List<FakeBatch> batches = List.of(new FakeBatch(0, 3), new FakeBatch(1, 1), new FakeBatch(2, 2));
        List<Row> seen = new ArrayList<>();
        try (Stream<Row> rows = flatten(batches.stream())) {
            Iterator<Row> it = rows.iterator();
            while (it.hasNext()) {
                Row row = it.next();
                assertThat(row.source().closed)
                        .as("row %s of batch %s delivered after close", row.index(), row.source().id)
                        .isFalse();
                seen.add(row);
            }
        }
        assertThat(seen).hasSize(6);
        assertThat(batches).allMatch(b -> b.closed);
    }

    @Test
    void batchClosesOnlyWhenAdvancingPastItsLastRow() {
        FakeBatch first = new FakeBatch(0, 2);
        FakeBatch second = new FakeBatch(1, 1);
        try (Stream<Row> rows = flatten(Stream.of(first, second))) {
            Iterator<Row> it = rows.iterator();
            Row r0 = it.next();
            Row r1 = it.next();
            assertThat(r0.source()).isSameAs(first);
            assertThat(r1.source()).isSameAs(first);
            assertThat(first.closed)
                    .as("last row of the first batch was delivered; batch must still be open")
                    .isFalse();
            Row r2 = it.next();
            assertThat(r2.source()).isSameAs(second);
            assertThat(first.closed)
                    .as("advancing into the second batch closes the first")
                    .isTrue();
            assertThat(second.closed).isFalse();
            assertThat(it.hasNext()).isFalse();
        }
        assertThat(second.closed).isTrue();
    }

    @Test
    void closingTheStreamEarlyClosesTheCurrentBatch() {
        FakeBatch first = new FakeBatch(0, 3);
        FakeBatch second = new FakeBatch(1, 3);
        Stream<Row> rows = flatten(Stream.of(first, second));
        Iterator<Row> it = rows.iterator();
        it.next();
        rows.close();
        assertThat(first.closed).isTrue();
        assertThat(second.closed)
                .as("a batch the flatten never reached is not the flatten's to close")
                .isFalse();
    }

    @Test
    void zeroRowBatchesAreSkippedAndClosed() {
        FakeBatch empty = new FakeBatch(0, 0);
        FakeBatch full = new FakeBatch(1, 1);
        try (Stream<Row> rows = flatten(Stream.of(empty, full))) {
            List<Row> all = rows.toList();
            assertThat(all).hasSize(1);
            assertThat(all.get(0).source()).isSameAs(full);
        }
        assertThat(empty.closed).isTrue();
        assertThat(full.closed).isTrue();
    }

    @Test
    void materializeFailurePropagatesAndTheStreamCloseStillClosesTheBatch() {
        FakeBatch failing = new FakeBatch(0, 1);
        Stream<Row> rows = BatchRows.flatten(
                Stream.of(failing),
                b -> b.rows,
                (b, r) -> {
                    throw new IllegalStateException("boom");
                },
                b -> b.closed = true);
        try (rows) {
            assertThatThrownBy(() -> rows.forEach(r -> {})).isInstanceOf(IllegalStateException.class);
        }
        assertThat(failing.closed).isTrue();
    }

    @Test
    void streamOpsSeeTheSameElementsAndOrderAsFlatMapWould() {
        List<FakeBatch> batches = List.of(new FakeBatch(0, 2), new FakeBatch(1, 0), new FakeBatch(2, 3));
        try (Stream<Row> rows = flatten(batches.stream())) {
            List<String> ids = rows.map(r -> r.source().id + ":" + r.index()).toList();
            assertThat(ids).containsExactly("0:0", "0:1", "2:0", "2:1", "2:2");
        }
    }
}
