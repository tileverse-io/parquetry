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
package io.tileverse.parquetry.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ParquetRecordBatchTest {

    @Test
    void closeReleasesArena() {
        Arena arena = Arena.ofConfined();
        arena.allocate(8);
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 0, arena);

        batch.close();

        // Arena is closed: a subsequent allocate must throw.
        assertThatThrownBy(() -> arena.allocate(8)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void doubleCloseIsNoOp() {
        Arena arena = Arena.ofConfined();
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 0, arena);
        batch.close();
        batch.close();
        // If close() is idempotent the arena must be closed after the first call and remain closed.
        assertThatThrownBy(() -> arena.allocate(8)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void columnsAccessorReturnsLookup() {
        Arena arena = Arena.ofConfined();
        ColumnPath aPath = ColumnPath.of("a");
        BitSet validity = new BitSet(2);
        validity.set(0, 2);
        Map<ColumnPath, ColumnVector> cols = Map.of(aPath, IntVector.materialized(new int[] {1, 2}, validity));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), cols, 2, arena);

        assertThat(batch.columns()).containsKey(aPath);
        assertThat(batch.rowCount()).isEqualTo(2);

        batch.close();
    }

    @Test
    void materializeReturnsBatchBackedRecord() {
        Arena arena = Arena.ofConfined();
        ColumnPath aPath = ColumnPath.of("value");
        BitSet validity = new BitSet(1);
        validity.set(0);
        Map<ColumnPath, ColumnVector> cols = Map.of(aPath, IntVector.materialized(new int[] {42}, validity));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), cols, 1, arena);

        ParquetRecord row = batch.materialize(0);
        assertThat(row).isNotNull();
        assertThat(row.schema()).isEqualTo(batch.projectedSchema());
        assertThat(row.getInt(aPath)).isEqualTo(42);

        batch.close();
    }

    @Test
    void materializeRangeChecksRowIndex() {
        Arena arena = Arena.ofConfined();
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 2, arena);

        assertThatThrownBy(() -> batch.materialize(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> batch.materialize(2)).isInstanceOf(IndexOutOfBoundsException.class);

        batch.close();
    }

    @Test
    void negativeRowCountRejected() {
        Arena arena = Arena.ofConfined();
        ParquetSchema schema = minimalSchema();
        Map<ColumnPath, ColumnVector> cols = Map.of();
        assertThatThrownBy(() -> new DefaultParquetRecordBatch(schema, cols, -1, arena))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowCount must be >= 0");
        arena.close();
    }

    @Test
    void approximateHeapBytesSumsColumns() {
        Arena arena = Arena.ofConfined();
        ColumnPath aPath = ColumnPath.of("value");
        BitSet validity = new BitSet(3);
        validity.set(0, 3);
        Map<ColumnPath, ColumnVector> cols = Map.of(aPath, IntVector.materialized(new int[] {1, 2, 3}, validity));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), cols, 3, arena);

        assertThat(batch.approximateHeapBytes()).isGreaterThanOrEqualTo(12L);

        batch.close();
    }

    @Test
    void closeRunsReleaseActionExactlyOnce() {
        Arena arena = Arena.ofConfined();
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 0, arena);
        AtomicInteger releases = new AtomicInteger();
        batch.attachReleaseAction(releases::incrementAndGet);

        batch.close();
        batch.close();

        assertThat(releases.get())
                .as("release action runs once despite double close")
                .isEqualTo(1);
    }

    @Test
    void releaseActionRunsAfterArenaClose() {
        Arena arena = Arena.ofConfined();
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 0, arena);
        AtomicBoolean arenaAliveAtRelease = new AtomicBoolean();
        batch.attachReleaseAction(() -> arenaAliveAtRelease.set(arena.scope().isAlive()));

        batch.close();

        assertThat(arenaAliveAtRelease.get())
                .as("Arena is closed before the release action runs")
                .isFalse();
    }

    @Test
    void closeWithoutReleaseActionStillClosesArena() {
        Arena arena = Arena.ofConfined();
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 0, arena);
        batch.close();
        assertThatThrownBy(() -> arena.allocate(8)).isInstanceOf(IllegalStateException.class);
    }

    /**
     * Returns a minimal one-column schema sufficient for tests that don't exercise schema content. Uses the same
     * construction pattern as RowGroupPipelineTest.singleColumnSchema().
     */
    private static ParquetSchema minimalSchema() {
        return new ParquetSchema(new SchemaNode.Group(
                "root",
                Repetition.REQUIRED,
                List.of(new SchemaNode.Primitive(
                        "value", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1)),
                Optional.empty(),
                -1));
    }
}
