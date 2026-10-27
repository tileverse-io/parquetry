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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void closeReleasesOwnedBuffers() {
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 0);
        AtomicInteger closes = new AtomicInteger();
        batch.registerBuffer(closes::incrementAndGet);

        batch.close();

        assertThat(closes.get()).isEqualTo(1);
    }

    @Test
    void doubleCloseIsNoOp() {
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 0);
        AtomicInteger closes = new AtomicInteger();
        batch.registerBuffer(closes::incrementAndGet);
        batch.close();
        batch.close();
        assertThat(closes.get()).as("an owned buffer is closed once").isEqualTo(1);
    }

    @Test
    void columnsAccessorReturnsLookup() {
        ColumnPath aPath = ColumnPath.of("a");
        BitSet validity = new BitSet(2);
        validity.set(0, 2);
        Map<ColumnPath, ColumnVector> cols =
                Map.of(aPath, IntVector.materialized(new int[] {1, 2}, Validity.of(validity, 2)));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), cols, 2);

        assertThat(batch.columns()).containsKey(aPath);
        assertThat(batch.rowCount()).isEqualTo(2);

        batch.close();
    }

    @Test
    void materializeReturnsBatchBackedRecord() {
        ColumnPath aPath = ColumnPath.of("value");
        BitSet validity = new BitSet(1);
        validity.set(0);
        Map<ColumnPath, ColumnVector> cols =
                Map.of(aPath, IntVector.materialized(new int[] {42}, Validity.of(validity, 1)));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), cols, 1);

        ParquetRecord row = batch.materialize(0);
        assertThat(row).isNotNull();
        assertThat(row.schema()).isEqualTo(batch.projectedSchema());
        assertThat(row.getInt(aPath)).isEqualTo(42);

        batch.close();
    }

    @Test
    void materializeRangeChecksRowIndex() {
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 2);

        assertThatThrownBy(() -> batch.materialize(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> batch.materialize(2)).isInstanceOf(IndexOutOfBoundsException.class);

        batch.close();
    }

    @Test
    void negativeRowCountRejected() {
        ParquetSchema schema = minimalSchema();
        Map<ColumnPath, ColumnVector> cols = Map.of();
        assertThatThrownBy(() -> new DefaultParquetRecordBatch(schema, cols, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowCount must be >= 0");
    }

    @Test
    void approximateHeapBytesSumsColumns() {
        ColumnPath aPath = ColumnPath.of("value");
        BitSet validity = new BitSet(3);
        validity.set(0, 3);
        Map<ColumnPath, ColumnVector> cols =
                Map.of(aPath, IntVector.materialized(new int[] {1, 2, 3}, Validity.of(validity, 3)));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), cols, 3);

        assertThat(batch.approximateHeapBytes()).isGreaterThanOrEqualTo(12L);

        batch.close();
    }

    @Test
    void closeRunsReleaseActionExactlyOnce() {
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 0);
        AtomicInteger releases = new AtomicInteger();
        batch.attachReleaseAction(releases::incrementAndGet);

        batch.close();
        batch.close();

        assertThat(releases.get())
                .as("release action runs once despite double close")
                .isEqualTo(1);
    }

    @Test
    void releaseActionRunsAfterBuffers() {
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 0);
        AtomicBoolean bufferClosed = new AtomicBoolean();
        AtomicBoolean bufferClosedAtRelease = new AtomicBoolean();
        batch.registerBuffer(() -> bufferClosed.set(true));
        batch.attachReleaseAction(() -> bufferClosedAtRelease.set(bufferClosed.get()));

        batch.close();

        assertThat(bufferClosedAtRelease.get())
                .as("the owned buffers are closed before the release action runs")
                .isTrue();
    }

    @Test
    void noActionStillClosesBuffers() {
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(minimalSchema(), Map.of(), 0);
        AtomicBoolean bufferClosed = new AtomicBoolean();
        batch.registerBuffer(() -> bufferClosed.set(true));
        batch.close();
        assertThat(bufferClosed.get()).isTrue();
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
