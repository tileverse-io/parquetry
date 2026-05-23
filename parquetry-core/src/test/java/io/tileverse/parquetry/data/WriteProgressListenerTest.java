/*
 * Copyright (c) 2026 Tileverse.io
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
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.data.WriteProgressListener.RowGroupFlushEvent;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class WriteProgressListenerTest {

    @Test
    void defaultMethodsAreNoOps() {
        WriteProgressListener listener = new WriteProgressListener() {};

        assertThatCode(() -> {
                    listener.onRowsWritten(123L);
                    listener.onRowGroupFlushed(new RowGroupFlushEvent(0, 1, 2, 3, Duration.ZERO, 4));
                    listener.onClose(10L, 20L, Duration.ofSeconds(1));
                })
                .doesNotThrowAnyException();
    }

    @Test
    void rowGroupFlushEventCarriesAllFields() {
        RowGroupFlushEvent event = new RowGroupFlushEvent(3, 1_000L, 4_096L, 1_024L, Duration.ofMillis(50), 8);

        assertThat(event.rowGroupIndex()).isEqualTo(3);
        assertThat(event.rowCount()).isEqualTo(1_000L);
        assertThat(event.uncompressedBytes()).isEqualTo(4_096L);
        assertThat(event.compressedBytes()).isEqualTo(1_024L);
        assertThat(event.scopeDuration()).isEqualTo(Duration.ofMillis(50));
        assertThat(event.columnsEncoded()).isEqualTo(8);
    }

    @Test
    void overriddenCallbacksReceiveExpectedArgs() {
        long[] rowsWritten = {0L};
        RowGroupFlushEvent[] flushed = new RowGroupFlushEvent[1];
        long[] closeTotals = new long[2];
        Duration[] closeDuration = new Duration[1];

        WriteProgressListener listener = new WriteProgressListener() {
            @Override
            public void onRowsWritten(long totalRows) {
                rowsWritten[0] = totalRows;
            }

            @Override
            public void onRowGroupFlushed(RowGroupFlushEvent event) {
                flushed[0] = event;
            }

            @Override
            public void onClose(long totalRows, long totalBytes, Duration totalDuration) {
                closeTotals[0] = totalRows;
                closeTotals[1] = totalBytes;
                closeDuration[0] = totalDuration;
            }
        };

        listener.onRowsWritten(500L);
        RowGroupFlushEvent event = new RowGroupFlushEvent(2, 250L, 8_192L, 2_048L, Duration.ofMillis(12), 5);
        listener.onRowGroupFlushed(event);
        listener.onClose(1_000L, 65_536L, Duration.ofSeconds(2));

        assertThat(rowsWritten[0]).isEqualTo(500L);
        assertThat(flushed[0]).isSameAs(event);
        assertThat(closeTotals).containsExactly(1_000L, 65_536L);
        assertThat(closeDuration[0]).isEqualTo(Duration.ofSeconds(2));
    }

    @Test
    void onRowsWrittenFiresAtConfiguredCadence(@TempDir Path tempDir) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        List<Long> rowMilestones = new ArrayList<>();
        WriteProgressListener listener = new WriteProgressListener() {
            @Override
            public void onRowsWritten(long totalRows) {
                rowMilestones.add(totalRows);
            }
        };
        WriteOptions options = options(tempDir)
                .progressListener(listener)
                .progressListenerCadenceRows(100L)
                .build();
        try (ParquetWriter writer = ParquetWriter.create(new ByteArrayOutputStream(), schema, options)) {
            for (int i = 0; i < 350; i++) {
                writer.write(row(Map.of(ColumnPath.of("id"), i)));
            }
        }
        assertThat(rowMilestones).containsExactly(100L, 200L, 300L);
    }

    @Test
    void onRowGroupFlushedCarriesIndexAndCounts(@TempDir Path tempDir) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        List<WriteProgressListener.RowGroupFlushEvent> events = new ArrayList<>();
        WriteProgressListener listener = new WriteProgressListener() {
            @Override
            public void onRowGroupFlushed(RowGroupFlushEvent event) {
                events.add(event);
            }
        };
        WriteOptions options = options(tempDir)
                .progressListener(listener)
                .rowGroupSize(RowGroupSize.rows(200L))
                .build();
        try (ParquetWriter writer = ParquetWriter.create(new ByteArrayOutputStream(), schema, options)) {
            for (int i = 0; i < 600; i++) {
                writer.write(row(Map.of(ColumnPath.of("id"), i)));
            }
        }
        assertThat(events).hasSize(3);
        assertThat(events.get(0).rowGroupIndex()).isZero();
        assertThat(events.get(1).rowGroupIndex()).isEqualTo(1);
        assertThat(events.get(2).rowGroupIndex()).isEqualTo(2);
        for (WriteProgressListener.RowGroupFlushEvent event : events) {
            assertThat(event.rowCount()).isEqualTo(200L);
            assertThat(event.columnsEncoded()).isEqualTo(1);
            assertThat(event.compressedBytes()).isPositive();
        }
    }

    @Test
    void onCloseFiresOnceWithFinalCounters(@TempDir Path tempDir) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        AtomicInteger closeCount = new AtomicInteger();
        AtomicInteger lastRowsRef = new AtomicInteger();
        AtomicInteger lastBytesRef = new AtomicInteger();
        WriteProgressListener listener = new WriteProgressListener() {
            @Override
            public void onClose(long totalRows, long totalBytes, Duration totalDuration) {
                closeCount.incrementAndGet();
                lastRowsRef.set((int) totalRows);
                lastBytesRef.set((int) totalBytes);
            }
        };
        WriteOptions options = options(tempDir).progressListener(listener).build();
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        try (ParquetWriter writer = ParquetWriter.create(sink, schema, options)) {
            for (int i = 0; i < 10; i++) {
                writer.write(row(Map.of(ColumnPath.of("id"), i)));
            }
        }
        assertThat(closeCount.get()).isEqualTo(1);
        assertThat(lastRowsRef.get()).isEqualTo(10);
        assertThat(lastBytesRef.get()).isEqualTo(sink.size());
    }

    @Test
    void absentListenerProducesNoCallbacks(@TempDir Path tempDir) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options(tempDir).build();
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        try (ParquetWriter writer = ParquetWriter.create(sink, schema, options)) {
            for (int i = 0; i < 5; i++) {
                writer.write(row(Map.of(ColumnPath.of("id"), i)));
            }
        }
        assertThat(sink.size()).isPositive();
    }

    // --- helpers ---

    private WriteOptions.Builder options(Path tempDir) {
        return WriteOptions.builder().tempDir(tempDir);
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static WriteRow row(Map<ColumnPath, Object> values) {
        Map<ColumnPath, Object> copy = new HashMap<>(values);
        return copy::get;
    }
}
