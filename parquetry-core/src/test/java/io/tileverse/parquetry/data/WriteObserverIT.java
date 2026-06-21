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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.observe.IndexesWritten;
import io.tileverse.parquetry.observe.RowGroupFlushed;
import io.tileverse.parquetry.observe.WriteObserver;
import io.tileverse.parquetry.observe.WriteStarted;
import io.tileverse.parquetry.observe.WriteStats;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class WriteObserverIT {

    private static final int ROWS_PER_GROUP = 200;
    private static final int TOTAL_ROWS = 600;
    private static final int EXPECTED_ROW_GROUPS = TOTAL_ROWS / ROWS_PER_GROUP;

    @Test
    void observerReceivesEveryWriteEvent(@TempDir Path tempDir) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        RecordingObserver recorder = new RecordingObserver();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .indexedColumns("id")
                .writeObserver(recorder)
                .build();

        try (ParquetWriter writer = ParquetWriter.create(new ByteArrayOutputStream(), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(ROWS_PER_GROUP);
            for (int i = 0; i < TOTAL_ROWS; i++) {
                WriteFixtures.appendRow(appender, schema, Map.of(ColumnPath.of("id"), i));
            }
            appender.flush();
        }

        assertWriteStartedFiredOnce(recorder, schema);
        assertRowGroupsFlushed(recorder, schema);
        assertIndexesWritten(recorder);
        assertWriteFinished(recorder);
    }

    @Test
    void everyProgressMilestoneFiresInOrderWithinASingleFlush(@TempDir Path tempDir) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        RecordingObserver recorder = new RecordingObserver();
        long cadence = 1000L;
        int rowCount = 8000;
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .writeObserverCadenceRows(cadence)
                .writeObserver(recorder)
                .build();

        try (ParquetWriter writer = ParquetWriter.create(new ByteArrayOutputStream(), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(rowCount + 1);
            for (int i = 0; i < rowCount; i++) {
                WriteFixtures.appendRow(appender, schema, Map.of(ColumnPath.of("id"), i));
            }
            appender.flush();
        }

        assertThat(recorder.rowsWritten).containsExactly(1000L, 2000L, 3000L, 4000L, 5000L, 6000L, 7000L, 8000L);
    }

    private void assertWriteStartedFiredOnce(RecordingObserver recorder, ParquetSchema schema) {
        assertThat(recorder.started).hasSize(1);
        assertThat(recorder.started.get(0).schema()).isEqualTo(schema);
    }

    private void assertRowGroupsFlushed(RecordingObserver recorder, ParquetSchema schema) {
        assertThat(recorder.flushed).hasSize(EXPECTED_ROW_GROUPS);
        int columnCount = schema.leafColumns().size();
        for (int i = 0; i < recorder.flushed.size(); i++) {
            RowGroupFlushed event = recorder.flushed.get(i);
            assertThat(event.rowGroupIndex()).isEqualTo(i);
            assertThat(event.rowCount()).isEqualTo(ROWS_PER_GROUP);
            assertThat(event.columnsEncoded()).isEqualTo(columnCount);
            assertThat(event.dictionaryBytes()).isGreaterThanOrEqualTo(0L);
            assertThat(event.compressedBytes()).isPositive();
            assertThat(event.uncompressedBytes()).isPositive();
        }
    }

    private void assertIndexesWritten(RecordingObserver recorder) {
        assertThat(recorder.indexes).hasSize(EXPECTED_ROW_GROUPS);
        for (IndexesWritten event : recorder.indexes) {
            assertThat(event.columnIndexBytes()).isPositive();
            assertThat(event.offsetIndexBytes()).isPositive();
            assertThat(event.bloomFilterBytes()).isPositive();
        }
    }

    private void assertWriteFinished(RecordingObserver recorder) {
        assertThat(recorder.finished).hasSize(1);
        WriteStats stats = recorder.finished.get(0);
        assertThat(stats.totalRows()).isEqualTo(TOTAL_ROWS);
        assertThat(stats.rowGroups()).isEqualTo(EXPECTED_ROW_GROUPS);
        assertThat(stats.compressionRatio()).isPositive();
    }

    private static final class RecordingObserver implements WriteObserver {

        private final List<WriteStarted> started = new ArrayList<>();
        private final List<RowGroupFlushed> flushed = new ArrayList<>();
        private final List<IndexesWritten> indexes = new ArrayList<>();
        private final List<WriteStats> finished = new ArrayList<>();
        private final List<Long> rowsWritten = new ArrayList<>();

        @Override
        public void onWriteStarted(WriteStarted event) {
            started.add(event);
        }

        @Override
        public void onRowsWritten(long totalRows) {
            rowsWritten.add(totalRows);
        }

        @Override
        public void onRowGroupFlushed(RowGroupFlushed event) {
            flushed.add(event);
        }

        @Override
        public void onIndexesWritten(IndexesWritten event) {
            indexes.add(event);
        }

        @Override
        public void onWriteFinished(WriteStats stats) {
            finished.add(stats);
        }
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
}
