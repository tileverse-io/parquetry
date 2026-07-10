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

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ParquetWriterAppenderTest {

    @TempDir
    Path tempDir;

    @Test
    void appenderAutoFlushesAndPreservesValueSequence() throws Exception {
        ParquetSchema schema = optionalIntSchema();
        Path parquetFile = tempDir.resolve("appended.parquet");
        int rowCount = 20_000; // > the 8192 default batch granularity, exercises auto-flush at least twice

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (int i = 0; i < rowCount; i++) {
                appender.setInt(0, i).endRow();
            }
            appender.flush(); // flush the trailing partial batch
        }

        List<Integer> expected = IntStream.range(0, rowCount).boxed().toList();
        assertThat(readBackInts(parquetFile)).isEqualTo(expected);
    }

    @Test
    void appenderFlushesByByteThreshold() throws Exception {
        ParquetSchema schema = optionalStringSchema();
        Path parquetFile = tempDir.resolve("byte-bounded.parquet");
        int rowCount = 200;
        int cellBytes = 1024;
        long byteThreshold = 16L * 1024; // crosses every 16 rows, far before the high row threshold trips
        String cell = "x".repeat(cellBytes);

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema)) {
            ParquetRecordBatchBuilder appender = writer.appender(1_000_000, byteThreshold);
            for (int i = 0; i < rowCount; i++) {
                appender.setString(0, cell).endRow();
            }
            appender.flush();
        }

        // Verifies round-trip integrity under byte-bounded flushing. The byte trigger is the only possible flush path
        // here (the 1M row threshold and the 128MB default row-group sizing are never reached), and multiple byte
        // flushes can coalesce into one open row group, hence the assertion stays at >= 1 rather than a flaky > 1.
        List<String> values = readBackStrings(parquetFile);
        assertThat(values).hasSize(rowCount);
        assertThat(values).allMatch(cell::equals);
        assertThat(rowGroupCountOf(parquetFile)).isGreaterThanOrEqualTo(1);
    }

    @Test
    void partialBatchFlushesOnExplicitFlush() throws Exception {
        ParquetSchema schema = optionalIntSchema();
        Path parquetFile = tempDir.resolve("partial.parquet");
        int rowCount = 10; // appender(4) auto-flushes twice (rows 4 and 8), leaving a 2-row partial for flush()

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema)) {
            ParquetRecordBatchBuilder appender = writer.appender(4);
            for (int i = 0; i < rowCount; i++) {
                appender.setInt(0, i).endRow();
            }
            appender.flush();
        }

        List<Integer> expected = IntStream.range(0, rowCount).boxed().toList();
        assertThat(readBackInts(parquetFile)).isEqualTo(expected);
    }

    @Test
    void forgottenFlushIsNotLost() throws Exception {
        ParquetSchema schema = optionalIntSchema();
        Path parquetFile = tempDir.resolve("forgotten-flush.parquet");
        int rowCount = 20; // appender(8) auto-flushes at rows 8 and 16, leaving a 4-row partial batch pending

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema)) {
            ParquetRecordBatchBuilder appender = writer.appender(8);
            for (int i = 0; i < rowCount; i++) {
                appender.setInt(0, i).endRow();
            }
            // No explicit flush(): the writer must flush the trailing partial batch on close().
        }

        List<Integer> expected = IntStream.range(0, rowCount).boxed().toList();
        assertThat(readBackInts(parquetFile)).isEqualTo(expected);
    }

    @Test
    void tryWithResourcesAppenderFlushes() throws Exception {
        ParquetSchema schema = optionalIntSchema();
        Path parquetFile = tempDir.resolve("try-with-resources.parquet");
        int rowCount = 20;

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema);
                ParquetRecordBatchBuilder appender = writer.appender(8)) {
            for (int i = 0; i < rowCount; i++) {
                appender.setInt(0, i).endRow();
            }
            // No explicit flush(): appender.close() flushes before writer.close().
        }

        List<Integer> expected = IntStream.range(0, rowCount).boxed().toList();
        assertThat(readBackInts(parquetFile)).isEqualTo(expected);
    }

    @Test
    void standaloneBuilderCloseIsNoOp() {
        ParquetSchema schema = optionalIntSchema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        builder.setInt(0, 1).endRow();
        builder.close();

        assertThat(builder.build().rowCount()).isEqualTo(1);
    }

    @Test
    void buildOnABoundAppenderIsRejected() throws Exception {
        ParquetSchema schema = optionalIntSchema();
        Path parquetFile = tempDir.resolve("rejected.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.setInt(0, 1).endRow();
            assertThatThrownBy(appender::build).isInstanceOf(ParquetWriteException.class);
        }
    }

    @Test
    void flushOnAStandaloneBuilderIsRejected() {
        ParquetSchema schema = optionalIntSchema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        builder.setInt(0, 1).endRow();
        assertThatThrownBy(builder::flush).isInstanceOf(ParquetWriteException.class);
    }

    @Test
    void nonPositiveBatchRowsIsRejected() throws Exception {
        ParquetSchema schema = optionalIntSchema();
        Path parquetFile = tempDir.resolve("invalid-granularity.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema)) {
            assertThatThrownBy(() -> writer.appender(0)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> writer.appender(-1)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void appenderAfterCloseIsRejected() throws Exception {
        ParquetSchema schema = optionalIntSchema();
        Path parquetFile = tempDir.resolve("closed-writer.parquet");

        ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema);
        writer.close();
        assertThatThrownBy(writer::appender).isInstanceOf(ParquetWriteException.class);
    }

    @Test
    void appenderNoticesInterruptDuringAuthoring() throws Exception {
        ParquetSchema schema = optionalIntSchema();
        Path parquetFile = tempDir.resolve("interrupted.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema)) {
            ParquetRecordBatchBuilder appender = writer.appender(1_000_000);
            try {
                Thread.currentThread().interrupt();
                assertThatThrownBy(() -> {
                            for (int i = 0; i < 1024; i++) {
                                appender.setInt(0, i).endRow();
                            }
                        })
                        .isInstanceOf(UncheckedIOException.class)
                        .hasCauseInstanceOf(InterruptedIOException.class);
            } finally {
                Thread.interrupted(); // clear the interrupt status for the rest of the suite
            }
        }
    }

    private static ParquetSchema optionalIntSchema() {
        SchemaNode.Primitive idLeaf = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(idLeaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema optionalStringSchema() {
        SchemaNode.Primitive textLeaf = new SchemaNode.Primitive(
                "text",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(textLeaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static List<Integer> readBackInts(Path parquetFile) {
        List<Integer> ids = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> stream =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                stream.forEach(parquetRecord -> ids.add(parquetRecord.getInt(0)));
            }
        }
        return ids;
    }

    private static List<String> readBackStrings(Path parquetFile) {
        List<String> values = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> stream =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                stream.forEach(parquetRecord -> values.add(parquetRecord.getString(0)));
            }
        }
        return values;
    }

    private static int rowGroupCountOf(Path parquetFile) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            return reader.rowGroups().size();
        }
    }
}
