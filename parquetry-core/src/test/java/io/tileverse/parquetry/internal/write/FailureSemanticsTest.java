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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.io.ByteSink;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The fault is injected through the output sink rather than the row data: an {@link ExplodingChannel} throws once
 * enough bytes have reached it, which forces a failure while a row group is being consolidated to the channel. The
 * writer must then mark itself terminally failed, reject further writes, write no footer, clean its temp directory, and
 * treat {@link ParquetWriter#close()} as idempotent.
 */
class FailureSemanticsTest {

    @TempDir
    Path tempDir;

    // The throwing lambda intentionally drives a sequence of writeRow calls: the sink fails once a byte threshold is
    // passed, which is the failure point under test. The fault cannot be pinned to a single call without changing what
    // the test exercises.
    @SuppressWarnings("java:S5778")
    @Test
    void sinkFailureMarksWriterFailedAndCleansTemp() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"), requiredInt32("count"));
        // rows(1) flushes a row group on every appended row, which drives bytes to the sink each time.
        WriteOptions options = options().rowGroupSize(RowGroupSize.rows(1)).build();
        long tempCountBefore = countParquetryDirs();

        ExplodingChannel sink = ExplodingChannel.failingAfterBytes(8, FaultKind.RUNTIME);
        try (ParquetWriter writer = ParquetWriter.create(sink, schema, options)) {
            assertThatThrownBy(() -> {
                        for (int i = 0; i < 10; i++) {
                            writeRow(writer, schema, Map.of(ColumnPath.of("id"), i, ColumnPath.of("count"), i));
                        }
                    })
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("sink blew up");

            Map<ColumnPath, Object> afterFailure = Map.of(ColumnPath.of("id"), 5, ColumnPath.of("count"), 5);
            assertThatThrownBy(() -> writeRow(writer, schema, afterFailure))
                    .isInstanceOf(ParquetWriteException.class)
                    .hasMessageContaining("failed");
        }
        assertThat(countParquetryDirs())
                .as("temp dirs must be cleaned up after failure")
                .isLessThanOrEqualTo(tempCountBefore);
    }

    // The throwing lambda intentionally drives a sequence of appendRow calls: the appender's auto-flush fails once the
    // sink passes its byte threshold, which is the failure point under test. The fault cannot be pinned to a single
    // call without changing what the test exercises.
    @SuppressWarnings("java:S5778")
    @Test
    void sinkIoFailureIsWrappedAsParquetWriteExceptionThroughTheAppender() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"), requiredInt32("count"));
        WriteOptions options = options().rowGroupSize(RowGroupSize.rows(1)).build();
        long tempCountBefore = countParquetryDirs();

        // The realistic fault: the underlying stream throws IOException partway through a row-group flush. The
        // appender's auto-flush wraps that checked IOException into ParquetWriteException for the caller.
        ExplodingChannel sink = ExplodingChannel.failingAfterBytes(8, FaultKind.IO);
        try (ParquetWriter writer = ParquetWriter.create(sink, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            assertThatThrownBy(() -> {
                        for (int i = 0; i < 10; i++) {
                            WriteFixtures.appendRow(
                                    appender, schema, Map.of(ColumnPath.of("id"), i, ColumnPath.of("count"), i));
                        }
                    })
                    .isInstanceOf(ParquetWriteException.class)
                    .hasRootCauseInstanceOf(IOException.class)
                    .hasMessageContaining("Appender flush failed");

            Map<ColumnPath, Object> afterFailure = Map.of(ColumnPath.of("id"), 5, ColumnPath.of("count"), 5);
            assertThatThrownBy(() -> writeRow(writer, schema, afterFailure))
                    .isInstanceOf(ParquetWriteException.class)
                    .hasMessageContaining("failed");
        }
        assertThat(countParquetryDirs())
                .as("temp dirs must be cleaned up after failure")
                .isLessThanOrEqualTo(tempCountBefore);
    }

    @Test
    void closeOnFailedWriterDoesNotEmitFooter() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().rowGroupSize(RowGroupSize.rows(1)).build();

        // Let the leading magic through, then fail on the first row-group flush.
        ExplodingChannel sink = ExplodingChannel.failingAfterBytes(4, FaultKind.RUNTIME);
        ParquetWriter writer = ParquetWriter.create(sink, schema, options);
        Map<ColumnPath, Object> firstRow = Map.of(ColumnPath.of("id"), 1);
        assertThatThrownBy(() -> writeRow(writer, schema, firstRow)).isInstanceOf(RuntimeException.class);
        writer.close();

        byte[] bytes = sink.writtenBytes();
        // Magic was written; footer was not. The trailing 4 bytes should NOT be PAR1.
        assertThat(bytes).startsWith(new byte[] {'P', 'A', 'R', '1'});
        assertThat(parquetFooterLooksValid(bytes))
                .as("Footer must not have been written on failure")
                .isFalse();
    }

    // The throwing lambda intentionally drives the third row group's rows: the sink fails partway through that flush,
    // which is the failure point under test. The fault cannot be pinned to a single call without changing what the
    // test exercises.
    @SuppressWarnings("java:S5778")
    @Test
    void partialRowGroupsLeavePrefixWithNoFooter() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().rowGroupSize(RowGroupSize.rows(3)).build();
        long tempCountBefore = countParquetryDirs();

        // The threshold lets the first two row groups (six rows) flush in full before the sink blows up partway
        // through the third row group's flush, leaving a prefix with committed row groups but no footer.
        ExplodingChannel sink = ExplodingChannel.failingAfterBytes(300, FaultKind.RUNTIME);
        ParquetWriter writer = ParquetWriter.create(sink, schema, options);

        for (int i = 0; i < 6; i++) {
            writeRow(writer, schema, Map.of(ColumnPath.of("id"), i));
        }
        assertThat(writer.rowGroupsWritten())
                .as("two row groups must commit before the fault")
                .isEqualTo(2L);

        assertThatThrownBy(() -> {
                    for (int i = 6; i < 9; i++) {
                        writeRow(writer, schema, Map.of(ColumnPath.of("id"), i));
                    }
                })
                .isInstanceOf(RuntimeException.class);
        writer.close();

        byte[] bytes = sink.writtenBytes();
        assertThat(bytes).isNotEmpty();
        assertThat(parquetFooterLooksValid(bytes))
                .as("Footer must not have been written on failure")
                .isFalse();
        assertThat(countParquetryDirs()).isLessThanOrEqualTo(tempCountBefore);
    }

    @Test
    void callerInterruptSurfacesAsInterruptedIOException() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().build();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> interrupted = new AtomicReference<>();
        long tempCountBefore = countParquetryDirs();

        Thread worker = new Thread(() -> {
            try (ParquetWriter writer = ParquetWriter.create(new ByteArrayOutputStream(), schema, options)) {
                writeRow(writer, schema, Map.of(ColumnPath.of("id"), 1));
                Thread.currentThread().interrupt();
                writeRow(writer, schema, Map.of(ColumnPath.of("id"), 2));
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        worker.start();
        worker.join();

        assertThat(failure.get())
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(InterruptedIOException.class);
        assertThat(interrupted.get()).as("interrupt status restored").isTrue();
        assertThat(countParquetryDirs()).isLessThanOrEqualTo(tempCountBefore);
    }

    @Test
    void closeIsIdempotentAfterFailure() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().rowGroupSize(RowGroupSize.rows(1)).build();

        ExplodingChannel sink = ExplodingChannel.failingAfterBytes(4, FaultKind.RUNTIME);
        ParquetWriter writer = ParquetWriter.create(sink, schema, options);
        Map<ColumnPath, Object> firstRow = Map.of(ColumnPath.of("id"), 1);
        assertThatThrownBy(() -> writeRow(writer, schema, firstRow)).isInstanceOf(RuntimeException.class);
        writer.close();
        long firstSize = sink.writtenBytes().length;
        writer.close();
        assertThat(sink.writtenBytes().length).isEqualTo(firstSize);
    }

    // --- helpers ---

    private WriteOptions.Builder options() {
        return WriteOptions.builder().tempDir(tempDir);
    }

    private static void writeRow(ParquetWriter writer, ParquetSchema schema, Map<ColumnPath, Object> values)
            throws IOException {
        writer.writeBatch(WriteFixtures.batch(schema, List.of(values)));
    }

    private long countParquetryDirs() throws IOException {
        if (!Files.exists(tempDir)) {
            return 0L;
        }
        try (Stream<Path> walk = Files.list(tempDir)) {
            return walk.filter(p -> p.getFileName().toString().startsWith("parquetry-write-"))
                    .count();
        }
    }

    private static boolean parquetFooterLooksValid(byte[] bytes) {
        if (bytes.length < 12) {
            return false;
        }
        // Tail must be PAR1.
        byte[] tail = new byte[] {
            bytes[bytes.length - 4], bytes[bytes.length - 3], bytes[bytes.length - 2], bytes[bytes.length - 1]
        };
        boolean tailMagic = tail[0] == 'P' && tail[1] == 'A' && tail[2] == 'R' && tail[3] == '1';
        // Footer length is sane (positive and points to bytes after the leading magic).
        int footerLen = (bytes[bytes.length - 8] & 0xff)
                | ((bytes[bytes.length - 7] & 0xff) << 8)
                | ((bytes[bytes.length - 6] & 0xff) << 16)
                | ((bytes[bytes.length - 5] & 0xff) << 24);
        return tailMagic && footerLen > 0 && footerLen < bytes.length - 12;
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

    private enum FaultKind {
        IO,
        RUNTIME
    }

    /**
     * A sink that accepts bytes until it has passed {@code failAfterBytes}, then throws on the next write to model an
     * output stream that fails partway through a row-group consolidation. An IO fault arrives as an
     * {@link UncheckedIOException} wrapping an {@link IOException}, matching how parquetry's RuntimeException-only
     * sinks report a failed write.
     */
    private static final class ExplodingChannel implements ByteSink {

        private final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        private final long failAfterBytes;
        private final FaultKind faultKind;
        private long bytesAccepted;

        private ExplodingChannel(long failAfterBytes, FaultKind faultKind) {
            this.failAfterBytes = failAfterBytes;
            this.faultKind = faultKind;
        }

        static ExplodingChannel failingAfterBytes(long failAfterBytes, FaultKind faultKind) {
            return new ExplodingChannel(failAfterBytes, faultKind);
        }

        byte[] writtenBytes() {
            return captured.toByteArray();
        }

        @Override
        public void write(MemorySegment src) {
            if (bytesAccepted >= failAfterBytes) {
                throw explode();
            }
            byte[] bytes = src.toArray(ValueLayout.JAVA_BYTE);
            captured.writeBytes(bytes);
            bytesAccepted += bytes.length;
        }

        @Override
        public long position() {
            return bytesAccepted;
        }

        private RuntimeException explode() {
            if (faultKind == FaultKind.IO) {
                return new UncheckedIOException(new IOException("sink blew up"));
            }
            return new RuntimeException("sink blew up");
        }

        @Override
        public void close() {
            // The writer never closes the sink; nothing to release here.
        }
    }
}
