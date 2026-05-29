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
package io.tileverse.parquetry.data.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class FailureSemanticsTest {

    @TempDir
    Path tempDir;

    @Test
    void throwingRowCarrierMarksWriterFailedAndCleansTemp() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"), requiredInt32("count"));
        WriteOptions options = options().build();
        long tempCountBefore = countParquetryDirs();

        try (ParquetWriter writer = ParquetWriter.create(new ByteArrayOutputStream(), schema, options)) {
            writer.write(row(Map.of(ColumnPath.of("id"), 1, ColumnPath.of("count"), 1)));
            WriteRow exploding = path -> {
                if (path.equals(ColumnPath.of("count"))) {
                    throw new RuntimeException("count column blew up");
                }
                return 99;
            };
            assertThatThrownBy(() -> writer.write(exploding))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("count column blew up");

            WriteRow followup = row(Map.of(ColumnPath.of("id"), 5, ColumnPath.of("count"), 5));
            assertThatThrownBy(() -> writer.write(followup))
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
        WriteOptions options = options().build();

        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        ParquetWriter writer = ParquetWriter.create(sink, schema, options);
        WriteRow exploding = path -> {
            throw new RuntimeException("row carrier explodes");
        };
        assertThatThrownBy(() -> writer.write(exploding)).isInstanceOf(RuntimeException.class);
        writer.close();
        byte[] bytes = sink.toByteArray();
        // Magic was written; footer was not. The trailing 4 bytes should NOT be PAR1.
        assertThat(bytes).startsWith(new byte[] {'P', 'A', 'R', '1'});
        if (bytes.length >= 4) {
            byte[] tail = new byte[] {
                bytes[bytes.length - 4], bytes[bytes.length - 3], bytes[bytes.length - 2], bytes[bytes.length - 1]
            };
            assertThat(tail).containsExactly('P', 'A', 'R', '1').isEqualTo(new byte[] {'P', 'A', 'R', '1'});
        }
    }

    @Test
    void partialRowGroupsLeavePrefixWithNoFooter() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().rowGroupSize(RowGroupSize.rows(3)).build();
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        ParquetWriter writer = ParquetWriter.create(sink, schema, options);
        long tempCountBefore = countParquetryDirs();

        for (int i = 0; i < 6; i++) {
            writer.write(row(Map.of(ColumnPath.of("id"), i)));
        }
        assertThat(writer.rowGroupsWritten()).isEqualTo(2L);

        // Now feed a throwing row to fail mid-third-RG.
        WriteRow exploding = path -> {
            throw new RuntimeException("third RG explodes");
        };
        assertThatThrownBy(() -> writer.write(exploding)).isInstanceOf(RuntimeException.class);
        writer.close();

        byte[] bytes = sink.toByteArray();
        assertThat(bytes).isNotEmpty();
        // Footer NOT present: the file does not have a proper Parquet footer signature.
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
                writer.write(row(Map.of(ColumnPath.of("id"), 1)));
                Thread.currentThread().interrupt();
                writer.write(row(Map.of(ColumnPath.of("id"), 2)));
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        worker.start();
        worker.join();

        assertThat(failure.get()).isInstanceOf(InterruptedIOException.class);
        assertThat(interrupted.get()).as("interrupt status restored").isTrue();
        assertThat(countParquetryDirs()).isLessThanOrEqualTo(tempCountBefore);
    }

    @Test
    void closeIsIdempotentAfterFailure() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().build();
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        ParquetWriter writer = ParquetWriter.create(sink, schema, options);
        WriteRow exploding = path -> {
            throw new RuntimeException("boom");
        };
        assertThatThrownBy(() -> writer.write(exploding)).isInstanceOf(RuntimeException.class);
        writer.close();
        long firstSize = sink.size();
        writer.close();
        assertThat(sink.size()).isEqualTo(firstSize);
    }

    // --- helpers ---

    private WriteOptions.Builder options() {
        return WriteOptions.builder().tempDir(tempDir);
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

    private static WriteRow row(Map<ColumnPath, Object> values) {
        Map<ColumnPath, Object> copy = new HashMap<>(values);
        return copy::get;
    }
}
