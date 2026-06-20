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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.IntSequence;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Pins that the three Arrow IPC output paths produce byte-identical bytes for the same logical batch: the
 * {@link java.io.OutputStream} overload, a non-gathering {@link java.nio.channels.WritableByteChannel} (the sequential
 * framing branch), and a {@link FileChannel} (a {@link java.nio.channels.GatheringByteChannel}, the gathering framing
 * branch). A fresh batch is built per call because a record batch (and its backing stream) is consumed once.
 */
class ArrowIpcWriterChannelTest {

    @Test
    void gatheringChannelOutputEqualsStreamOutput(@TempDir Path tempDir) throws Exception {
        ParquetSchema schema = schema();

        ByteArrayOutputStream streamSink = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch(schema)), streamSink);
        byte[] bytesA = streamSink.toByteArray();

        // A FileChannel is a GatheringByteChannel, exercising the gathering write(ByteBuffer[]) branch.
        Path file = tempDir.resolve("output.arrow");
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch(schema)), channel);
        }
        byte[] bytesB = Files.readAllBytes(file);

        assertThat(bytesB).isEqualTo(bytesA);
    }

    @Test
    void sequentialChannelOutputEqualsStreamOutput() {
        ParquetSchema schema = schema();

        ByteArrayOutputStream streamSink = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch(schema)), streamSink);
        byte[] bytesA = streamSink.toByteArray();

        // Channels.newChannel(OutputStream) is NOT a GatheringByteChannel, exercising the sequential branch.
        ByteArrayOutputStream channelSink = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch(schema)), Channels.newChannel(channelSink));
        byte[] bytesB = channelSink.toByteArray();

        assertThat(bytesB).isEqualTo(bytesA);
    }

    @Test
    void flushesTheOutputStreamSoBufferedSinksAreComplete() throws Exception {
        ParquetSchema schema = schema();

        ByteArrayOutputStream reference = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch(schema)), reference);
        byte[] expected = reference.toByteArray();

        ByteArrayOutputStream underlying = new ByteArrayOutputStream();
        BufferedOutputStream buffered = new BufferedOutputStream(underlying);
        // No manual flush: if the writer does not flush, the buffer withholds the trailing bytes.
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch(schema)), buffered);
        byte[] actual = underlying.toByteArray();

        assertThat(actual).isEqualTo(expected);
        assertThat(endOfStreamMarker(actual)).isTrue();
    }

    // The Arrow IPC stream ends with an 8-byte end-of-stream marker: continuation 0xFFFFFFFF then int32 0 (LE).
    private static boolean endOfStreamMarker(byte[] stream) {
        if (stream.length < 8) {
            return false;
        }
        ByteBuffer tail = ByteBuffer.wrap(stream, stream.length - 8, 8).order(ByteOrder.LITTLE_ENDIAN);
        return tail.getInt() == 0xFFFFFFFF && tail.getInt() == 0;
    }

    private static ParquetSchema schema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 0);
        SchemaNode.Primitive name = new SchemaNode.Primitive(
                "name",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                1);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(id, name), Optional.empty(), -1));
    }

    private static ParquetRecordBatch batch(ParquetSchema schema) {
        BitSet idValidBits = new BitSet();
        idValidBits.set(0);
        idValidBits.set(2);
        idValidBits.set(3);
        Validity idValidity = Validity.of(idValidBits, 4);

        MemorySegment red = MemorySegment.ofArray("red".getBytes(StandardCharsets.UTF_8));
        MemorySegment green = MemorySegment.ofArray("green".getBytes(StandardCharsets.UTF_8));
        IntSequence indices = IntSequence.of(new int[] {0, 1, 0, 0});
        BitSet nameValidBits = new BitSet();
        nameValidBits.set(0);
        nameValidBits.set(1);
        nameValidBits.set(3);
        Validity nameValidity = Validity.of(nameValidBits, 4);

        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("id"), IntVector.materialized(new int[] {1, 0, 3, 4}, idValidity));
        columns.put(
                ColumnPath.of("name"),
                BinaryVector.dictionary(new MemorySegment[] {red, green}, indices, nameValidity));
        return new DefaultParquetRecordBatch(schema, columns, 4, Arena.ofShared());
    }
}
