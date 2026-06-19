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
package io.tileverse.parquetry.arrow;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.flatbuf.RecordBatch;

import com.google.flatbuffers.FlatBufferBuilder;

import io.tileverse.parquetry.arrow.columnar.ArrowBufferCodec;
import io.tileverse.parquetry.arrow.columnar.EncodedBuffer;
import io.tileverse.parquetry.arrow.columnar.EncodedNode;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.schema.ColumnPath;

/** Encodes one {@link ParquetRecordBatch} as an Arrow RecordBatch message plus its body buffer segments. */
final class ArrowBatchEncoder {

    private ArrowBatchEncoder() {}

    // Internal carrier only - never compared or used as a map key.
    record Encoded(ByteBuffer metadata, List<MemorySegment> body) {}

    static Encoded encode(ParquetRecordBatch batch) {
        List<ColumnPath> leaves = batch.projectedSchema().leafColumns();
        List<FieldNodeData> nodes = new ArrayList<>();
        List<BufferRange> ranges = new ArrayList<>();
        List<MemorySegment> body = new ArrayList<>();
        long offset = 0;

        for (ColumnPath path : leaves) {
            ColumnVector vector = batch.columns().get(path).toConsolidated();
            EncodedNode node = ArrowBufferCodec.encode(vector);
            nodes.add(new FieldNodeData(node.length(), node.nullCount()));
            // Only primitive leaves reach this loop: the IPC writer validates the projected schema up front and
            // rejects nested columns, and the projected leaf columns are all primitive. Each node therefore has no
            // child nodes, and its flat buffer list is the whole node.
            for (EncodedBuffer buffer : node.buffers()) {
                MemorySegment bytes = buffer.bytes();
                long length = bytes.byteSize();
                ranges.add(new BufferRange(offset, length));
                body.add(bytes);
                offset += length; // every buffer is already 8-byte padded, keeping offsets aligned
            }
        }
        ByteBuffer metadata = buildMessage(batch.rowCount(), nodes, ranges, offset);
        return new Encoded(metadata, body);
    }

    private static ByteBuffer buildMessage(
            long rowCount, List<FieldNodeData> nodes, List<BufferRange> buffers, long bodyLength) {
        FlatBufferBuilder builder = new FlatBufferBuilder();

        RecordBatch.startNodesVector(builder, nodes.size());
        // FieldNode is a struct, created inline inside the open vector (reverse order per FlatBuffers layout).
        for (int i = nodes.size() - 1; i >= 0; i--) {
            FieldNode.createFieldNode(
                    builder, nodes.get(i).length(), nodes.get(i).nullCount());
        }
        int nodesVector = builder.endVector();

        RecordBatch.startBuffersVector(builder, buffers.size());
        for (int i = buffers.size() - 1; i >= 0; i--) {
            Buffer.createBuffer(builder, buffers.get(i).offset(), buffers.get(i).length());
        }
        int buffersVector = builder.endVector();

        RecordBatch.startRecordBatch(builder);
        RecordBatch.addLength(builder, rowCount);
        RecordBatch.addNodes(builder, nodesVector);
        RecordBatch.addBuffers(builder, buffersVector);
        int recordBatchOffset = RecordBatch.endRecordBatch(builder);

        Message.startMessage(builder);
        Message.addVersion(builder, MetadataVersion.V5);
        Message.addHeaderType(builder, MessageHeader.RecordBatch);
        Message.addHeader(builder, recordBatchOffset);
        Message.addBodyLength(builder, bodyLength);
        int messageOffset = Message.endMessage(builder);
        builder.finish(messageOffset);
        return builder.dataBuffer();
    }

    private record FieldNodeData(long length, long nullCount) {}

    private record BufferRange(long offset, long length) {}
}
