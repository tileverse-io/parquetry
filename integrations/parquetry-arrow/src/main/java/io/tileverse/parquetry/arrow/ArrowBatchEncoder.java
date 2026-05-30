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

import java.io.ByteArrayOutputStream;
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

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.schema.ColumnPath;

/** Encodes one {@link ParquetRecordBatch} as an Arrow RecordBatch message plus its body bytes. */
final class ArrowBatchEncoder {

    private ArrowBatchEncoder() {}

    // Internal carrier only - never compared or used as a map key.
    @SuppressWarnings("java:S6218")
    record Encoded(ByteBuffer metadata, byte[] body) {}

    static Encoded encode(ParquetRecordBatch batch) {
        List<ColumnPath> leaves = batch.projectedSchema().leafColumns();
        List<FieldNodeData> nodes = new ArrayList<>();
        List<BufferData> buffers = new ArrayList<>();
        ByteArrayOutputStream body = new ByteArrayOutputStream();

        for (ColumnPath path : leaves) {
            ColumnVector vector = batch.columns().get(path);
            nodes.add(new FieldNodeData(vector.size(), nullCount(vector)));
            for (byte[] buffer : ColumnBuffers.forVector(vector)) {
                buffers.add(new BufferData(body.size(), buffer.length));
                body.writeBytes(buffer);
            }
        }
        ByteBuffer metadata = buildMessage(batch.rowCount(), nodes, buffers, body.size());
        return new Encoded(metadata, body.toByteArray());
    }

    private static ByteBuffer buildMessage(
            int rowCount, List<FieldNodeData> nodes, List<BufferData> buffers, int bodyLength) {
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

    private static long nullCount(ColumnVector vector) {
        return (long) vector.size() - vector.validity().cardinality();
    }

    private record FieldNodeData(long length, long nullCount) {}

    private record BufferData(long offset, long length) {}
}
