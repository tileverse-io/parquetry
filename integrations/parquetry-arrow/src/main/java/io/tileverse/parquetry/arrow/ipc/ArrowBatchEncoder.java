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

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
import io.tileverse.parquetry.arrow.ipc.LogicalColumns.LogicalColumn;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.FilteredRecordBatch;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/** Encodes one {@link ParquetRecordBatch} as an Arrow RecordBatch message plus its body buffer segments. */
final class ArrowBatchEncoder {

    private ArrowBatchEncoder() {}

    // Internal carrier only - never compared or used as a map key.
    record Encoded(ByteBuffer metadata, List<MemorySegment> body) {}

    /**
     * Encodes {@code batch}, resolving the logical columns from its schema. A caller that writes a stream of batches
     * should resolve the columns once and use {@link #encode(ParquetRecordBatch, List)} instead; the columns depend
     * only on the (constant) projected schema.
     */
    static Encoded encode(ParquetRecordBatch batch) {
        ParquetSchema schema = batch.projectedSchema();
        // The body is geometry-agnostic; extension metadata lives only in the schema message, never in the buffers.
        return encode(batch, LogicalColumns.of(schema, GeoArrowFields.resolve(schema, Optional.empty())));
    }

    /**
     * Encodes {@code batch} against {@code columns} already resolved from the projected schema. A
     * {@link FilteredRecordBatch} is densified first ({@link FilteredRecordBatch#compacted()}), one dense batch per
     * input batch; a dense batch passes through untouched.
     */
    static Encoded encode(ParquetRecordBatch batch, List<LogicalColumn> columns) {
        ParquetRecordBatch dense = batch instanceof FilteredRecordBatch filtered ? filtered.compacted() : batch;
        try {
            Map<ColumnPath, ColumnVector> vectors = dense.columns();
            BodyWriter writer = new BodyWriter();
            for (LogicalColumn column : columns) {
                ColumnVector vector = requireColumn(vectors, column.path());
                ColumnVector prepared = ArrowExportPrep.prepareForExport(vector, column.field());
                writer.append(ArrowBufferCodec.encode(prepared));
            }
            ByteBuffer metadata = buildMessage(dense.rowCount(), writer.nodes, writer.ranges, writer.offset);
            return new Encoded(metadata, writer.body);
        } finally {
            // The encoded body holds heap copies; the transient dense batch can release here.
            if (dense != batch) {
                dense.close();
            }
        }
    }

    private static ColumnVector requireColumn(Map<ColumnPath, ColumnVector> vectors, ColumnPath path) {
        ColumnVector vector = vectors.get(path);
        if (vector == null) {
            throw new IllegalStateException("Batch has no column vector for top-level column " + path.dot());
        }
        return vector;
    }

    /**
     * Flattens an {@link EncodedNode} tree into the Arrow RecordBatch field-node and buffer order: a depth-first
     * pre-order walk that emits each node's FieldNode and its buffers before descending into the node's children. Every
     * buffer is already 8-byte padded, keeping the running body offset aligned.
     */
    private static final class BodyWriter {

        private final List<FieldNodeData> nodes = new ArrayList<>();
        private final List<BufferRange> ranges = new ArrayList<>();
        private final List<MemorySegment> body = new ArrayList<>();
        private long offset = 0;

        private void append(EncodedNode node) {
            nodes.add(new FieldNodeData(node.length(), node.nullCount()));
            for (EncodedBuffer buffer : node.buffers()) {
                MemorySegment bytes = buffer.bytes();
                long length = bytes.byteSize();
                ranges.add(new BufferRange(offset, length));
                body.add(bytes);
                offset += length;
            }
            for (EncodedNode child : node.children()) {
                append(child);
            }
        }
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
