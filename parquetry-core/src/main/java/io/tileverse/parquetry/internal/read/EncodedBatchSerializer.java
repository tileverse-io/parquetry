/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.internal.read;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.arrow.columnar.EncodedBatch;
import io.tileverse.parquetry.arrow.columnar.EncodedBuffer;
import io.tileverse.parquetry.arrow.columnar.EncodedBuffer.BufferRole;
import io.tileverse.parquetry.arrow.columnar.EncodedNode;
import io.tileverse.parquetry.arrow.columnar.NodeEncoding;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Serializes an {@link EncodedBatch} to a flat byte image and reads it back, for spilling a decoded batch to a file.
 * The index scalars (lengths, counts, encoding tags, column paths) are big-endian; each buffer's bytes are written
 * verbatim in Arrow layout. Reading wraps each buffer span as a read-only heap {@link MemorySegment}, rebuilding a
 * batch the vector factories accept without re-copying buffer bytes through any other form.
 */
final class EncodedBatchSerializer {

    private static final int ENCODING_PLAIN = 0;
    private static final int ENCODING_FIXED_WIDTH = 1;
    private static final int ENCODING_DICTIONARY = 2;

    private EncodedBatchSerializer() {
        // utility
    }

    /** Serializes {@code batch} to a fresh read-only heap segment. */
    static MemorySegment serialize(EncodedBatch batch) {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(sink)) {
            out.writeInt(batch.rowCount());
            out.writeInt(batch.columns().size());
            for (ColumnPath path : batch.columns()) {
                writeString(out, path.dot());
            }
            for (EncodedNode node : batch.nodes()) {
                writeNode(out, node);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize decoded batch", e);
        }
        return MemorySegment.ofArray(sink.toByteArray()).asReadOnly();
    }

    /** Reads back a batch from a segment produced by {@link #serialize(EncodedBatch)}. */
    static EncodedBatch deserialize(MemorySegment segment) {
        byte[] bytes = segment.toArray(ValueLayout.JAVA_BYTE);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            int rowCount = in.readInt();
            int columnCount = in.readInt();
            List<ColumnPath> columns = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columns.add(ColumnPath.parse(readString(in)));
            }
            List<EncodedNode> nodes = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                nodes.add(readNode(in));
            }
            return new EncodedBatch(rowCount, columns, nodes);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize decoded batch", e);
        }
    }

    private static void writeNode(DataOutputStream out, EncodedNode node) throws IOException {
        out.writeInt(node.length());
        out.writeInt(node.nullCount());
        writeEncoding(out, node.encoding());
        out.writeInt(node.buffers().size());
        for (EncodedBuffer buffer : node.buffers()) {
            out.writeByte(buffer.role().ordinal());
            byte[] payload = buffer.bytes().toArray(ValueLayout.JAVA_BYTE);
            out.writeInt(payload.length);
            out.write(payload);
        }
        out.writeInt(node.children().size());
        for (EncodedNode child : node.children()) {
            writeNode(out, child);
        }
    }

    private static EncodedNode readNode(DataInputStream in) throws IOException {
        int length = in.readInt();
        int nullCount = in.readInt();
        NodeEncoding encoding = readEncoding(in);
        int bufferCount = in.readInt();
        List<EncodedBuffer> buffers = new ArrayList<>(bufferCount);
        for (int i = 0; i < bufferCount; i++) {
            BufferRole role = BufferRole.values()[in.readUnsignedByte()];
            int payloadLength = in.readInt();
            byte[] payload = new byte[payloadLength];
            in.readFully(payload);
            buffers.add(new EncodedBuffer(role, MemorySegment.ofArray(payload).asReadOnly()));
        }
        int childCount = in.readInt();
        List<EncodedNode> children = new ArrayList<>(childCount);
        for (int i = 0; i < childCount; i++) {
            children.add(readNode(in));
        }
        return new EncodedNode(length, nullCount, buffers, children, encoding);
    }

    private static void writeEncoding(DataOutputStream out, NodeEncoding encoding) throws IOException {
        switch (encoding) {
            case NodeEncoding.Plain _ -> out.writeByte(ENCODING_PLAIN);
            case NodeEncoding.FixedWidth(int byteWidth) -> {
                out.writeByte(ENCODING_FIXED_WIDTH);
                out.writeInt(byteWidth);
            }
            case NodeEncoding.Dictionary(EncodedNode dictionary, int byteWidth) -> {
                out.writeByte(ENCODING_DICTIONARY);
                out.writeInt(byteWidth);
                writeNode(out, dictionary);
            }
        }
    }

    private static NodeEncoding readEncoding(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        return switch (tag) {
            case ENCODING_PLAIN -> new NodeEncoding.Plain();
            case ENCODING_FIXED_WIDTH -> new NodeEncoding.FixedWidth(in.readInt());
            case ENCODING_DICTIONARY -> {
                int byteWidth = in.readInt();
                EncodedNode dictionary = readNode(in);
                yield new NodeEncoding.Dictionary(dictionary, byteWidth);
            }
            default -> throw new IOException("Unknown node encoding tag " + tag);
        };
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
