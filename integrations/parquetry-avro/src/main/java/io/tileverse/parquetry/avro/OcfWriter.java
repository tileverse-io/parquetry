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
package io.tileverse.parquetry.avro;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import io.tileverse.parquetry.io.ByteSink;

/**
 * Writes an Avro Object Container File to a {@link ByteSink}, inverting {@link OcfReader} and {@link OcfHeader}: the
 * header (magic, metadata, sync marker) is written on construction, then each call to {@link #append} adds an encoded
 * record to the current block and flushes a framed block once the buffered size crosses the threshold. Thread-confined.
 */
final class OcfWriter implements AutoCloseable {

    private static final byte[] MAGIC = {'O', 'b', 'j', 1};

    private final ByteSink sink;
    private final byte[] syncMarker;
    private final BlockCodec codec;
    private final int blockSizeThreshold;
    private final AvroBinaryEncoder blockBuffer = new AvroBinaryEncoder();
    private long recordCount;

    OcfWriter(ByteSink sink, String schemaJson, String codecName, byte[] syncMarker, int blockSizeThreshold) {
        this.sink = sink;
        this.syncMarker = syncMarker.clone();
        this.codec = BlockCodec.fromName(codecName);
        this.blockSizeThreshold = blockSizeThreshold;
        writeHeader(schemaJson, codecName);
    }

    void append(byte[] encodedRecord) {
        blockBuffer.writeFixed(encodedRecord);
        recordCount++;
        if (blockBuffer.size() >= blockSizeThreshold) {
            flushBlock();
        }
    }

    @Override
    public void close() {
        flushBlock();
        sink.close();
    }

    private void writeHeader(String schemaJson, String codecName) {
        Map<String, byte[]> metadata = new LinkedHashMap<>();
        metadata.put("avro.schema", schemaJson.getBytes(StandardCharsets.UTF_8));
        metadata.put("avro.codec", codecName.getBytes(StandardCharsets.UTF_8));

        AvroBinaryEncoder header = new AvroBinaryEncoder();
        header.writeFixed(MAGIC);
        header.writeLong(metadata.size());
        for (Map.Entry<String, byte[]> entry : metadata.entrySet()) {
            header.writeString(entry.getKey());
            header.writeBytes(entry.getValue());
        }
        header.writeLong(0);
        header.writeFixed(syncMarker);
        sink.write(MemorySegment.ofArray(header.toByteArray()));
    }

    private void flushBlock() {
        if (recordCount == 0) {
            return;
        }
        MemorySegment payload = codec.compress(MemorySegment.ofArray(blockBuffer.toByteArray()));
        AvroBinaryEncoder frame = new AvroBinaryEncoder();
        frame.writeLong(recordCount);
        frame.writeLong(payload.byteSize());
        sink.write(MemorySegment.ofArray(frame.toByteArray()));
        sink.write(payload);
        sink.write(MemorySegment.ofArray(syncMarker));
        blockBuffer.reset();
        recordCount = 0;
    }
}
