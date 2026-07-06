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
package io.tileverse.parquetry.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import io.tileverse.parquetry.compression.Codec;

/** Test-only assembler of OCF byte arrays. */
final class OcfTestWriter {

    static final byte[] SYNC = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    private final Map<String, byte[]> metadata = new LinkedHashMap<>();
    private final ByteArrayOutputStream body = new ByteArrayOutputStream();
    private long firstBlockLength = -1;

    OcfTestWriter schema(String avroSchemaJson) {
        metadata.put("avro.schema", avroSchemaJson.getBytes(StandardCharsets.UTF_8));
        return this;
    }

    OcfTestWriter codec(String name) {
        metadata.put("avro.codec", name.getBytes(StandardCharsets.UTF_8));
        return this;
    }

    OcfTestWriter rawBlock(byte[] framedBlock) {
        if (firstBlockLength < 0) {
            firstBlockLength = framedBlock.length;
        }
        body.writeBytes(framedBlock);
        return this;
    }

    OcfTestWriter block(List<byte[]> records, boolean deflate) {
        return block(records, deflate ? "deflate" : "null");
    }

    /** Frames a list of records into a data block: record count, payload size, payload, trailing sync marker. */
    OcfTestWriter block(List<byte[]> records, String codecName) {
        ByteArrayOutputStream recordBytes = new ByteArrayOutputStream();
        for (byte[] recordBytesEntry : records) {
            recordBytes.writeBytes(recordBytesEntry);
        }
        byte[] payload = encodePayload(recordBytes.toByteArray(), codecName);
        ByteArrayOutputStream framed = new ByteArrayOutputStream();
        varLong(framed, records.size());
        varLong(framed, payload.length);
        framed.writeBytes(payload);
        framed.writeBytes(SYNC);
        return rawBlock(framed.toByteArray());
    }

    private byte[] encodePayload(byte[] payload, String codecName) {
        if (codecName.equals("null")) {
            return payload;
        }
        codec(codecName);
        return switch (codecName) {
            case "deflate" -> compress(Codec.deflate(), payload);
            case "snappy" -> snappyWithCrc(payload);
            case "zstandard" -> compress(Codec.zstd(), payload);
            default -> throw new IllegalArgumentException("Unsupported test codec: " + codecName);
        };
    }

    /** The Avro snappy block framing: the raw snappy block plus a big-endian CRC32 of the uncompressed data. */
    private static byte[] snappyWithCrc(byte[] data) {
        byte[] compressed = compress(Codec.snappy(), data);
        CRC32 crc = new CRC32();
        crc.update(data);
        ByteBuffer out = ByteBuffer.allocate(compressed.length + Integer.BYTES);
        out.put(compressed);
        out.putInt((int) crc.getValue());
        return out.array();
    }

    private static byte[] compress(Codec codec, byte[] data) {
        try {
            byte[] buffer = new byte[(int) codec.maxCompressedLength(data.length)];
            int written = codec.compress(MemorySegment.ofArray(data), MemorySegment.ofArray(buffer));
            return Arrays.copyOf(buffer, written);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    byte[] build() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeHeader(out);
        out.writeBytes(body.toByteArray());
        return out.toByteArray();
    }

    /** The absolute byte offset where the first data block ends, for asserting the reader stops there. */
    long firstBlockEnd() {
        return headerLength() + firstBlockLength;
    }

    private long headerLength() {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        writeHeader(header);
        return header.size();
    }

    private void writeHeader(ByteArrayOutputStream out) {
        out.writeBytes(new byte[] {'O', 'b', 'j', 1});
        varLong(out, metadata.size());
        for (Map.Entry<String, byte[]> entry : metadata.entrySet()) {
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            varLong(out, key.length);
            out.writeBytes(key);
            varLong(out, entry.getValue().length);
            out.writeBytes(entry.getValue());
        }
        varLong(out, 0);
        out.writeBytes(SYNC);
    }

    static void varLong(ByteArrayOutputStream out, long signed) {
        long n = (signed << 1) ^ (signed >> 63);
        while ((n & ~0x7FL) != 0) {
            out.write((int) ((n & 0x7F) | 0x80));
            n >>>= 7;
        }
        out.write((int) n);
    }
}
