/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.format.codec;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Hand-rolled Thrift Compact Protocol decoder per <a
 * href="https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md">spec</a>.
 *
 * <p>Operates on a plain {@link InputStream} so it composes with buffered/inflater streams. Thread-confined; create one
 * per parse.
 */
public final class CompactProtocolReader {

    private final InputStream in;

    public CompactProtocolReader(InputStream in) {
        this.in = in;
    }

    public int readI32() throws IOException {
        long raw = readVarLong();
        return (int) ((raw >>> 1) ^ -(raw & 1));
    }

    public long readI64() throws IOException {
        long raw = readVarLong();
        return (raw >>> 1) ^ -(raw & 1);
    }

    public double readDouble() throws IOException {
        byte[] buf = readN(8);
        long bits = ((long) (buf[0] & 0xff))
                | ((long) (buf[1] & 0xff) << 8)
                | ((long) (buf[2] & 0xff) << 16)
                | ((long) (buf[3] & 0xff) << 24)
                | ((long) (buf[4] & 0xff) << 32)
                | ((long) (buf[5] & 0xff) << 40)
                | ((long) (buf[6] & 0xff) << 48)
                | ((long) (buf[7] & 0xff) << 56);
        return Double.longBitsToDouble(bits);
    }

    public ByteBuffer readBinary() throws IOException {
        int len = (int) readVarLong();
        byte[] bytes = readN(len);
        return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    public String readString() throws IOException {
        return StandardCharsets.UTF_8.decode(readBinary()).toString();
    }

    public boolean readBool() throws IOException {
        int b = in.read();
        if (b < 0) {
            throw new EOFException("EOF reading bool");
        }
        return b == 0x01;
    }

    /** Unsigned varint (used for varint64-encoded raw values and lengths). */
    long readVarLong() throws IOException {
        long result = 0L;
        int shift = 0;
        while (true) {
            int b = in.read();
            if (b < 0) {
                throw new EOFException("EOF reading varint");
            }
            result |= ((long) (b & 0x7f)) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 70) {
                throw new IOException("varint too long");
            }
        }
    }

    int readByte() throws IOException {
        return in.read();
    }

    byte[] readN(int n) throws IOException {
        if (n == 0) {
            return new byte[0];
        }
        byte[] buf = new byte[n];
        int read = 0;
        while (read < n) {
            int r = in.read(buf, read, n - read);
            if (r < 0) {
                throw new EOFException("EOF, expected " + n + " bytes, got " + read);
            }
            read += r;
        }
        return buf;
    }

    /**
     * Reads a field header using the Thrift Compact Protocol delta encoding.
     *
     * <p>The high nibble carries the delta from {@code lastFieldId} (1-15), or 0 to signal that the next bytes contain
     * a zigzag-encoded absolute field ID. The low nibble is the {@link CompactType} code. A zero byte is the STOP
     * marker (end of struct).
     */
    public FieldHeader readFieldHeader(int lastFieldId) throws IOException {
        int b = in.read();
        if (b < 0) {
            throw new EOFException("EOF reading field header");
        }
        if (b == 0x00) {
            return new FieldHeader((short) 0, CompactType.STOP);
        }
        int typeCode = b & 0x0f;
        int delta = (b & 0xf0) >>> 4;
        short fieldId;
        if (delta != 0) {
            fieldId = (short) (lastFieldId + delta);
        } else {
            long raw = readVarLong();
            int signed = (int) ((raw >>> 1) ^ -(raw & 1));
            fieldId = (short) signed;
        }
        return new FieldHeader(fieldId, CompactType.of(typeCode));
    }

    /** Size and element type for a Thrift list or set. */
    public record ListHeader(int size, CompactType elementType) {}

    /**
     * Reads a list (or set) header. The high nibble is the size when it fits in 4 bits (0-14); a value of 0xf means the
     * actual size follows as a varint. The low nibble is the element {@link CompactType} code.
     */
    public ListHeader readListHeader() throws IOException {
        int b = in.read();
        if (b < 0) {
            throw new EOFException("EOF reading list header");
        }
        int size = (b & 0xf0) >>> 4;
        CompactType elementType = CompactType.of(b & 0x0f);
        if (size == 0x0f) {
            size = (int) readVarLong();
        }
        return new ListHeader(size, elementType);
    }

    /**
     * Skips all remaining fields in a struct up to (and including) the STOP byte.
     *
     * <p>Used for forward compatibility: when a newer writer adds fields that this reader does not recognise, calling
     * this method consumes those bytes cleanly.
     */
    public void skipStruct() throws IOException {
        int lastFieldId = 0;
        while (true) {
            FieldHeader header = readFieldHeader(lastFieldId);
            if (header.isStop()) return;
            skipField(header.type());
            lastFieldId = header.fieldId();
        }
    }

    /**
     * Skips a single field value of the given {@link CompactType}.
     *
     * <p>Handles all types except MAP, which is not used in parquet.thrift.
     */
    public void skipField(CompactType type) throws IOException {
        switch (type) {
            case BOOLEAN_TRUE, BOOLEAN_FALSE -> {}
            case BYTE -> readN(1);
            case I16, I32, I64 -> readVarLong();
            case DOUBLE -> readN(8);
            case BINARY -> readBinary();
            case LIST, SET -> {
                ListHeader lh = readListHeader();
                for (int i = 0; i < lh.size(); i++) {
                    skipField(lh.elementType());
                }
            }
            case STRUCT -> skipStruct();
            case MAP -> throw new IOException("MAP not used in parquet.thrift");
            case STOP -> throw new IOException("unexpected STOP in skipField");
        }
    }
}
