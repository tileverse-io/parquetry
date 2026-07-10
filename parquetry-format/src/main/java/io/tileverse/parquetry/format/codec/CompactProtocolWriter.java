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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Hand-rolled Thrift Compact Protocol encoder per <a
 * href="https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md">spec</a>.
 *
 * <p>Strict inverse of {@link CompactProtocolReader}: every byte sequence this writer emits must parse back through the
 * reader to the same logical value.
 *
 * <p>Writes to a plain {@link OutputStream} so it composes with buffered/deflater streams. Thread-confined; create one
 * per encode.
 *
 * <p>Struct scopes are tracked on an internal stack so the writer can emit field headers using the protocol's delta
 * encoding (high nibble = id delta when 1-15, else 0 followed by an explicit zigzag-varint id). Open every struct with
 * {@link #writeStructBegin()}, finish field emission with {@link #writeFieldStop()}, and close the scope with
 * {@link #writeStructEnd()}.
 */
final class CompactProtocolWriter {

    private final OutputStream out;

    /**
     * Per-struct {@code lastFieldId} stack. The top entry tracks the most recently written field id in the current
     * struct scope; {@link #writeStructBegin()} pushes a fresh 0 and {@link #writeStructEnd()} pops it.
     */
    private final short[] stack = new short[64];

    private int depth = -1;

    public CompactProtocolWriter(OutputStream out) {
        this.out = out;
    }

    // -- struct framing --------------------------------------------------------------------------

    /**
     * Opens a new struct scope. Pushes a fresh {@code lastFieldId=0} so the next field's delta is computed against
     * zero, matching the reader's initial state at the start of a struct.
     */
    public void writeStructBegin() {
        depth++;
        if (depth >= stack.length) {
            throw new IllegalStateException("struct nesting exceeds " + stack.length);
        }
        stack[depth] = 0;
    }

    /**
     * Closes the current struct scope. {@link #writeFieldStop()} must already have emitted the STOP byte; this call
     * only pops the field-id tracking state.
     */
    public void writeStructEnd() {
        if (depth < 0) {
            throw new IllegalStateException("writeStructEnd without matching writeStructBegin");
        }
        depth--;
    }

    /** Emits the STOP byte (0x00) terminating the current struct. */
    public void writeFieldStop() throws IOException {
        out.write(0x00);
    }

    /**
     * Writes a field header using the delta encoding from the spec.
     *
     * <p>If {@code id - lastFieldId} is between 1 and 15, the delta packs into the high nibble of a single byte
     * alongside the type code in the low nibble. Otherwise the high nibble is 0 and an explicit zigzag-varint field id
     * follows the type byte.
     */
    public void writeFieldBegin(short id, CompactType type) throws IOException {
        if (depth < 0) {
            throw new IllegalStateException("writeFieldBegin outside of a struct scope");
        }
        int last = stack[depth];
        int delta = id - last;
        if (delta > 0 && delta <= 15) {
            out.write((delta << 4) | type.code);
        } else {
            out.write(type.code);
            writeZigzagVarLong(id);
        }
        stack[depth] = id;
    }

    // -- primitive writers -----------------------------------------------------------------------

    /**
     * Writes a standalone boolean as a single byte ({@code 0x01} true / {@code 0x00} false). For boolean values inside
     * a field, prefer {@link #writeBoolField(short, boolean)}, which folds the value into the field-header type nibble
     * per the Thrift Compact Protocol. This method is the symmetric inverse of {@link CompactProtocolReader#readBool()}
     * and is the right entry point when a boolean appears as a list, set, or map element.
     */
    public void writeBool(boolean value) throws IOException {
        out.write(value ? 0x01 : 0x00);
    }

    /** Writes a raw byte (8 bits, no encoding). */
    public void writeByte(byte value) throws IOException {
        out.write(value & 0xff);
    }

    /**
     * Writes a 16-bit signed integer as zigzag-varint. The compact protocol stores I16, I32 and I64 with the same
     * zigzag-varint wire form; the type code in the field header distinguishes them.
     */
    public void writeI16(short value) throws IOException {
        writeZigzagVarLong(value);
    }

    /** Writes a 32-bit signed integer as zigzag-varint. */
    public void writeI32(int value) throws IOException {
        writeZigzagVarLong(value);
    }

    /** Writes a 64-bit signed integer as zigzag-varint. */
    public void writeI64(long value) throws IOException {
        writeZigzagVarLong(value);
    }

    /** Writes a double as 8 little-endian bytes of the IEEE 754 bit pattern. */
    public void writeDouble(double value) throws IOException {
        long bits = Double.doubleToRawLongBits(value);
        for (int i = 0; i < 8; i++) {
            out.write((int) ((bits >>> (i * 8)) & 0xff));
        }
    }

    /**
     * Writes a binary value: unsigned varint length prefix followed by the raw payload bytes. Strings use the same wire
     * form; only the type code differs.
     */
    public void writeBinary(byte[] value) throws IOException {
        writeVarLong(value.length);
        out.write(value);
    }

    /** Writes a UTF-8 string using the BINARY wire form. */
    public void writeString(String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeBinary(bytes);
    }

    // -- list / set / map framing ----------------------------------------------------------------

    /**
     * Writes a list header. Sizes 0-14 pack into the high nibble alongside the element type in the low nibble; size 15
     * or more uses the long form (high nibble 0xf, then a varint size).
     */
    public void writeListBegin(CompactType elementType, int size) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("negative list size: " + size);
        }
        if (size < 15) {
            out.write((size << 4) | elementType.code);
        } else {
            out.write(0xf0 | elementType.code);
            writeVarLong(size);
        }
    }

    /** No-op terminator for symmetry with {@link #writeListBegin(CompactType, int)}. */
    public void writeListEnd() {
        // Compact protocol lists have no trailer; the size prefix bounds the payload.
    }

    /** Writes a set header. Sets share the list wire form in the compact protocol. */
    public void writeSetBegin(CompactType elementType, int size) throws IOException {
        writeListBegin(elementType, size);
    }

    /** No-op terminator for symmetry with {@link #writeSetBegin(CompactType, int)}. */
    public void writeSetEnd() {
        // Compact protocol sets have no trailer; the size prefix bounds the payload.
    }

    /**
     * Writes a map header. Empty maps emit a single zero byte; non-empty maps emit a varint size followed by a
     * key-type/value-type byte (high nibble = key type, low nibble = value type).
     *
     * <p>Note: parquet.thrift does not use MAP, so {@link CompactProtocolReader} does not decode it. This entry point
     * exists for completeness so the writer remains a faithful encoder of the full Thrift Compact Protocol; other
     * Thrift IDLs that include MAP fields can use it.
     */
    public void writeMapBegin(CompactType keyType, CompactType valueType, int size) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("negative map size: " + size);
        }
        if (size == 0) {
            out.write(0x00);
            return;
        }
        writeVarLong(size);
        out.write((keyType.code << 4) | valueType.code);
    }

    /** No-op terminator for symmetry with {@link #writeMapBegin(CompactType, CompactType, int)}. */
    public void writeMapEnd() {
        // Compact protocol maps have no trailer; the size prefix bounds the payload.
    }

    // -- field convenience writers ---------------------------------------------------------------

    /**
     * Writes a boolean field. The value is encoded directly in the field header type nibble
     * ({@link CompactType#BOOLEAN_TRUE} or {@link CompactType#BOOLEAN_FALSE}); no payload follows.
     */
    public void writeBoolField(short id, boolean value) throws IOException {
        CompactType type = value ? CompactType.BOOLEAN_TRUE : CompactType.BOOLEAN_FALSE;
        writeFieldBegin(id, type);
    }

    public void writeByteField(short id, byte value) throws IOException {
        writeFieldBegin(id, CompactType.BYTE);
        writeByte(value);
    }

    public void writeI16Field(short id, short value) throws IOException {
        writeFieldBegin(id, CompactType.I16);
        writeI16(value);
    }

    public void writeI32Field(short id, int value) throws IOException {
        writeFieldBegin(id, CompactType.I32);
        writeI32(value);
    }

    public void writeI64Field(short id, long value) throws IOException {
        writeFieldBegin(id, CompactType.I64);
        writeI64(value);
    }

    public void writeDoubleField(short id, double value) throws IOException {
        writeFieldBegin(id, CompactType.DOUBLE);
        writeDouble(value);
    }

    public void writeBinaryField(short id, byte[] value) throws IOException {
        writeFieldBegin(id, CompactType.BINARY);
        writeBinary(value);
    }

    public void writeStringField(short id, String value) throws IOException {
        writeFieldBegin(id, CompactType.BINARY);
        writeString(value);
    }

    /**
     * Writes a list field: field header (LIST), then list header, then each element via {@code itemWriter}. The
     * {@code itemWriter} lambda receives the same writer instance so callers can compose nested writes (lists of
     * structs, lists of binaries, etc.).
     */
    public <T> void writeListField(short id, CompactType elementType, List<? extends T> items, ItemWriter<T> itemWriter)
            throws IOException {
        writeFieldBegin(id, CompactType.LIST);
        writeListBegin(elementType, items.size());
        for (T item : items) {
            itemWriter.write(this, item);
        }
        writeListEnd();
    }

    /** Writes a set field. Sets share the list wire form. */
    public <T> void writeSetField(short id, CompactType elementType, List<? extends T> items, ItemWriter<T> itemWriter)
            throws IOException {
        writeFieldBegin(id, CompactType.SET);
        writeSetBegin(elementType, items.size());
        for (T item : items) {
            itemWriter.write(this, item);
        }
        writeSetEnd();
    }

    /**
     * Writes a map field with caller-supplied key and value serializers. Note: parquet.thrift does not use MAP; this
     * method exists for completeness with the Thrift IDL surface.
     */
    public <K, V> void writeMapField(
            short id,
            CompactType keyType,
            CompactType valueType,
            Map<? extends K, ? extends V> entries,
            ItemWriter<K> keyWriter,
            ItemWriter<V> valueWriter)
            throws IOException {
        writeFieldBegin(id, CompactType.MAP);
        writeMapBegin(keyType, valueType, entries.size());
        for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
            keyWriter.write(this, entry.getKey());
            valueWriter.write(this, entry.getValue());
        }
        writeMapEnd();
    }

    /** Functional interface for writing one element of a list, set, or map through a writer instance. */
    @FunctionalInterface
    public interface ItemWriter<T> {
        void write(CompactProtocolWriter writer, T item) throws IOException;
    }

    // -- low-level helpers -----------------------------------------------------------------------

    /** Writes an unsigned varint (7 bits per byte, high bit signals continuation). */
    void writeVarLong(long value) throws IOException {
        long remaining = value;
        while ((remaining & ~0x7fL) != 0L) {
            out.write((int) ((remaining & 0x7f) | 0x80));
            remaining >>>= 7;
        }
        out.write((int) remaining);
    }

    /** Writes a signed integer as zigzag-encoded varint: {@code (n << 1) ^ (n >> 63)}. */
    void writeZigzagVarLong(long value) throws IOException {
        long zz = (value << 1) ^ (value >> 63);
        writeVarLong(zz);
    }
}
