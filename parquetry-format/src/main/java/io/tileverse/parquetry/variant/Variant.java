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
package io.tileverse.parquetry.variant;

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.io.Segments;
import io.tileverse.parquetry.schema.UuidConverter;

/**
 * A lazy, random-access view over one Parquet Variant value. Wraps the value buffer slice for this node plus the shared
 * metadata dictionary; navigation reads on demand and allocates proportional to the path actually read. Children are
 * fresh views over sub-slices of the same value buffer.
 *
 * <p>The view does not trust the buffer. Each read bounds-checks its headers, counts, offsets and lengths against the
 * slice in O(1) and throws {@link ParquetFormatException} on malformed bytes; nothing validates the whole tree up
 * front. A child view knows its nesting depth and a child deeper than {@value #MAX_DEPTH} levels is rejected,
 * protecting recursive consumers from stack exhaustion on hostile input.
 */
public final class Variant {

    /**
     * The deepest nesting level accepted for a child view, counting the top-level value as level 0. The limit matches
     * the nesting limit of the parquet-java Variant JSON parser, the reference used by the parquet-testing corpus.
     */
    static final int MAX_DEPTH = 500;

    public enum Type {
        NULL,
        BOOLEAN,
        INT8,
        INT16,
        INT32,
        INT64,
        FLOAT,
        DOUBLE,
        DECIMAL4,
        DECIMAL8,
        DECIMAL16,
        DATE,
        TIMESTAMP_TZ,
        TIMESTAMP_NTZ,
        TIMESTAMP_NANOS_TZ,
        TIMESTAMP_NANOS_NTZ,
        TIME_NTZ,
        BINARY,
        STRING,
        UUID,
        OBJECT,
        ARRAY
    }

    private static final int BASIC_TYPE_MASK = 0x03;
    private static final int BASIC_TYPE_SHORT_STRING = 1;
    private static final int BASIC_TYPE_OBJECT = 2;
    private static final int BASIC_TYPE_ARRAY = 3;

    private static final long PAYLOAD_START = 1L;
    private static final int LENGTH_PREFIX_BYTES = Integer.BYTES;
    private static final int DECIMAL_SCALE_BYTES = 1;
    private static final int INT128_BYTES = 16;
    private static final int UUID_BYTES = 16;
    private static final int SMALL_COUNT_BYTES = 1;
    private static final int LARGE_COUNT_BYTES = 4;

    private final MemorySegment value;
    private final VariantMetadata metadata;
    private final int depth;

    public static Variant of(MemorySegment value, VariantMetadata metadata) {
        return new Variant(value, metadata, 0);
    }

    private Variant(MemorySegment value, VariantMetadata metadata, int depth) {
        requireDepthWithinLimit(depth);
        requireHeaderByte(value);
        this.value = value;
        this.metadata = metadata;
        this.depth = depth;
    }

    private static void requireDepthWithinLimit(int depth) {
        if (depth > MAX_DEPTH) {
            throw new ParquetFormatException("Variant nesting depth exceeds the maximum of " + MAX_DEPTH + " levels");
        }
    }

    private static void requireHeaderByte(MemorySegment value) {
        if (value.byteSize() == 0) {
            throw new ParquetFormatException("Variant value is empty");
        }
    }

    /** The canonical serialized form: the metadata bytes followed by the value bytes. */
    public byte[] serialize() {
        byte[] metadataBytes = metadata.rawBytes();
        byte[] valueBytes = value.toArray(JAVA_BYTE);
        byte[] result = new byte[metadataBytes.length + valueBytes.length];
        System.arraycopy(metadataBytes, 0, result, 0, metadataBytes.length);
        System.arraycopy(valueBytes, 0, result, metadataBytes.length, valueBytes.length);
        return result;
    }

    /**
     * Returns a copy of this value backed by fresh read-only heap segments, decoupled from the batch's page buffer.
     * Both the value bytes and the shared metadata dictionary are copied, letting the result outlive the batch it came
     * from. The copy keeps this view's nesting depth.
     */
    public Variant detach() {
        MemorySegment detachedValue = Segments.toHeapReadOnly(value);
        return new Variant(detachedValue, metadata.detach(), depth);
    }

    public Type type() {
        int header = header();
        int basicType = header & BASIC_TYPE_MASK;
        return switch (basicType) {
            case BASIC_TYPE_SHORT_STRING -> Type.STRING;
            case BASIC_TYPE_OBJECT -> Type.OBJECT;
            case BASIC_TYPE_ARRAY -> Type.ARRAY;
            default -> primitiveType(header >> 2);
        };
    }

    private Type primitiveType(int id) {
        return switch (id) {
            case 0 -> Type.NULL;
            case 1, 2 -> Type.BOOLEAN;
            case 3 -> Type.INT8;
            case 4 -> Type.INT16;
            case 5 -> Type.INT32;
            case 6 -> Type.INT64;
            case 7 -> Type.DOUBLE;
            case 8 -> Type.DECIMAL4;
            case 9 -> Type.DECIMAL8;
            case 10 -> Type.DECIMAL16;
            case 11 -> Type.DATE;
            case 12 -> Type.TIMESTAMP_TZ;
            case 13 -> Type.TIMESTAMP_NTZ;
            case 14 -> Type.FLOAT;
            case 15 -> Type.BINARY;
            case 16 -> Type.STRING;
            case 17 -> Type.TIME_NTZ;
            case 18 -> Type.TIMESTAMP_NANOS_TZ;
            case 19 -> Type.TIMESTAMP_NANOS_NTZ;
            case 20 -> Type.UUID;
            default -> throw new ParquetFormatException("Unknown variant primitive type id " + id);
        };
    }

    public boolean getBoolean() {
        require(Type.BOOLEAN, "getBoolean");
        return (header() >> 2) == 1;
    }

    public byte getByte() {
        requireScalar(Type.INT8, "getByte");
        return value.get(JAVA_BYTE, PAYLOAD_START);
    }

    public short getShort() {
        requireScalar(Type.INT16, "getShort");
        int low = value.get(JAVA_BYTE, PAYLOAD_START) & 0xFF;
        int high = value.get(JAVA_BYTE, PAYLOAD_START + 1) & 0xFF;
        return (short) (low | (high << 8));
    }

    public int getInt() {
        requireScalar(Type.INT32, "getInt");
        return value.get(INT32, PAYLOAD_START);
    }

    public long getLong() {
        requireScalar(Type.INT64, "getLong");
        return value.get(INT64, PAYLOAD_START);
    }

    public float getFloat() {
        requireScalar(Type.FLOAT, "getFloat");
        return value.get(FLOAT, PAYLOAD_START);
    }

    public double getDouble() {
        requireScalar(Type.DOUBLE, "getDouble");
        return value.get(DOUBLE, PAYLOAD_START);
    }

    public String getString() {
        int header = header();
        if ((header & BASIC_TYPE_MASK) == BASIC_TYPE_SHORT_STRING) {
            return readUtf8(PAYLOAD_START, shortStringLength(header));
        }
        require(Type.STRING, "getString");
        long length = lengthPrefixedPayloadLength(Type.STRING);
        return readUtf8(PAYLOAD_START + LENGTH_PREFIX_BYTES, length);
    }

    public BigDecimal getBigDecimal() {
        Type actual = requireDecimal();
        requirePayload(actual);
        int scale = value.get(JAVA_BYTE, PAYLOAD_START) & 0xFF;
        return new BigDecimal(unscaledDecimal(actual), scale);
    }

    private Type requireDecimal() {
        Type actual = type();
        if (actual != Type.DECIMAL4 && actual != Type.DECIMAL8 && actual != Type.DECIMAL16) {
            throw new IllegalStateException("getBigDecimal called on " + actual);
        }
        return actual;
    }

    private BigInteger unscaledDecimal(Type type) {
        long unscaledStart = PAYLOAD_START + DECIMAL_SCALE_BYTES;
        return switch (type) {
            case DECIMAL4 -> BigInteger.valueOf(value.get(INT32, unscaledStart));
            case DECIMAL8 -> BigInteger.valueOf(value.get(INT64, unscaledStart));
            case DECIMAL16 -> readInt128(unscaledStart);
            default -> throw new IllegalStateException("getBigDecimal called on " + type);
        };
    }

    public MemorySegment getBinary() {
        require(Type.BINARY, "getBinary");
        long length = lengthPrefixedPayloadLength(Type.BINARY);
        return value.asSlice(PAYLOAD_START + LENGTH_PREFIX_BYTES, length).asReadOnly();
    }

    public UUID getUuid() {
        requireScalar(Type.UUID, "getUuid");
        return UuidConverter.fromSegment(value, PAYLOAD_START);
    }

    public int getDateDays() {
        requireScalar(Type.DATE, "getDateDays");
        return value.get(INT32, PAYLOAD_START);
    }

    public long getTimeMicros() {
        requireScalar(Type.TIME_NTZ, "getTimeMicros");
        return value.get(INT64, PAYLOAD_START);
    }

    public long getTimestampMicros() {
        Type actual = requireTimestampMicros();
        requirePayload(actual);
        return value.get(INT64, PAYLOAD_START);
    }

    public long getTimestampNanos() {
        Type actual = requireTimestampNanos();
        requirePayload(actual);
        return value.get(INT64, PAYLOAD_START);
    }

    private Type requireTimestampMicros() {
        Type actual = type();
        if (actual != Type.TIMESTAMP_TZ && actual != Type.TIMESTAMP_NTZ) {
            throw new IllegalStateException("getTimestampMicros called on " + actual);
        }
        return actual;
    }

    private Type requireTimestampNanos() {
        Type actual = type();
        if (actual != Type.TIMESTAMP_NANOS_TZ && actual != Type.TIMESTAMP_NANOS_NTZ) {
            throw new IllegalStateException("getTimestampNanos called on " + actual);
        }
        return actual;
    }

    public int numElements() {
        requireBasicType(BASIC_TYPE_ARRAY, "numElements");
        return arrayLayout().count();
    }

    public Variant getElement(int index) {
        requireBasicType(BASIC_TYPE_ARRAY, "getElement");
        ContainerLayout layout = arrayLayout();
        requireIndexInRange(index, layout.count(), "element");
        return childAt(layout, index);
    }

    public int numFields() {
        requireBasicType(BASIC_TYPE_OBJECT, "numFields");
        return objectLayout().count();
    }

    public Variant getField(String key) {
        requireBasicType(BASIC_TYPE_OBJECT, "getField");
        int id = metadata.idOf(key);
        if (id < 0) {
            return null;
        }
        ContainerLayout layout = objectLayout();
        int position = indexOfField(layout, key);
        if (position < 0) {
            return null;
        }
        return childAt(layout, position);
    }

    public List<String> fieldNames() {
        requireBasicType(BASIC_TYPE_OBJECT, "fieldNames");
        ContainerLayout layout = objectLayout();
        List<String> names = new ArrayList<>(layout.count());
        for (int i = 0; i < layout.count(); i++) {
            names.add(metadata.key(fieldIdAt(layout, i)));
        }
        return names;
    }

    public Variant getFieldAtIndex(int index) {
        requireBasicType(BASIC_TYPE_OBJECT, "getFieldAtIndex");
        ContainerLayout layout = objectLayout();
        requireIndexInRange(index, layout.count(), "field");
        return childAt(layout, index);
    }

    private int indexOfField(ContainerLayout layout, String key) {
        int low = 0;
        int high = layout.count() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            String midKey = metadata.key(fieldIdAt(layout, mid));
            int comparison = midKey.compareTo(key);
            if (comparison < 0) {
                low = mid + 1;
            } else if (comparison > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    /**
     * The value bytes of the field at {@code index}, bounded to that field's exact encoded length. A child view's
     * {@link #value()} spans from the field's start to the end of the object's data; this returns only the field's own
     * bytes, as required by a writer splicing a non-last field without its trailing siblings. The length comes from the
     * field's own encoding because field offsets need not be monotonic: two fields may share one value.
     */
    public MemorySegment boundedFieldValueAtIndex(int index) {
        requireBasicType(BASIC_TYPE_OBJECT, "boundedFieldValueAtIndex");
        ContainerLayout layout = objectLayout();
        requireIndexInRange(index, layout.count(), "field");
        Variant field = childAt(layout, index);
        return field.value.asSlice(0, field.encodedSize()).asReadOnly();
    }

    /** The number of bytes encoding this node: its header plus its payload, or its whole container. */
    private long encodedSize() {
        int header = header();
        return switch (header & BASIC_TYPE_MASK) {
            case BASIC_TYPE_SHORT_STRING -> PAYLOAD_START + shortStringLength(header);
            case BASIC_TYPE_OBJECT -> objectLayout().encodedSize();
            case BASIC_TYPE_ARRAY -> arrayLayout().encodedSize();
            default -> primitiveSize(type());
        };
    }

    private long primitiveSize(Type type) {
        if (type == Type.BINARY || type == Type.STRING) {
            return PAYLOAD_START + LENGTH_PREFIX_BYTES + lengthPrefixedPayloadLength(type);
        }
        requirePayload(type);
        return PAYLOAD_START + fixedPayloadBytes(type);
    }

    /**
     * The checked shape of an object or array: the entry count, the field id table (objects only), the offset table,
     * and the data region holding the entries' values. The last offset table entry is the data region size.
     */
    private record ContainerLayout(
            Type type,
            int count,
            int idSize,
            int offsetSize,
            long idTableStart,
            long offsetTableStart,
            long valuesStart,
            long dataSize) {

        long encodedSize() {
            return valuesStart + dataSize;
        }
    }

    private ContainerLayout objectLayout() {
        int valueHeader = header() >> 2;
        int offsetSize = (valueHeader & 0x03) + 1;
        int idSize = ((valueHeader >> 2) & 0x03) + 1;
        boolean isLarge = ((valueHeader >> 4) & 0x01) != 0;
        return containerLayout(Type.OBJECT, isLarge, idSize, offsetSize);
    }

    private ContainerLayout arrayLayout() {
        int valueHeader = header() >> 2;
        int offsetSize = (valueHeader & 0x03) + 1;
        boolean isLarge = ((valueHeader >> 2) & 0x01) != 0;
        return containerLayout(Type.ARRAY, isLarge, 0, offsetSize);
    }

    private ContainerLayout containerLayout(Type type, boolean isLarge, int idSize, int offsetSize) {
        int countSize = isLarge ? LARGE_COUNT_BYTES : SMALL_COUNT_BYTES;
        long idTableStart = PAYLOAD_START + countSize;
        requireAvailable(idTableStart, type, "entry count");
        long count = readUnsigned(PAYLOAD_START, countSize);
        long offsetTableStart = idTableStart + count * idSize;
        long valuesStart = offsetTableStart + (count + 1) * offsetSize;
        requireAvailable(valuesStart, type, "offset table");
        long dataSize = readUnsigned(valuesStart - offsetSize, offsetSize);
        requireAvailable(valuesStart + dataSize, type, "data");
        return new ContainerLayout(
                type,
                indexableCount(count, type),
                idSize,
                offsetSize,
                idTableStart,
                offsetTableStart,
                valuesStart,
                dataSize);
    }

    private static int indexableCount(long count, Type type) {
        if (count > Integer.MAX_VALUE) {
            throw new ParquetFormatException(
                    "Variant " + type + " declares " + count + " entries, more than a view can index");
        }
        return (int) count;
    }

    /**
     * A child view over the entry at {@code position}, spanning from the entry's offset to the end of the container's
     * data region.
     */
    private Variant childAt(ContainerLayout layout, int position) {
        long offset = entryOffset(layout, position);
        MemorySegment entry = value.asSlice(layout.valuesStart() + offset, layout.dataSize() - offset);
        return new Variant(entry, metadata, depth + 1);
    }

    /** Reads an entry offset. An entry needs at least its header byte: the offset must lie strictly inside the data. */
    private long entryOffset(ContainerLayout layout, int position) {
        int offsetSize = layout.offsetSize();
        long offset = readUnsigned(layout.offsetTableStart() + (long) position * offsetSize, offsetSize);
        if (offset >= layout.dataSize()) {
            throw new ParquetFormatException("Variant " + layout.type() + " entry " + position + " starts at offset "
                    + offset + ", outside its " + layout.dataSize() + "-byte data region");
        }
        return offset;
    }

    private int fieldIdAt(ContainerLayout layout, int position) {
        long id = readUnsigned(layout.idTableStart() + (long) position * layout.idSize(), layout.idSize());
        int dictionarySize = metadata.dictionarySize();
        if (id >= dictionarySize) {
            throw new ParquetFormatException("Variant OBJECT field " + position + " has key id " + id
                    + ", out of range for a dictionary of " + dictionarySize + " keys");
        }
        return (int) id;
    }

    private void requireBasicType(int basicType, String accessor) {
        if ((header() & BASIC_TYPE_MASK) != basicType) {
            throw new IllegalStateException(accessor + " called on " + type());
        }
    }

    /** The read-only value buffer slice for this node, the bytes a writer emits to the {@code value} leaf. */
    public MemorySegment value() {
        return value.asReadOnly();
    }

    /** The shared metadata dictionary, the source of the bytes a writer emits to the {@code metadata} leaf. */
    public VariantMetadata metadata() {
        return metadata;
    }

    private int header() {
        return value.get(JAVA_BYTE, 0) & 0xFF;
    }

    private void require(Type expected, String accessor) {
        Type actual = type();
        if (actual != expected) {
            throw new IllegalStateException(accessor + " called on " + actual);
        }
    }

    private static void requireIndexInRange(int index, int count, String kind) {
        if (index < 0 || index >= count) {
            throw new IndexOutOfBoundsException(kind + " index " + index + " out of [0," + count + ")");
        }
    }

    private void requireScalar(Type expected, String accessor) {
        require(expected, accessor);
        requirePayload(expected);
    }

    private void requirePayload(Type type) {
        requireAvailable(PAYLOAD_START + fixedPayloadBytes(type), type, "payload");
    }

    private static int fixedPayloadBytes(Type type) {
        return switch (type) {
            case NULL, BOOLEAN -> 0;
            case INT8 -> Byte.BYTES;
            case INT16 -> Short.BYTES;
            case INT32, DATE, FLOAT -> Integer.BYTES;
            case INT64, DOUBLE, TIME_NTZ, TIMESTAMP_TZ, TIMESTAMP_NTZ, TIMESTAMP_NANOS_TZ, TIMESTAMP_NANOS_NTZ ->
                Long.BYTES;
            case DECIMAL4 -> DECIMAL_SCALE_BYTES + Integer.BYTES;
            case DECIMAL8 -> DECIMAL_SCALE_BYTES + Long.BYTES;
            case DECIMAL16 -> DECIMAL_SCALE_BYTES + INT128_BYTES;
            case UUID -> UUID_BYTES;
            case BINARY, STRING, OBJECT, ARRAY ->
                throw new IllegalArgumentException(type + " has no fixed payload size");
        };
    }

    private long shortStringLength(int header) {
        long length = header >> 2;
        requireAvailable(PAYLOAD_START + length, Type.STRING, "short string");
        return length;
    }

    private long lengthPrefixedPayloadLength(Type type) {
        long payloadStart = PAYLOAD_START + LENGTH_PREFIX_BYTES;
        requireAvailable(payloadStart, type, "length prefix");
        long length = readUnsigned(PAYLOAD_START, LENGTH_PREFIX_BYTES);
        requireAvailable(payloadStart + length, type, "payload");
        return length;
    }

    /**
     * Rejects a read reaching past this node's slice. A {@code long} holds each {@code end} without overflow: it adds a
     * few header bytes to products of unsigned 32-bit counts and widths of at most four bytes.
     */
    private void requireAvailable(long end, Type type, String part) {
        long available = value.byteSize();
        if (end > available) {
            throw new ParquetFormatException(
                    "Variant " + type + " " + part + " needs " + end + " bytes but the value has " + available);
        }
    }

    private String readUtf8(long offset, long length) {
        byte[] bytes = value.asSlice(offset, length).toArray(JAVA_BYTE);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private BigInteger readInt128(long offset) {
        byte[] littleEndian = value.asSlice(offset, INT128_BYTES).toArray(JAVA_BYTE);
        byte[] bigEndian = new byte[littleEndian.length];
        for (int i = 0; i < littleEndian.length; i++) {
            bigEndian[i] = littleEndian[littleEndian.length - 1 - i];
        }
        return new BigInteger(bigEndian);
    }

    private long readUnsigned(long offset, int width) {
        long result = 0L;
        for (int i = 0; i < width; i++) {
            int unsignedByte = value.get(JAVA_BYTE, offset + i) & 0xFF;
            result |= (long) unsignedByte << (8 * i);
        }
        return result;
    }
}
