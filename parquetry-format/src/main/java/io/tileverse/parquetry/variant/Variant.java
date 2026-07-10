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
 */
public final class Variant {

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

    private static final int BASIC_TYPE_SHORT_STRING = 1;
    private static final int BASIC_TYPE_OBJECT = 2;
    private static final int BASIC_TYPE_ARRAY = 3;

    private static final long PAYLOAD_START = 1L;
    private static final int LENGTH_PREFIX_BYTES = Integer.BYTES;
    private static final int INT128_BYTES = 16;
    private static final int SMALL_COUNT_BYTES = 1;
    private static final int LARGE_COUNT_BYTES = 4;

    private final MemorySegment value;
    private final VariantMetadata metadata;

    private Variant(MemorySegment value, VariantMetadata metadata) {
        this.value = value;
        this.metadata = metadata;
    }

    public static Variant of(MemorySegment value, VariantMetadata metadata) {
        return new Variant(value, metadata);
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
     * from.
     */
    public Variant detach() {
        MemorySegment detachedValue = Segments.toHeapReadOnly(value);
        return new Variant(detachedValue, metadata.detach());
    }

    public Type type() {
        int header = header();
        int basicType = header & 0x03;
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
        require(Type.INT8, "getByte");
        return value.get(JAVA_BYTE, PAYLOAD_START);
    }

    public short getShort() {
        require(Type.INT16, "getShort");
        int low = value.get(JAVA_BYTE, PAYLOAD_START) & 0xFF;
        int high = value.get(JAVA_BYTE, PAYLOAD_START + 1) & 0xFF;
        return (short) (low | (high << 8));
    }

    public int getInt() {
        require(Type.INT32, "getInt");
        return value.get(INT32, PAYLOAD_START);
    }

    public long getLong() {
        require(Type.INT64, "getLong");
        return value.get(INT64, PAYLOAD_START);
    }

    public float getFloat() {
        require(Type.FLOAT, "getFloat");
        return value.get(FLOAT, PAYLOAD_START);
    }

    public double getDouble() {
        require(Type.DOUBLE, "getDouble");
        return value.get(DOUBLE, PAYLOAD_START);
    }

    public String getString() {
        int header = header();
        if ((header & 0x03) == BASIC_TYPE_SHORT_STRING) {
            int length = header >> 2;
            return readUtf8(PAYLOAD_START, length);
        }
        require(Type.STRING, "getString");
        int length = value.get(INT32, PAYLOAD_START);
        return readUtf8(PAYLOAD_START + LENGTH_PREFIX_BYTES, length);
    }

    public BigDecimal getBigDecimal() {
        Type actual = type();
        BigInteger unscaled = unscaledDecimal(actual);
        int scale = value.get(JAVA_BYTE, PAYLOAD_START) & 0xFF;
        return new BigDecimal(unscaled, scale);
    }

    private BigInteger unscaledDecimal(Type type) {
        long unscaledStart = PAYLOAD_START + 1;
        return switch (type) {
            case DECIMAL4 -> BigInteger.valueOf(value.get(INT32, unscaledStart));
            case DECIMAL8 -> BigInteger.valueOf(value.get(INT64, unscaledStart));
            case DECIMAL16 -> readInt128(unscaledStart);
            default -> throw new IllegalStateException("getBigDecimal called on " + type);
        };
    }

    public MemorySegment getBinary() {
        require(Type.BINARY, "getBinary");
        int length = value.get(INT32, PAYLOAD_START);
        return value.asSlice(PAYLOAD_START + LENGTH_PREFIX_BYTES, length).asReadOnly();
    }

    public UUID getUuid() {
        require(Type.UUID, "getUuid");
        return UuidConverter.fromSegment(value, PAYLOAD_START);
    }

    public int getDateDays() {
        require(Type.DATE, "getDateDays");
        return value.get(INT32, PAYLOAD_START);
    }

    public long getTimeMicros() {
        require(Type.TIME_NTZ, "getTimeMicros");
        return value.get(INT64, PAYLOAD_START);
    }

    public long getTimestampMicros() {
        requireTimestampMicros();
        return value.get(INT64, PAYLOAD_START);
    }

    public long getTimestampNanos() {
        requireTimestampNanos();
        return value.get(INT64, PAYLOAD_START);
    }

    private void requireTimestampMicros() {
        Type actual = type();
        if (actual != Type.TIMESTAMP_TZ && actual != Type.TIMESTAMP_NTZ) {
            throw new IllegalStateException("getTimestampMicros called on " + actual);
        }
    }

    private void requireTimestampNanos() {
        Type actual = type();
        if (actual != Type.TIMESTAMP_NANOS_TZ && actual != Type.TIMESTAMP_NANOS_NTZ) {
            throw new IllegalStateException("getTimestampNanos called on " + actual);
        }
    }

    public int numElements() {
        requireBasicType(BASIC_TYPE_ARRAY, "numElements");
        return readElementCount();
    }

    public Variant getElement(int index) {
        requireBasicType(BASIC_TYPE_ARRAY, "getElement");
        int count = readElementCount();
        if (index < 0 || index >= count) {
            throw new IndexOutOfBoundsException("element index " + index + " out of [0," + count + ")");
        }
        int offsetSize = arrayOffsetSize();
        long offsetTableStart = arrayOffsetTableStart();
        long valuesStart = offsetTableStart + (long) (count + 1) * offsetSize;
        long elementOffset = readUnsigned(offsetTableStart + (long) index * offsetSize, offsetSize);
        return child(valuesStart + elementOffset);
    }

    public int numFields() {
        requireBasicType(BASIC_TYPE_OBJECT, "numFields");
        return objectHeader().numFields();
    }

    public Variant getField(String key) {
        requireBasicType(BASIC_TYPE_OBJECT, "getField");
        int id = metadata.idOf(key);
        if (id < 0) {
            return null;
        }
        ObjectHeader header = objectHeader();
        int position = indexOfField(header, key);
        if (position < 0) {
            return null;
        }
        return fieldAt(header, position);
    }

    public List<String> fieldNames() {
        requireBasicType(BASIC_TYPE_OBJECT, "fieldNames");
        ObjectHeader header = objectHeader();
        List<String> names = new ArrayList<>(header.numFields());
        for (int i = 0; i < header.numFields(); i++) {
            names.add(metadata.key(fieldIdAt(header, i)));
        }
        return names;
    }

    public Variant getFieldAtIndex(int index) {
        requireBasicType(BASIC_TYPE_OBJECT, "getFieldAtIndex");
        ObjectHeader header = objectHeader();
        if (index < 0 || index >= header.numFields()) {
            throw new IndexOutOfBoundsException("field index " + index + " out of [0," + header.numFields() + ")");
        }
        return fieldAt(header, index);
    }

    private int indexOfField(ObjectHeader header, String key) {
        int low = 0;
        int high = header.numFields() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            String midKey = metadata.key(fieldIdAt(header, mid));
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
     * The value bytes of the field at {@code index}, bounded to that field's exact length using the object's offset
     * table. Unlike {@link #value()} on a child view (which slices open-ended from the field's start), this returns
     * only the field's own bytes; a writer splicing a non-last field must not copy the trailing sibling fields.
     */
    public MemorySegment boundedFieldValueAtIndex(int index) {
        requireBasicType(BASIC_TYPE_OBJECT, "boundedFieldValueAtIndex");
        ObjectHeader header = objectHeader();
        if (index < 0 || index >= header.numFields()) {
            throw new IndexOutOfBoundsException("field index " + index + " out of [0," + header.numFields() + ")");
        }
        long start = header.valuesStart() + fieldValueOffset(header, index);
        long end = header.valuesStart() + fieldValueOffset(header, index + 1);
        return value.asSlice(start, end - start).asReadOnly();
    }

    private long fieldValueOffset(ObjectHeader header, int position) {
        return readUnsigned(header.offsetTableStart() + (long) position * header.offsetSize(), header.offsetSize());
    }

    private Variant fieldAt(ObjectHeader header, int position) {
        return child(header.valuesStart() + fieldValueOffset(header, position));
    }

    private int fieldIdAt(ObjectHeader header, int position) {
        return (int) readUnsigned(header.fieldIdStart() + (long) position * header.idSize(), header.idSize());
    }

    private record ObjectHeader(
            int numFields, int idSize, int offsetSize, long fieldIdStart, long offsetTableStart, long valuesStart) {}

    private ObjectHeader objectHeader() {
        int valueHeader = header() >> 2;
        int offsetSize = (valueHeader & 0x03) + 1;
        int idSize = ((valueHeader >> 2) & 0x03) + 1;
        boolean isLarge = ((valueHeader >> 4) & 0x01) != 0;
        int countSize = isLarge ? LARGE_COUNT_BYTES : SMALL_COUNT_BYTES;
        int numFields = (int) readUnsigned(PAYLOAD_START, countSize);
        long fieldIdStart = PAYLOAD_START + countSize;
        long offsetTableStart = fieldIdStart + (long) numFields * idSize;
        long valuesStart = offsetTableStart + (long) (numFields + 1) * offsetSize;
        return new ObjectHeader(numFields, idSize, offsetSize, fieldIdStart, offsetTableStart, valuesStart);
    }

    private int readElementCount() {
        return (int) readUnsigned(PAYLOAD_START, arrayCountSize());
    }

    private long arrayOffsetTableStart() {
        return PAYLOAD_START + arrayCountSize();
    }

    private int arrayCountSize() {
        return isLargeArray() ? LARGE_COUNT_BYTES : SMALL_COUNT_BYTES;
    }

    private int arrayOffsetSize() {
        return ((header() >> 2) & 0x03) + 1;
    }

    private boolean isLargeArray() {
        int valueHeader = header() >> 2;
        return ((valueHeader >> 2) & 0x01) != 0;
    }

    private Variant child(long start) {
        return new Variant(value.asSlice(start), metadata);
    }

    private void requireBasicType(int basicType, String accessor) {
        if ((header() & 0x03) != basicType) {
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

    private String readUtf8(long offset, int length) {
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
