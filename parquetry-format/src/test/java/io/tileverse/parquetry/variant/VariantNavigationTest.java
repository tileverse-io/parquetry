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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.ParquetFormatException;

/**
 * Checked navigation over hand-built Variant value buffers. The malformed layouts follow the {@code bad_data/variants}
 * files of {@code apache/parquet-testing}; each must fail with a {@link ParquetFormatException} while a consumer walks
 * the value, never with a raw bounds or arithmetic exception.
 */
class VariantNavigationTest {

    /** Version 1, one-byte offsets, an empty dictionary. */
    private static final byte[] EMPTY_METADATA = {0x01, 0x00, 0x00};

    /** Version 1, one-byte offsets, the dictionary {"a", "b"}. */
    private static final byte[] KEYS_A_B = {0x01, 0x02, 0x00, 0x01, 0x02, 'a', 'b'};

    @Test
    void readsFieldsSharingOneValueThroughEqualOffsets() {
        // object header 0x02, 2 fields, ids [0, 1], offsets [0, 0, 1], one TRUE value referenced by both fields
        byte[] value = {0x02, 0x02, 0x00, 0x01, 0x00, 0x00, 0x01, 0x04};
        Variant object = variant(KEYS_A_B, value);

        assertThat(object.type()).as("type").isEqualTo(Variant.Type.OBJECT);
        assertThat(object.fieldNames()).as("field names").containsExactly("a", "b");
        assertThat(object.getField("a").getBoolean()).as("field a").isTrue();
        assertThat(object.getField("b").getBoolean()).as("field b").isTrue();
        assertThat(bytesOf(object.boundedFieldValueAtIndex(0)))
                .as("bounded field a")
                .containsExactly(0x04);
        assertThat(bytesOf(object.boundedFieldValueAtIndex(1)))
                .as("bounded field b")
                .containsExactly(0x04);
    }

    static Stream<Arguments> malformedValues() {
        return Stream.of(
                malformed(
                        "array count overflowing 32-bit offset arithmetic",
                        EMPTY_METADATA,
                        bytes(0x13, 0xFF, 0xFF, 0xFF, 0x7F),
                        "ARRAY offset table needs"),
                malformed(
                        "object count overflowing 32-bit offset arithmetic",
                        EMPTY_METADATA,
                        bytes(0x42, 0xFF, 0xFF, 0xFF, 0xFF),
                        "OBJECT offset table needs"),
                malformed(
                        "array count larger than the buffer",
                        EMPTY_METADATA,
                        bytes(0x13, 0x00, 0x00, 0x00, 0x10, 0x00),
                        "ARRAY offset table needs"),
                malformed(
                        "array count cut short",
                        EMPTY_METADATA,
                        bytes(0x13, 0x01, 0x00),
                        "ARRAY entry count needs 5 bytes"),
                malformed(
                        "data region past the buffer",
                        EMPTY_METADATA,
                        bytes(0x03, 0x01, 0x00, 0xFF, 0x00),
                        "ARRAY data needs 259 bytes"),
                malformed(
                        "child offset past the data region",
                        EMPTY_METADATA,
                        bytes(0x03, 0x01, 0xFF, 0x01, 0x00),
                        "entry 0 starts at offset 255, outside its 1-byte data region"),
                malformed(
                        "child without a header byte",
                        EMPTY_METADATA,
                        bytes(0x03, 0x01, 0x00, 0x00),
                        "outside its 0-byte data region"),
                malformed(
                        "malformed child inside a well-formed parent",
                        EMPTY_METADATA,
                        bytes(0x03, 0x01, 0x00, 0x06, 0x13, 0x00, 0x00, 0x00, 0x10, 0x00),
                        "ARRAY offset table needs"),
                malformed(
                        "field id past the dictionary",
                        bytes(0x01, 0x01, 0x00, 0x01, 'a'),
                        bytes(0x02, 0x01, 0x05, 0x00, 0x01, 0x00),
                        "key id 5, out of range for a dictionary of 1 keys"),
                malformed(
                        "field key with decreasing dictionary offsets",
                        bytes(0x01, 0x02, 0x00, 0x05, 0x03, 'A', 'B', 'C', 'D', 'E'),
                        bytes(0x02, 0x01, 0x01, 0x00, 0x01, 0x00),
                        "key 1 has offsets [5, 3)"),
                malformed(
                        "string length past the buffer",
                        EMPTY_METADATA,
                        bytes(0x40, 0xFF, 0xFF, 0xFF, 0x7F),
                        "STRING payload needs 2147483652 bytes"),
                malformed(
                        "string without its length prefix",
                        EMPTY_METADATA,
                        bytes(0x40),
                        "STRING length prefix needs 5 bytes"),
                malformed(
                        "binary length past the buffer",
                        EMPTY_METADATA,
                        bytes(0x3C, 0x05, 0x00, 0x00, 0x00, 0x01),
                        "BINARY payload needs 10 bytes"),
                malformed("short string longer than the buffer", EMPTY_METADATA, bytes(0x1D), "short string needs 8"),
                malformed("truncated int8", EMPTY_METADATA, bytes(0x0C), "INT8 payload needs 2 bytes"),
                malformed("truncated int32", EMPTY_METADATA, bytes(0x14, 0x01, 0x02), "INT32 payload needs 5"),
                malformed("truncated double", EMPTY_METADATA, bytes(0x1C, 0x01), "DOUBLE payload needs 9"),
                malformed("truncated timestamp", EMPTY_METADATA, bytes(0x30, 0x01), "TIMESTAMP_TZ payload needs 9"),
                malformed("truncated decimal16", EMPTY_METADATA, bytes(0x28, 0x02, 0x01), "DECIMAL16 payload needs 18"),
                malformed("truncated uuid", EMPTY_METADATA, bytes(0x50, 0x01, 0x02), "UUID payload needs 17"),
                malformed(
                        "unknown primitive type",
                        EMPTY_METADATA,
                        bytes(0x03, 0x03, 0x00, 0x02, 0x03, 0x05, 0x0C, 0x0A, 0xFC, 0x0C, 0x14),
                        "primitive type id 63"));
    }

    private static Arguments malformed(String description, byte[] metadata, byte[] value, String messageFragment) {
        return Arguments.of(description, metadata, value, messageFragment);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedValues")
    void rejectsMalformedValueDuringAWalk(String description, byte[] metadata, byte[] value, String messageFragment) {
        Variant variant = variant(metadata, value);

        assertThatThrownBy(() -> walk(variant))
                .as(description)
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining(messageFragment);
    }

    @Test
    void rejectsAnEmptyValue() {
        assertThatThrownBy(() -> variant(EMPTY_METADATA, new byte[0]))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("Variant value is empty");
    }

    @Test
    void rejectsAMalformedChildOnlyWhenDescendingIntoIt() {
        // outer array of one 6-byte element; the inner array declares 0x10000000 elements
        byte[] value = bytes(0x03, 0x01, 0x00, 0x06, 0x13, 0x00, 0x00, 0x00, 0x10, 0x00);
        Variant outer = variant(EMPTY_METADATA, value);

        assertThat(outer.numElements()).as("outer element count").isEqualTo(1);
        Variant inner = outer.getElement(0);
        assertThatThrownBy(inner::numElements).as("inner element count").isInstanceOf(ParquetFormatException.class);
    }

    @Test
    void readsTheEntriesAroundAnUnknownPrimitive() {
        // array of three entries: INT8 10, an unknown primitive (id 63), INT8 20
        byte[] value = bytes(0x03, 0x03, 0x00, 0x02, 0x03, 0x05, 0x0C, 0x0A, 0xFC, 0x0C, 0x14);
        Variant array = variant(EMPTY_METADATA, value);

        assertThat(array.getElement(0).getByte()).as("first entry").isEqualTo((byte) 10);
        assertThat(array.getElement(2).getByte()).as("third entry").isEqualTo((byte) 20);
        Variant unknown = array.getElement(1);
        assertThatThrownBy(unknown::type).as("unknown entry").isInstanceOf(ParquetFormatException.class);
    }

    @Test
    void confinesAChildToItsContainerDataRegion() {
        // array with a 2-byte data region holding an INT32 header; the 3 bytes after the region are not its payload
        byte[] value = bytes(0x03, 0x01, 0x00, 0x02, 0x14, 0x01, 0x02, 0x03, 0x04);
        Variant array = variant(EMPTY_METADATA, value);

        Variant entry = array.getElement(0);
        assertThatThrownBy(entry::getInt)
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("INT32 payload needs 5 bytes but the value has 2");
    }

    @Test
    void rejectsNestingBeyondTheMaximumDepth() {
        Variant cursor = variant(EMPTY_METADATA, nestedArrayChain(Variant.MAX_DEPTH + 10));

        for (int level = 1; level <= Variant.MAX_DEPTH; level++) {
            cursor = cursor.getElement(0);
        }
        Variant deepest = cursor;
        assertThatThrownBy(() -> deepest.getElement(0))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("nesting depth exceeds the maximum of 500");
    }

    @Test
    void detachedViewKeepsItsDepth() {
        Variant cursor = variant(EMPTY_METADATA, nestedArrayChain(Variant.MAX_DEPTH + 1));
        for (int level = 1; level <= Variant.MAX_DEPTH; level++) {
            cursor = cursor.getElement(0);
        }

        Variant detached = cursor.detach();
        assertThatThrownBy(() -> detached.getElement(0))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("nesting depth");
    }

    /**
     * A chain of {@code levels} single-element arrays around a NULL leaf. Each array uses two-byte offsets, taking six
     * bytes: header, element count, then the offsets 0 and the size of the nested value.
     */
    private static byte[] nestedArrayChain(int levels) {
        int bytesPerLevel = 6;
        int total = bytesPerLevel * levels + 1;
        byte[] chain = new byte[total];
        for (int level = 0; level < levels; level++) {
            int position = level * bytesPerLevel;
            int nestedSize = total - position - bytesPerLevel;
            chain[position] = 0x07;
            chain[position + 1] = 0x01;
            chain[position + 4] = (byte) (nestedSize & 0xFF);
            chain[position + 5] = (byte) ((nestedSize >> 8) & 0xFF);
        }
        return chain;
    }

    private static void walk(Variant node) {
        switch (node.type()) {
            case OBJECT -> walkObject(node);
            case ARRAY -> walkArray(node);
            default -> readScalar(node);
        }
    }

    private static void walkObject(Variant object) {
        List<String> names = object.fieldNames();
        for (int i = 0; i < names.size(); i++) {
            walk(object.getFieldAtIndex(i));
        }
    }

    private static void walkArray(Variant array) {
        int count = array.numElements();
        for (int i = 0; i < count; i++) {
            walk(array.getElement(i));
        }
    }

    private static Object readScalar(Variant scalar) {
        return switch (scalar.type()) {
            case NULL -> null;
            case BOOLEAN -> scalar.getBoolean();
            case INT8 -> scalar.getByte();
            case INT16 -> scalar.getShort();
            case INT32 -> scalar.getInt();
            case INT64 -> scalar.getLong();
            case FLOAT -> scalar.getFloat();
            case DOUBLE -> scalar.getDouble();
            case DECIMAL4, DECIMAL8, DECIMAL16 -> scalar.getBigDecimal();
            case DATE -> scalar.getDateDays();
            case TIME_NTZ -> scalar.getTimeMicros();
            case TIMESTAMP_TZ, TIMESTAMP_NTZ -> scalar.getTimestampMicros();
            case TIMESTAMP_NANOS_TZ, TIMESTAMP_NANOS_NTZ -> scalar.getTimestampNanos();
            case BINARY -> scalar.getBinary();
            case STRING -> scalar.getString();
            case UUID -> scalar.getUuid();
            case OBJECT, ARRAY -> throw new IllegalArgumentException("not a scalar: " + scalar.type());
        };
    }

    private static Variant variant(byte[] metadata, byte[] value) {
        VariantMetadata dictionary =
                new VariantMetadata(MemorySegment.ofArray(metadata).asReadOnly());
        return Variant.of(MemorySegment.ofArray(value).asReadOnly(), dictionary);
    }

    private static int[] bytesOf(MemorySegment segment) {
        byte[] raw = segment.toArray(JAVA_BYTE);
        int[] unsigned = new int[raw.length];
        for (int i = 0; i < raw.length; i++) {
            unsigned[i] = raw[i] & 0xFF;
        }
        return unsigned;
    }

    private static byte[] bytes(int... values) {
        byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }
}
