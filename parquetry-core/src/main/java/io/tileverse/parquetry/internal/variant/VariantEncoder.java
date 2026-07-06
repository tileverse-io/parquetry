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
package io.tileverse.parquetry.internal.variant;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.schema.UuidConverter;
import io.tileverse.parquetry.variant.Variant;
import io.tileverse.parquetry.variant.VariantBytes;
import io.tileverse.parquetry.variant.VariantMetadata;

/**
 * Builds one Parquet Variant value in memory, then {@link #encode()} serializes it to a metadata dictionary and a value
 * buffer that the {@link Variant} navigator reads back byte-for-byte.
 *
 * <p>Building workflow: add a single top-level value, or open containers and fill them, then call {@link #encode()}.
 * Scalars are added directly ({@link #addInt(int)}, {@link #addString(String)}, and the other {@code add*} methods).
 * Objects nest via {@link #startObject()}, a {@link #field(String)} call before each value, and {@link #endObject()};
 * arrays via {@link #startArray()} and {@link #endArray()}. {@link #encode()} ends the build and may be called once the
 * top-level value is complete and every opened container has been closed.
 *
 * <p>{@link #encode()} returns an {@link Encoded} pair of read-only segments. The metadata dictionary collects every
 * object key across the whole tree and emits them lexicographically sorted, with the {@code sorted_strings} flag set,
 * matching the {@code idOf} binary search in {@link VariantMetadata}.
 */
public final class VariantEncoder {

    private static final int METADATA_VERSION = 1;
    private static final int SORTED_STRINGS_FLAG = 0x10;

    private static final int BASIC_TYPE_PRIMITIVE = 0;
    private static final int BASIC_TYPE_SHORT_STRING = 1;
    private static final int BASIC_TYPE_OBJECT = 2;
    private static final int BASIC_TYPE_ARRAY = 3;

    private static final int PRIMITIVE_NULL = 0;
    private static final int PRIMITIVE_BOOLEAN_TRUE = 1;
    private static final int PRIMITIVE_BOOLEAN_FALSE = 2;
    private static final int PRIMITIVE_INT8 = 3;
    private static final int PRIMITIVE_INT16 = 4;
    private static final int PRIMITIVE_INT32 = 5;
    private static final int PRIMITIVE_INT64 = 6;
    private static final int PRIMITIVE_DOUBLE = 7;
    private static final int PRIMITIVE_DECIMAL4 = 8;
    private static final int PRIMITIVE_DECIMAL8 = 9;
    private static final int PRIMITIVE_DECIMAL16 = 10;
    private static final int PRIMITIVE_FLOAT = 14;
    private static final int PRIMITIVE_BINARY = 15;
    private static final int PRIMITIVE_STRING = 16;
    private static final int PRIMITIVE_UUID = 20;

    private static final int SHORT_STRING_MAX_LENGTH = 63;
    private static final int DECIMAL4_MAX_BITS = 31;
    private static final int DECIMAL8_MAX_BITS = 63;
    private static final int DECIMAL16_MAX_BITS = 127;
    private static final int INT128_BYTES = 16;

    private final Deque<Container> openContainers = new ArrayDeque<>();
    private Node root;
    private String pendingFieldName;

    /** The serialized output: a read-only metadata dictionary and a read-only value buffer. */
    public record Encoded(MemorySegment metadata, MemorySegment value) {}

    /**
     * Serializes the built value into a read-only metadata dictionary and value buffer. Call once after the top-level
     * value is complete and every opened object or array has been closed.
     *
     * @return the {@code metadata} and {@code value} segments the {@link Variant} navigator reads back
     * @throws IllegalStateException if nothing was added, or if an object or array is still open
     */
    public Encoded encode() {
        requireCompleteTopLevelValue();
        List<String> dictionary = buildSortedDictionary();
        Map<String, Integer> keyIds = idsByKey(dictionary);
        MemorySegment metadata = encodeMetadata(dictionary);
        MemorySegment value = encodeValue(root, keyIds);
        return new Encoded(metadata, value);
    }

    /**
     * Serializes the built value into a read-only value buffer, resolving object field names against an existing
     * metadata dictionary rather than building a fresh one. Call once after the top-level value is complete and every
     * opened object or array has been closed.
     *
     * @param metadata the dictionary that already holds every object field name in the built value
     * @return the read-only value segment the {@link Variant} navigator reads back against {@code metadata}
     * @throws IllegalStateException if nothing was added, or if an object or array is still open
     * @throws ParquetFormatException if an object field name is absent from {@code metadata}
     */
    public MemorySegment encodeValue(VariantMetadata metadata) {
        requireCompleteTopLevelValue();
        Map<String, Integer> keyIds = idsFromMetadata(root, metadata);
        return encodeValue(root, keyIds);
    }

    private void requireCompleteTopLevelValue() {
        if (root == null) {
            throw new IllegalStateException("nothing was added to encode");
        }
        if (!openContainers.isEmpty()) {
            throw new IllegalStateException("an object or array is still open");
        }
    }

    private Map<String, Integer> idsFromMetadata(Node node, VariantMetadata metadata) {
        TreeSet<String> keys = new TreeSet<>();
        collectKeys(node, keys);
        Map<String, Integer> ids = new LinkedHashMap<>();
        for (String key : keys) {
            int id = metadata.idOf(key);
            if (id < 0) {
                throw new ParquetFormatException("variant field name is absent from the metadata dictionary: " + key);
            }
            ids.put(key, id);
        }
        return ids;
    }

    public VariantEncoder addNull() {
        return add(new PrimitiveNode(PRIMITIVE_NULL, new byte[0]));
    }

    /**
     * Splices a pre-encoded value buffer in verbatim, copying its bytes as the value of the current position. The bytes
     * are emitted unchanged by {@link #encodeValue(VariantMetadata)} and any field ids they reference must already
     * resolve against the metadata dictionary passed there.
     *
     * @param preEncodedValue a value buffer produced by a prior encode
     */
    public VariantEncoder addEncoded(MemorySegment preEncodedValue) {
        return add(new RawValueNode(preEncodedValue.toArray(ValueLayout.JAVA_BYTE)));
    }

    public VariantEncoder addBoolean(boolean value) {
        int id = value ? PRIMITIVE_BOOLEAN_TRUE : PRIMITIVE_BOOLEAN_FALSE;
        return add(new PrimitiveNode(id, new byte[0]));
    }

    public VariantEncoder addByte(byte value) {
        return add(new PrimitiveNode(PRIMITIVE_INT8, new byte[] {value}));
    }

    public VariantEncoder addShort(short value) {
        return add(new PrimitiveNode(PRIMITIVE_INT16, littleEndian(value & 0xFFFFL, Short.BYTES)));
    }

    public VariantEncoder addInt(int value) {
        return add(new PrimitiveNode(PRIMITIVE_INT32, littleEndian(value & 0xFFFFFFFFL, Integer.BYTES)));
    }

    public VariantEncoder addLong(long value) {
        return add(new PrimitiveNode(PRIMITIVE_INT64, littleEndian(value, Long.BYTES)));
    }

    public VariantEncoder addFloat(float value) {
        int bits = Float.floatToRawIntBits(value);
        return add(new PrimitiveNode(PRIMITIVE_FLOAT, littleEndian(bits & 0xFFFFFFFFL, Float.BYTES)));
    }

    public VariantEncoder addDouble(double value) {
        long bits = Double.doubleToRawLongBits(value);
        return add(new PrimitiveNode(PRIMITIVE_DOUBLE, littleEndian(bits, Double.BYTES)));
    }

    public VariantEncoder addString(String value) {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        if (utf8.length <= SHORT_STRING_MAX_LENGTH) {
            return add(new ShortStringNode(utf8));
        }
        return add(new PrimitiveNode(PRIMITIVE_STRING, lengthPrefixed(utf8)));
    }

    public VariantEncoder addBinary(MemorySegment binary) {
        byte[] bytes = binary.toArray(ValueLayout.JAVA_BYTE);
        return add(new PrimitiveNode(PRIMITIVE_BINARY, lengthPrefixed(bytes)));
    }

    public VariantEncoder addUuid(UUID uuid) {
        return add(new PrimitiveNode(PRIMITIVE_UUID, UuidConverter.toBytes(uuid)));
    }

    public VariantEncoder addBigDecimal(BigDecimal decimal) {
        BigInteger unscaled = decimal.unscaledValue();
        int scale = decimal.scale();
        int bitLength = unscaled.bitLength();
        if (bitLength <= DECIMAL4_MAX_BITS) {
            return add(new PrimitiveNode(PRIMITIVE_DECIMAL4, decimalPayload(scale, unscaled, Integer.BYTES)));
        }
        if (bitLength <= DECIMAL8_MAX_BITS) {
            return add(new PrimitiveNode(PRIMITIVE_DECIMAL8, decimalPayload(scale, unscaled, Long.BYTES)));
        }
        if (bitLength <= DECIMAL16_MAX_BITS) {
            return add(new PrimitiveNode(PRIMITIVE_DECIMAL16, decimalPayload(scale, unscaled, INT128_BYTES)));
        }
        throw new IllegalArgumentException("BigDecimal unscaled value exceeds 128 bits: " + decimal);
    }

    public VariantEncoder startObject() {
        ObjectNode object = new ObjectNode();
        attach(object);
        openContainers.push(object);
        return this;
    }

    public VariantEncoder field(String name) {
        if (!(openContainers.peek() instanceof ObjectNode)) {
            throw new IllegalStateException("field(name) is only valid inside an open object");
        }
        this.pendingFieldName = name;
        return this;
    }

    public VariantEncoder endObject() {
        if (!(openContainers.peek() instanceof ObjectNode)) {
            throw new IllegalStateException("endObject() without a matching startObject()");
        }
        openContainers.pop();
        return this;
    }

    public VariantEncoder startArray() {
        ArrayNode array = new ArrayNode();
        attach(array);
        openContainers.push(array);
        return this;
    }

    public VariantEncoder endArray() {
        if (!(openContainers.peek() instanceof ArrayNode)) {
            throw new IllegalStateException("endArray() without a matching startArray()");
        }
        openContainers.pop();
        return this;
    }

    private VariantEncoder add(Node node) {
        attach(node);
        return this;
    }

    private void attach(Node node) {
        Container open = openContainers.peek();
        if (open == null) {
            requireSingleRoot();
            this.root = node;
            return;
        }
        if (open instanceof ObjectNode object) {
            attachToObject(object, node);
            return;
        }
        ArrayNode array = (ArrayNode) open;
        array.elements.add(node);
    }

    private void attachToObject(ObjectNode object, Node node) {
        if (pendingFieldName == null) {
            throw new IllegalStateException("call field(name) before adding a value inside an object");
        }
        object.fields.put(pendingFieldName, node);
        this.pendingFieldName = null;
    }

    private void requireSingleRoot() {
        if (root != null) {
            throw new IllegalStateException("a top-level value was already added");
        }
    }

    private List<String> buildSortedDictionary() {
        TreeSet<String> keys = new TreeSet<>();
        collectKeys(root, keys);
        return new ArrayList<>(keys);
    }

    private void collectKeys(Node node, TreeSet<String> keys) {
        if (node instanceof ObjectNode object) {
            keys.addAll(object.fields.keySet());
            for (Node child : object.fields.values()) {
                collectKeys(child, keys);
            }
            return;
        }
        if (node instanceof ArrayNode array) {
            for (Node element : array.elements) {
                collectKeys(element, keys);
            }
        }
    }

    private Map<String, Integer> idsByKey(List<String> dictionary) {
        Map<String, Integer> ids = new LinkedHashMap<>();
        for (int id = 0; id < dictionary.size(); id++) {
            ids.put(dictionary.get(id), id);
        }
        return ids;
    }

    private MemorySegment encodeMetadata(List<String> dictionary) {
        List<byte[]> keyBytes = utf8Keys(dictionary);
        int totalKeyBytes = sumLengths(keyBytes);
        int offsetSize = minimumWidth(Math.max(dictionary.size(), totalKeyBytes));
        int header = METADATA_VERSION | SORTED_STRINGS_FLAG | ((offsetSize - 1) << 6);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(header);
        writeLittleEndian(out, dictionary.size(), offsetSize);
        int runningOffset = 0;
        writeLittleEndian(out, runningOffset, offsetSize);
        for (byte[] key : keyBytes) {
            runningOffset += key.length;
            writeLittleEndian(out, runningOffset, offsetSize);
        }
        for (byte[] key : keyBytes) {
            out.writeBytes(key);
        }
        return readOnlySegment(out.toByteArray());
    }

    private List<byte[]> utf8Keys(List<String> dictionary) {
        List<byte[]> keyBytes = new ArrayList<>(dictionary.size());
        for (String key : dictionary) {
            keyBytes.add(key.getBytes(StandardCharsets.UTF_8));
        }
        return keyBytes;
    }

    private int sumLengths(List<byte[]> chunks) {
        int total = 0;
        for (byte[] chunk : chunks) {
            total += chunk.length;
        }
        return total;
    }

    private MemorySegment encodeValue(Node node, Map<String, Integer> keyIds) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeNode(out, node, keyIds);
        return readOnlySegment(out.toByteArray());
    }

    private void writeNode(ByteArrayOutputStream out, Node node, Map<String, Integer> keyIds) {
        if (node instanceof PrimitiveNode primitive) {
            writePrimitive(out, primitive);
            return;
        }
        if (node instanceof ShortStringNode shortString) {
            writeShortString(out, shortString);
            return;
        }
        if (node instanceof ObjectNode object) {
            writeObject(out, object, keyIds);
            return;
        }
        if (node instanceof RawValueNode raw) {
            writeRawValue(out, raw);
            return;
        }
        writeArray(out, (ArrayNode) node, keyIds);
    }

    private void writePrimitive(ByteArrayOutputStream out, PrimitiveNode primitive) {
        out.write(header(BASIC_TYPE_PRIMITIVE, primitive.typeId()));
        out.writeBytes(primitive.payload());
    }

    private void writeRawValue(ByteArrayOutputStream out, RawValueNode raw) {
        out.writeBytes(raw.bytes());
    }

    private void writeShortString(ByteArrayOutputStream out, ShortStringNode shortString) {
        out.write(header(BASIC_TYPE_SHORT_STRING, shortString.utf8().length));
        out.writeBytes(shortString.utf8());
    }

    private void writeObject(ByteArrayOutputStream out, ObjectNode object, Map<String, Integer> keyIds) {
        List<Map.Entry<Integer, byte[]>> sortedFields = encodeObjectFields(object, keyIds);
        List<Integer> fieldIds = new ArrayList<>(sortedFields.size());
        List<byte[]> values = new ArrayList<>(sortedFields.size());
        for (Map.Entry<Integer, byte[]> field : sortedFields) {
            fieldIds.add(field.getKey());
            values.add(field.getValue());
        }

        int numElements = sortedFields.size();
        int maxFieldId = maxValue(fieldIds);
        int valuesRegionSize = sumLengths(values);
        int idSize = minimumWidth(maxFieldId);
        int offsetSize = minimumWidth(valuesRegionSize);
        boolean isLarge = numElements > 0xFF;
        int valueHeader = (offsetSize - 1) | ((idSize - 1) << 2) | ((isLarge ? 1 : 0) << 4);

        out.write(header(BASIC_TYPE_OBJECT, valueHeader));
        writeLittleEndian(out, numElements, isLarge ? Integer.BYTES : 1);
        for (int fieldId : fieldIds) {
            writeLittleEndian(out, fieldId, idSize);
        }
        writeOffsetTable(out, values, offsetSize);
        for (byte[] value : values) {
            out.writeBytes(value);
        }
    }

    private List<Map.Entry<Integer, byte[]>> encodeObjectFields(ObjectNode object, Map<String, Integer> keyIds) {
        List<Map.Entry<Integer, byte[]>> fields = new ArrayList<>(object.fields.size());
        for (Map.Entry<String, Node> field : object.fields.entrySet()) {
            int fieldId = keyIds.get(field.getKey());
            byte[] encodedValue = encodeValue(field.getValue(), keyIds).toArray(ValueLayout.JAVA_BYTE);
            fields.add(Map.entry(fieldId, encodedValue));
        }
        fields.sort(Comparator.comparingInt(Map.Entry::getKey));
        return fields;
    }

    private void writeArray(ByteArrayOutputStream out, ArrayNode array, Map<String, Integer> keyIds) {
        List<byte[]> values = new ArrayList<>(array.elements.size());
        for (Node element : array.elements) {
            values.add(encodeValue(element, keyIds).toArray(ValueLayout.JAVA_BYTE));
        }

        int numElements = values.size();
        int valuesRegionSize = sumLengths(values);
        int offsetSize = minimumWidth(valuesRegionSize);
        boolean isLarge = numElements > 0xFF;
        int valueHeader = (offsetSize - 1) | ((isLarge ? 1 : 0) << 2);

        out.write(header(BASIC_TYPE_ARRAY, valueHeader));
        writeLittleEndian(out, numElements, isLarge ? Integer.BYTES : 1);
        writeOffsetTable(out, values, offsetSize);
        for (byte[] value : values) {
            out.writeBytes(value);
        }
    }

    private void writeOffsetTable(ByteArrayOutputStream out, List<byte[]> values, int offsetSize) {
        int runningOffset = 0;
        writeLittleEndian(out, runningOffset, offsetSize);
        for (byte[] value : values) {
            runningOffset += value.length;
            writeLittleEndian(out, runningOffset, offsetSize);
        }
    }

    private int maxValue(List<Integer> values) {
        int max = 0;
        for (int value : values) {
            max = Math.max(max, value);
        }
        return max;
    }

    private int header(int basicType, int valueHeader) {
        return VariantBytes.header(basicType, valueHeader);
    }

    private byte[] decimalPayload(int scale, BigInteger unscaled, int unscaledBytes) {
        byte[] payload = new byte[1 + unscaledBytes];
        payload[0] = (byte) scale;
        byte[] littleEndian = VariantBytes.twosComplementLittleEndian(unscaled, unscaledBytes);
        System.arraycopy(littleEndian, 0, payload, 1, unscaledBytes);
        return payload;
    }

    private byte[] littleEndian(long value, int width) {
        byte[] bytes = new byte[width];
        for (int i = 0; i < width; i++) {
            bytes[i] = (byte) ((value >>> (8 * i)) & 0xFF);
        }
        return bytes;
    }

    private byte[] lengthPrefixed(byte[] bytes) {
        byte[] prefixed = new byte[Integer.BYTES + bytes.length];
        byte[] length = littleEndian(bytes.length & 0xFFFFFFFFL, Integer.BYTES);
        System.arraycopy(length, 0, prefixed, 0, Integer.BYTES);
        System.arraycopy(bytes, 0, prefixed, Integer.BYTES, bytes.length);
        return prefixed;
    }

    private void writeLittleEndian(ByteArrayOutputStream out, int value, int width) {
        for (int i = 0; i < width; i++) {
            out.write((value >>> (8 * i)) & 0xFF);
        }
    }

    private int minimumWidth(int maxValue) {
        if (maxValue <= 0xFF) {
            return 1;
        }
        if (maxValue <= 0xFFFF) {
            return 2;
        }
        if (maxValue <= 0xFFFFFF) {
            return 3;
        }
        return 4;
    }

    private MemorySegment readOnlySegment(byte[] bytes) {
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private sealed interface Node permits PrimitiveNode, ShortStringNode, ObjectNode, ArrayNode, RawValueNode {}

    private interface Container {}

    // Transient build-tree holders, never compared by value; array-content equals/hashCode would be dead code.
    @SuppressWarnings("java:S6218")
    private record PrimitiveNode(int typeId, byte[] payload) implements Node {}

    @SuppressWarnings("java:S6218")
    private record ShortStringNode(byte[] utf8) implements Node {}

    @SuppressWarnings("java:S6218")
    private record RawValueNode(byte[] bytes) implements Node {}

    private static final class ObjectNode implements Node, Container {
        private final Map<String, Node> fields = new LinkedHashMap<>();
    }

    private static final class ArrayNode implements Node, Container {
        private final List<Node> elements = new ArrayList<>();
    }
}
