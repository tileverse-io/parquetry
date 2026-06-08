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
package io.tileverse.parquetry.internal.batch;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SequencedMap;
import java.util.Set;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.Int96Vector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.MapVector;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.batch.VariantVector;
import io.tileverse.parquetry.internal.arrow.buffer.ArrowBuffers;
import io.tileverse.parquetry.internal.arrow.buffer.EncodedBuffer;
import io.tileverse.parquetry.internal.arrow.buffer.EncodedBuffer.BufferRole;
import io.tileverse.parquetry.internal.arrow.buffer.EncodedNode;
import io.tileverse.parquetry.internal.arrow.buffer.LeafType;
import io.tileverse.parquetry.internal.arrow.buffer.NodeEncoding;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Converts a {@link ColumnVector} to and from its Arrow buffer layout. Fixed-width, boolean, fixed-length binary, and
 * {@code Int96} leaves encode to a validity bitmap plus a data buffer; variable-length binary adds an {@code int32}
 * offsets buffer between them (validity, offsets, data). The encoded form is the byte-stable shape a column spills to
 * disk and reloads from.
 *
 * <p>Every emitted buffer is padded to an 8-byte boundary, as Arrow's buffer layout requires. This handles the
 * fixed-width and boolean leaves and the binary family (variable-length binary, fixed-length binary, and {@code Int96})
 * in both consolidated and dictionary modes. A dictionary-mode vector keeps its dictionary encoding on the wire: the
 * node holds {@code [validity][int32 indices]} and a nested dictionary node holds the distinct values, which avoids
 * materializing low-cardinality columns to their full consolidated form.
 *
 * <p>Nested kinds encode recursively. A list holds {@code [validity][int32 offsets]} and one child node for its element
 * vector; a struct holds {@code [validity]} and one child node per field ordered by field path; a map follows Arrow's
 * {@code List<Struct<key, value>>} shape; a Variant holds {@code [validity]} and the metadata and value binary
 * children. Decoding a nested node needs the recursive {@link ColumnType} descriptor rather than a flat leaf type,
 * because the child shape is not recoverable from the buffers alone.
 *
 * <p>The per-type buffer layouts (validity bitmap, offsets, fixed-size binary, dictionary encoding, and the nested
 * list/struct/map shapes) follow the Apache Arrow columnar format specification.
 *
 * @see <a href="https://arrow.apache.org/docs/format/Columnar.html">Apache Arrow columnar format specification</a>
 */
final class ArrowBufferCodec {

    private static final int VALIDITY_BUFFER = 0;
    private static final int DATA_BUFFER = 1;
    private static final int DICTIONARY_INDICES_BUFFER = 1;

    private static final int BINARY_OFFSETS_BUFFER = 1;
    private static final int BINARY_DATA_BUFFER = 2;

    private static final int LIST_OFFSETS_BUFFER = 1;
    private static final int LIST_CHILD = 0;
    private static final int MAP_ENTRY_STRUCT = 0;
    private static final int MAP_KEY_CHILD = 0;
    private static final int MAP_VALUE_CHILD = 1;
    private static final int VARIANT_METADATA_CHILD = 0;
    private static final int VARIANT_VALUE_CHILD = 1;

    private static final int INT32_WIDTH = Integer.BYTES;
    private static final int INT64_WIDTH = Long.BYTES;
    private static final int FLOAT_WIDTH = Float.BYTES;
    private static final int DOUBLE_WIDTH = Double.BYTES;
    private static final int BOOLEAN_WIDTH = 1;
    private static final int INT96_WIDTH = 12;
    private static final int VARIABLE_WIDTH = -1;

    private ArrowBufferCodec() {
        // utility
    }

    /** Encodes {@code vector} into an Arrow field node with a validity buffer and a packed data buffer. */
    static EncodedNode encode(ColumnVector vector) {
        return switch (vector) {
            case IntVector intVector -> fixedWidthNode(intVector, ArrowBuffers.encodeInts(intVector), INT32_WIDTH);
            case LongVector longVector -> fixedWidthNode(longVector, ArrowBuffers.encodeLongs(longVector), INT64_WIDTH);
            case FloatVector floatVector ->
                fixedWidthNode(floatVector, ArrowBuffers.encodeFloats(floatVector), FLOAT_WIDTH);
            case DoubleVector doubleVector ->
                fixedWidthNode(doubleVector, ArrowBuffers.encodeDoubles(doubleVector), DOUBLE_WIDTH);
            case BooleanVector booleanVector ->
                fixedWidthNode(booleanVector, ArrowBuffers.encodeBooleanBitmap(booleanVector), BOOLEAN_WIDTH);
            case BinaryVector binaryVector -> encodeBinary(binaryVector);
            case FixedLenBinaryVector fixedVector -> encodeFixedSizeBinary(fixedVector);
            case Int96Vector int96Vector -> encodeInt96(int96Vector);
            case ListVector listVector -> encodeList(listVector);
            case StructVector structVector -> encodeStruct(structVector);
            case MapVector mapVector -> encodeMap(mapVector);
            case VariantVector variantVector -> encodeVariant(variantVector);
        };
    }

    /** Rebuilds the {@code type} column vector from {@code node}'s validity and data buffers. */
    static ColumnVector decode(EncodedNode node, LeafType type) {
        Validity validity = decodeValidity(node);
        int length = node.length();
        return switch (type) {
            case INT32 -> IntVector.materialized(ArrowBuffers.decodeInts(dataBuffer(node), length), validity);
            case INT64 -> LongVector.materialized(ArrowBuffers.decodeLongs(dataBuffer(node), length), validity);
            case FLOAT -> FloatVector.materialized(ArrowBuffers.decodeFloats(dataBuffer(node), length), validity);
            case DOUBLE -> DoubleVector.materialized(ArrowBuffers.decodeDoubles(dataBuffer(node), length), validity);
            case BOOLEAN ->
                BooleanVector.materialized(ArrowBuffers.decodeBooleanBitmap(dataBuffer(node), length), validity);
            case BINARY -> decodeBinaryKind(node, validity, length);
            case FIXED_LEN_BINARY -> decodeFixedSizeBinaryKind(node, validity, length);
            case INT96 -> decodeInt96Kind(node, validity, length);
        };
    }

    /**
     * Rebuilds a column vector of any shape from {@code node}, following the recursive {@code type} descriptor into the
     * node's children. Primitive types delegate to the leaf {@link #decode(EncodedNode, LeafType)} path; nested types
     * recurse one level at a time.
     */
    static ColumnVector decode(EncodedNode node, ColumnType type) {
        return switch (type) {
            case ColumnType.Primitive(LeafType leaf) -> decode(node, leaf);
            case ColumnType.ListType(ColumnType element) -> decodeList(node, element);
            case ColumnType.StructType(SequencedMap<ColumnPath, ColumnType> fields) -> decodeStruct(node, fields);
            case ColumnType.MapType(ColumnType key, ColumnType value) -> decodeMap(node, key, value);
            case ColumnType.VariantType _ -> decodeVariant(node);
        };
    }

    private static EncodedNode encodeBinary(BinaryVector vector) {
        if (vector.isDictionary()) {
            EncodedNode dictionary = binaryDictionaryNode(vector.dictionaryEntries());
            return dictionaryNode(vector, dictionary, VARIABLE_WIDTH);
        }
        return consolidatedBinaryNode(vector.validity(), vector.consolidatedBacking(), vector.consolidatedOffsets());
    }

    /**
     * Encodes a consolidated binary vector as Arrow Binary: validity, an {@code int32} offsets buffer of {@code size+1}
     * entries starting at zero, and the packed data bytes.
     *
     * <p>The offsets index absolutely into the shared backing window and need not start at zero, while Arrow Binary
     * offsets must start at zero. The per-row lengths {@code offsets[i+1] - offsets[i]} are base-independent, and
     * {@link ArrowBuffers#encodeOffsets} rebuilds a zero-based offsets buffer from those lengths. The data buffer is
     * the window {@code [base, end)} of the backing, where {@code base = offsets[0]} and {@code end = offsets[size]}.
     */
    private static EncodedNode consolidatedBinaryNode(Validity validity, MemorySegment backing, int[] offsets) {
        int size = offsets.length - 1;
        int base = offsets[0];
        int end = offsets[size];

        int[] rowLengths = new int[size];
        for (int row = 0; row < size; row++) {
            rowLengths[row] = offsets[row + 1] - offsets[row];
        }

        MemorySegment window = backing.asSlice(base, (long) end - base);
        EncodedBuffer validityBuffer = new EncodedBuffer(BufferRole.VALIDITY, ArrowBuffers.encodeValidity(validity));
        EncodedBuffer offsetsBuffer = new EncodedBuffer(BufferRole.OFFSETS, ArrowBuffers.encodeOffsets(rowLengths));
        EncodedBuffer dataBuffer = new EncodedBuffer(BufferRole.DATA, ArrowBuffers.paddedCopy(window));
        return new EncodedNode(
                size,
                validity.nullCount(),
                List.of(validityBuffer, offsetsBuffer, dataBuffer),
                List.of(),
                new NodeEncoding.Plain());
    }

    private static EncodedNode encodeFixedSizeBinary(FixedLenBinaryVector vector) {
        int byteWidth = vector.byteWidth();
        if (vector.isDictionary()) {
            EncodedNode dictionary = fixedSizeBinaryDictionaryNode(vector.dictionaryEntries(), byteWidth);
            return dictionaryNode(vector, dictionary, byteWidth);
        }
        return consolidatedFixedSizeBinaryNode(
                vector.validity(), vector.consolidatedBacking(), byteWidth, vector.size());
    }

    private static EncodedNode encodeInt96(Int96Vector vector) {
        if (vector.isDictionary()) {
            EncodedNode dictionary = fixedSizeBinaryDictionaryNode(vector.dictionaryEntries(), INT96_WIDTH);
            return dictionaryNode(vector, dictionary, INT96_WIDTH);
        }
        return consolidatedFixedSizeBinaryNode(
                vector.validity(), vector.consolidatedBacking(), INT96_WIDTH, vector.size());
    }

    /**
     * Encodes a list as Arrow List: {@code [validity][int32 offsets]} plus one child node for the element vector. The
     * offsets are stored verbatim (not re-based) and index into the full, unsliced element vector exactly as the source
     * {@link ListVector} held them, which lets the decoder rebuild the same offsets and child without re-aligning.
     */
    private static EncodedNode encodeList(ListVector vector) {
        Validity validity = vector.validity();
        EncodedBuffer validityBuffer = new EncodedBuffer(BufferRole.VALIDITY, ArrowBuffers.encodeValidity(validity));
        requireZeroBasedOffsets(vector.offsets(), "List");
        EncodedBuffer offsetsBuffer = new EncodedBuffer(BufferRole.OFFSETS, ArrowBuffers.encodeInts(vector.offsets()));
        EncodedNode child = encode(vector.child());
        return new EncodedNode(
                vector.size(),
                validity.nullCount(),
                List.of(validityBuffer, offsetsBuffer),
                List.of(child),
                new NodeEncoding.Plain());
    }

    /**
     * Encodes a struct as Arrow Struct: a {@code [validity]} buffer and one child node per field.
     * {@link StructVector}'s children map does not preserve insertion order, and the encoded node has no field names,
     * so encode and decode both order the fields by their {@link ColumnPath}. Sorting on both sides pins the child node
     * positions to the same sequence the decoder walks, which keeps each field paired with its own node.
     */
    private static EncodedNode encodeStruct(StructVector vector) {
        Validity validity = vector.validity();
        EncodedBuffer validityBuffer = new EncodedBuffer(BufferRole.VALIDITY, ArrowBuffers.encodeValidity(validity));
        List<ColumnPath> orderedFields = sortedFieldKeys(vector.children().keySet());
        List<EncodedNode> children = new ArrayList<>(orderedFields.size());
        for (ColumnPath field : orderedFields) {
            children.add(encode(vector.children().get(field)));
        }
        return new EncodedNode(
                vector.size(),
                validity.nullCount(),
                List.of(validityBuffer),
                List.copyOf(children),
                new NodeEncoding.Plain());
    }

    /** Returns the field keys in a stable order that encode and decode both follow to pin child node positions. */
    private static List<ColumnPath> sortedFieldKeys(Set<ColumnPath> keys) {
        List<ColumnPath> ordered = new ArrayList<>(keys);
        ordered.sort(Comparator.comparing(ColumnPath::dot));
        return ordered;
    }

    /**
     * Encodes a map as Arrow Map, which is a {@code List<Struct<key, value>>}: a {@code [validity][int32 offsets]} list
     * node whose single child is an all-valid struct entry node holding the flattened key and value vectors. The
     * key/value vectors are parallel; the struct length is the total entry count {@code keys.size()}.
     */
    private static EncodedNode encodeMap(MapVector vector) {
        Validity validity = vector.validity();
        EncodedBuffer validityBuffer = new EncodedBuffer(BufferRole.VALIDITY, ArrowBuffers.encodeValidity(validity));
        requireZeroBasedOffsets(vector.offsets(), "Map");
        EncodedBuffer offsetsBuffer = new EncodedBuffer(BufferRole.OFFSETS, ArrowBuffers.encodeInts(vector.offsets()));
        EncodedNode entries = mapEntryStructNode(vector.keys(), vector.values());
        return new EncodedNode(
                vector.size(),
                validity.nullCount(),
                List.of(validityBuffer, offsetsBuffer),
                List.of(entries),
                new NodeEncoding.Plain());
    }

    /**
     * Arrow list and map offsets index into the unsliced child starting at zero. The read path always emits zero-based
     * offsets; a hand-built vector that does not is rejected here rather than producing a non-Arrow-compliant buffer.
     */
    private static void requireZeroBasedOffsets(int[] offsets, String kind) {
        if (offsets.length > 0 && offsets[0] != 0) {
            throw new IllegalArgumentException(
                    kind + " offsets must be zero-based for Arrow encoding, but the first offset is " + offsets[0]);
        }
    }

    /** Builds the all-valid {@code Struct<key, value>} entry node a map's list child wraps. */
    private static EncodedNode mapEntryStructNode(ColumnVector keys, ColumnVector values) {
        int entryCount = keys.size();
        Validity allValid = Validity.allValid(entryCount);
        EncodedBuffer validityBuffer = new EncodedBuffer(BufferRole.VALIDITY, ArrowBuffers.encodeValidity(allValid));
        EncodedNode keyNode = encode(keys);
        EncodedNode valueNode = encode(values);
        return new EncodedNode(
                entryCount, 0, List.of(validityBuffer), List.of(keyNode, valueNode), new NodeEncoding.Plain());
    }

    /**
     * Encodes a Variant as a {@code [validity]} node with the metadata and value binary leaves as its two children. The
     * decoder reads both children as binary and pairs them back into a {@link VariantVector}.
     */
    private static EncodedNode encodeVariant(VariantVector vector) {
        Validity validity = vector.validity();
        EncodedBuffer validityBuffer = new EncodedBuffer(BufferRole.VALIDITY, ArrowBuffers.encodeValidity(validity));
        EncodedNode metadata = encode(vector.metadataColumn());
        EncodedNode value = encode(vector.valueColumn());
        return new EncodedNode(
                vector.size(),
                validity.nullCount(),
                List.of(validityBuffer),
                List.of(metadata, value),
                new NodeEncoding.Plain());
    }

    private static ListVector decodeList(EncodedNode node, ColumnType elementType) {
        Validity validity = decodeValidity(node);
        int size = node.length();
        int[] offsets =
                ArrowBuffers.decodeInts(node.buffers().get(LIST_OFFSETS_BUFFER).bytes(), size + 1);
        ColumnVector child = decode(node.children().get(LIST_CHILD), elementType);
        return new ListVector(offsets, child, validity, size);
    }

    private static StructVector decodeStruct(EncodedNode node, SequencedMap<ColumnPath, ColumnType> fields) {
        Validity validity = decodeValidity(node);
        int size = node.length();
        List<ColumnPath> orderedFields = sortedFieldKeys(fields.keySet());
        SequencedMap<ColumnPath, ColumnVector> children = new LinkedHashMap<>();
        for (int childIndex = 0; childIndex < orderedFields.size(); childIndex++) {
            ColumnPath field = orderedFields.get(childIndex);
            ColumnVector child = decode(node.children().get(childIndex), fields.get(field));
            children.put(field, child);
        }
        return new StructVector(children, validity, size);
    }

    private static MapVector decodeMap(EncodedNode node, ColumnType keyType, ColumnType valueType) {
        Validity validity = decodeValidity(node);
        int size = node.length();
        int[] offsets =
                ArrowBuffers.decodeInts(node.buffers().get(LIST_OFFSETS_BUFFER).bytes(), size + 1);
        EncodedNode entries = node.children().get(MAP_ENTRY_STRUCT);
        ColumnVector keys = decode(entries.children().get(MAP_KEY_CHILD), keyType);
        ColumnVector values = decode(entries.children().get(MAP_VALUE_CHILD), valueType);
        return new MapVector(offsets, keys, values, validity, size);
    }

    private static VariantVector decodeVariant(EncodedNode node) {
        Validity validity = decodeValidity(node);
        int size = node.length();
        BinaryVector metadata = (BinaryVector) decode(node.children().get(VARIANT_METADATA_CHILD), LeafType.BINARY);
        BinaryVector value = (BinaryVector) decode(node.children().get(VARIANT_VALUE_CHILD), LeafType.BINARY);
        return new VariantVector(metadata, value, validity, size);
    }

    /**
     * Encodes a consolidated fixed-stride binary vector (FixedSizeBinary or {@code Int96}) as validity plus a packed
     * data buffer of {@code size * byteWidth} bytes, with {@code byteWidth} recorded in the node encoding.
     */
    private static EncodedNode consolidatedFixedSizeBinaryNode(
            Validity validity, MemorySegment backing, int byteWidth, int size) {
        EncodedBuffer validityBuffer = new EncodedBuffer(BufferRole.VALIDITY, ArrowBuffers.encodeValidity(validity));
        EncodedBuffer dataBuffer = new EncodedBuffer(BufferRole.DATA, ArrowBuffers.paddedCopy(backing));
        return new EncodedNode(
                size,
                validity.nullCount(),
                List.of(validityBuffer, dataBuffer),
                List.of(),
                new NodeEncoding.FixedWidth(byteWidth));
    }

    /**
     * Builds the index node of a dictionary-mode vector: {@code [validity][int32 indices]} with the distinct values
     * held in a nested {@code dictionary} node. Keeping the indices and a single copy of the values avoids expanding a
     * low-cardinality column to its full consolidated form.
     */
    private static EncodedNode dictionaryNode(ColumnVector vector, EncodedNode dictionary, int byteWidth) {
        Validity validity = vector.validity();
        int[] indices = dictionaryIndices(vector);
        EncodedBuffer validityBuffer = new EncodedBuffer(BufferRole.VALIDITY, ArrowBuffers.encodeValidity(validity));
        EncodedBuffer indicesBuffer =
                new EncodedBuffer(BufferRole.DICTIONARY_INDICES, ArrowBuffers.encodeInts(indices));
        return new EncodedNode(
                vector.size(),
                validity.nullCount(),
                List.of(validityBuffer, indicesBuffer),
                List.of(),
                new NodeEncoding.Dictionary(dictionary, byteWidth));
    }

    private static int[] dictionaryIndices(ColumnVector vector) {
        return switch (vector) {
            case BinaryVector binaryVector -> binaryVector.dictionaryIndices();
            case FixedLenBinaryVector fixedVector -> fixedVector.dictionaryIndices();
            case Int96Vector int96Vector -> int96Vector.dictionaryIndices();
            default -> throw unsupported(vector);
        };
    }

    /**
     * Encodes the distinct values of a binary dictionary as an all-valid consolidated Arrow Binary node. The entries
     * have no nulls, which makes consolidating them into a single backing and offsets buffer straightforward.
     */
    private static EncodedNode binaryDictionaryNode(MemorySegment[] entries) {
        BinaryVector.VariableLayout layout = BinaryVector.consolidate(entries);
        return consolidatedBinaryNode(Validity.allValid(entries.length), layout.backing(), layout.offsets());
    }

    /** Encodes the distinct values of a fixed-stride dictionary as an all-valid consolidated FixedSizeBinary node. */
    private static EncodedNode fixedSizeBinaryDictionaryNode(MemorySegment[] entries, int byteWidth) {
        MemorySegment backing = FixedLenBinaryVector.packFullSlots(entries, byteWidth);
        return consolidatedFixedSizeBinaryNode(Validity.allValid(entries.length), backing, byteWidth, entries.length);
    }

    private static BinaryVector decodeBinaryKind(EncodedNode node, Validity validity, int length) {
        if (node.encoding() instanceof NodeEncoding.Dictionary(EncodedNode dictionaryNode, int _)) {
            MemorySegment[] entries = decodeBinaryDictionaryEntries(dictionaryNode);
            int[] indices = decodeIndices(node, length);
            return BinaryVector.dictionary(entries, indices, validity);
        }
        return decodeConsolidatedBinary(node, validity, length);
    }

    private static BinaryVector decodeConsolidatedBinary(EncodedNode node, Validity validity, int length) {
        MemorySegment offsetsBytes = node.buffers().get(BINARY_OFFSETS_BUFFER).bytes();
        int[] offsets = ArrowBuffers.decodeOffsets(offsetsBytes, length);
        MemorySegment data = ownedCopy(node.buffers().get(BINARY_DATA_BUFFER).bytes());
        return BinaryVector.of(data, offsets, validity);
    }

    private static FixedLenBinaryVector decodeFixedSizeBinaryKind(EncodedNode node, Validity validity, int length) {
        if (node.encoding() instanceof NodeEncoding.Dictionary(EncodedNode dictionaryNode, int byteWidth)) {
            MemorySegment[] entries = decodeFixedSizeBinaryDictionaryEntries(dictionaryNode, byteWidth);
            int[] indices = decodeIndices(node, length);
            return FixedLenBinaryVector.dictionary(entries, indices, byteWidth, validity);
        }
        int byteWidth = ((NodeEncoding.FixedWidth) node.encoding()).byteWidth();
        MemorySegment rows = fixedStrideRows(node, byteWidth);
        return FixedLenBinaryVector.of(rows, byteWidth, validity);
    }

    private static Int96Vector decodeInt96Kind(EncodedNode node, Validity validity, int length) {
        if (node.encoding() instanceof NodeEncoding.Dictionary(EncodedNode dictionaryNode, int _)) {
            MemorySegment[] entries = decodeFixedSizeBinaryDictionaryEntries(dictionaryNode, INT96_WIDTH);
            int[] indices = decodeIndices(node, length);
            return Int96Vector.dictionary(entries, indices, validity);
        }
        return Int96Vector.of(fixedStrideRows(node, INT96_WIDTH), validity);
    }

    /** Reads the {@code length} per-row dictionary indices from a dictionary node's int32 indices buffer. */
    private static int[] decodeIndices(EncodedNode node, int length) {
        MemorySegment indicesBytes =
                node.buffers().get(DICTIONARY_INDICES_BUFFER).bytes();
        return ArrowBuffers.decodeInts(indicesBytes, length);
    }

    /**
     * Rebuilds the distinct values of a binary dictionary by decoding its all-valid consolidated node and pulling each
     * row out as an owned read-only segment.
     */
    private static MemorySegment[] decodeBinaryDictionaryEntries(EncodedNode dictionary) {
        int count = dictionary.length();
        MemorySegment offsetsBytes =
                dictionary.buffers().get(BINARY_OFFSETS_BUFFER).bytes();
        int[] offsets = ArrowBuffers.decodeOffsets(offsetsBytes, count);
        MemorySegment data = dictionary.buffers().get(BINARY_DATA_BUFFER).bytes();
        MemorySegment[] entries = new MemorySegment[count];
        for (int i = 0; i < count; i++) {
            int start = offsets[i];
            int entryLength = offsets[i + 1] - start;
            entries[i] = ownedCopy(data.asSlice(start, entryLength));
        }
        return entries;
    }

    /**
     * Rebuilds the distinct values of a fixed-stride dictionary, each entry an owned read-only {@code byteWidth} slot.
     */
    private static MemorySegment[] decodeFixedSizeBinaryDictionaryEntries(EncodedNode dictionary, int byteWidth) {
        int count = dictionary.length();
        MemorySegment data = dataBuffer(dictionary);
        MemorySegment[] entries = new MemorySegment[count];
        for (int i = 0; i < count; i++) {
            entries[i] = ownedCopy(data.asSlice((long) i * byteWidth, byteWidth));
        }
        return entries;
    }

    /**
     * Copies exactly {@code node.length()} rows of {@code byteWidth} bytes from the (8-byte padded) data buffer into an
     * owned heap segment. Trailing alignment pad is left behind, which keeps the fixed-stride factory's {@code byteSize
     * / byteWidth} row count equal to Arrow's field node length rather than over-reporting it.
     */
    private static MemorySegment fixedStrideRows(EncodedNode node, int byteWidth) {
        long rowBytes = (long) node.length() * byteWidth;
        MemorySegment rows = dataBuffer(node).asSlice(0, rowBytes);
        return ownedCopy(rows);
    }

    /** Copies {@code source} into a freshly allocated read-only heap segment the rebuilt vector can own outright. */
    private static MemorySegment ownedCopy(MemorySegment source) {
        byte[] bytes = source.toArray(ValueLayout.JAVA_BYTE);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static EncodedNode fixedWidthNode(ColumnVector vector, MemorySegment data, int byteWidth) {
        Validity validity = vector.validity();
        EncodedBuffer validityBuffer = new EncodedBuffer(BufferRole.VALIDITY, ArrowBuffers.encodeValidity(validity));
        EncodedBuffer dataBuffer = new EncodedBuffer(BufferRole.DATA, data);
        return new EncodedNode(
                vector.size(),
                validity.nullCount(),
                List.of(validityBuffer, dataBuffer),
                List.of(),
                new NodeEncoding.FixedWidth(byteWidth));
    }

    private static Validity decodeValidity(EncodedNode node) {
        MemorySegment validityBytes = node.buffers().get(VALIDITY_BUFFER).bytes();
        return ArrowBuffers.decodeValidity(validityBytes, node.nullCount(), node.length());
    }

    private static MemorySegment dataBuffer(EncodedNode node) {
        return node.buffers().get(DATA_BUFFER).bytes();
    }

    private static UnsupportedOperationException unsupported(ColumnVector vector) {
        return new UnsupportedOperationException(
                "ArrowBufferCodec does not yet handle " + vector.getClass().getSimpleName());
    }
}
