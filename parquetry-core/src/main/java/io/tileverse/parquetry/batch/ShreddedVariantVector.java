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
package io.tileverse.parquetry.batch;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.data.variant.ShreddedVariant;
import io.tileverse.parquetry.data.variant.ShreddedVariantReconstructor;
import io.tileverse.parquetry.data.variant.ShreddedVariantReconstructor.NodeReader;
import io.tileverse.parquetry.data.variant.Variant;
import io.tileverse.parquetry.data.variant.VariantMetadata;

import lombok.NonNull;

/**
 * A column vector for shredded Parquet Variant columns. Each row's typed_value subtree decodes into a tree of leaf
 * vectors at read time; {@link #get(int)} reconstructs that row's canonical unshredded value bytes through a
 * {@link ShreddedVariantReconstructor} and wraps them in a {@link Variant} navigator. Reconstruction is per-row lazy
 * and cached: the first read of a row reconstructs and caches its value bytes, later reads reuse the cached segment.
 *
 * @see VariantVector
 */
public final class ShreddedVariantVector implements ColumnVector {

    private final BinaryVector metadataColumn;
    private final ShreddedVariant model;
    private final VariantInput root;
    private final Validity validity;
    private final int size;
    private final MemorySegment[] cache;

    public ShreddedVariantVector(
            @NonNull BinaryVector metadataColumn,
            @NonNull ShreddedVariant model,
            @NonNull VariantInput root,
            @NonNull Validity validity,
            int size) {
        this.metadataColumn = metadataColumn;
        this.model = model;
        this.root = root;
        this.validity = validity;
        this.size = size;
        this.cache = new MemorySegment[size];
    }

    /** One shredded node's assembled inputs: an optional unshredded value leaf and an optional typed representation. */
    public record VariantInput(BinaryVector value, TypedInput typed) {}

    /** A shredded node's typed_value representation: a scalar leaf, a nested object group, or an array. */
    public sealed interface TypedInput permits ScalarInput, ObjectInput, ArrayInput {}

    /** A scalar typed_value: one primitive leaf vector whose row maps to a Variant primitive type. */
    public record ScalarInput(ColumnVector vector) implements TypedInput {}

    /**
     * An object typed_value group: a per-row presence mask plus the shredded field inputs. A null group at a row (the
     * presence bit clear) means there are no shredded fields there, which the reconstructor distinguishes from a
     * present-but-empty group.
     */
    public record ObjectInput(Validity presence, Map<String, VariantInput> fields) implements TypedInput {}

    /**
     * An array typed_value: per-row {@code offsets} of length {@code size + 1} into the element-aligned {@code element}
     * input, plus the array group's per-row {@code presence}. A null group at a row (the presence bit clear) means the
     * array is null/absent, which the reconstructor distinguishes from a present-but-empty array. Array elements are
     * never omitted; each slot in {@code [offsets[row], offsets[row + 1])} maps to one element of {@code element}.
     */
    public record ArrayInput(int[] offsets, Validity presence, VariantInput element) implements TypedInput {}

    @Override
    public int size() {
        return size;
    }

    @Override
    public Validity validity() {
        return validity;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Variant get(int row) {
        if (validity.isNull(row)) {
            return null;
        }
        VariantMetadata metadata = new VariantMetadata(metadataColumn.get(row));
        MemorySegment value = cache[row];
        if (value == null) {
            value = reconstruct(metadata, row);
            cache[row] = value;
        }
        return Variant.of(value, metadata);
    }

    private MemorySegment reconstruct(VariantMetadata metadata, int row) {
        ShreddedVariantReconstructor reconstructor =
                new ShreddedVariantReconstructor(metadata, ShreddedVariantReconstructor.Leniency.STRICT);
        return reconstructor.reconstruct(model, readerAt(root, row));
    }

    private NodeReader readerAt(VariantInput input, int row) {
        return new InputNodeReader(input, row);
    }

    @Override
    public long approximateHeapBytes() {
        return validity.heapBytes() + metadataColumn.approximateHeapBytes() + inputHeapBytes(root);
    }

    private long inputHeapBytes(VariantInput input) {
        long total = 0L;
        if (input.value() != null) {
            total += input.value().approximateHeapBytes();
        }
        total += typedHeapBytes(input.typed());
        return total;
    }

    private long typedHeapBytes(TypedInput typed) {
        return switch (typed) {
            case null -> 0L;
            case ScalarInput scalar -> scalar.vector().approximateHeapBytes();
            case ObjectInput object -> objectHeapBytes(object);
            case ArrayInput array -> arrayHeapBytes(array);
        };
    }

    private long objectHeapBytes(ObjectInput object) {
        long total = object.presence().heapBytes();
        for (VariantInput field : object.fields().values()) {
            total += inputHeapBytes(field);
        }
        return total;
    }

    private long arrayHeapBytes(ArrayInput array) {
        long offsetBytes = (long) array.offsets().length * Integer.BYTES;
        return offsetBytes + array.presence().heapBytes() + inputHeapBytes(array.element());
    }

    /** A {@link NodeReader} over one shredded node's assembled inputs at a single row. */
    private final class InputNodeReader implements NodeReader {

        private final VariantInput input;
        private final int row;

        private InputNodeReader(VariantInput input, int row) {
            this.input = input;
            this.row = row;
        }

        @Override
        public MemorySegment value() {
            return input.value() == null ? null : input.value().get(row);
        }

        @Override
        public boolean hasTypedScalar() {
            return input.typed() instanceof ScalarInput scalar
                    && !scalar.vector().isNull(row);
        }

        @Override
        public Object typedScalar() {
            return ((ScalarInput) input.typed()).vector().get(row);
        }

        @Override
        public boolean hasTypedObject() {
            return input.typed() instanceof ObjectInput object
                    && !object.presence().isNull(row);
        }

        @Override
        public Map<String, NodeReader> typedObjectFields() {
            if (!(input.typed() instanceof ObjectInput object)
                    || object.presence().isNull(row)) {
                return null;
            }
            Map<String, NodeReader> fieldReaders =
                    new LinkedHashMap<>(object.fields().size());
            for (Map.Entry<String, VariantInput> field : object.fields().entrySet()) {
                fieldReaders.put(field.getKey(), readerAt(field.getValue(), row));
            }
            return fieldReaders;
        }

        @Override
        public List<NodeReader> typedArrayElements() {
            if (!(input.typed() instanceof ArrayInput array) || array.presence().isNull(row)) {
                return null;
            }
            int start = array.offsets()[row];
            int end = array.offsets()[row + 1];
            List<NodeReader> elementReaders = new ArrayList<>(end - start);
            for (int element = start; element < end; element++) {
                elementReaders.add(readerAt(array.element(), element));
            }
            return elementReaders;
        }
    }
}
