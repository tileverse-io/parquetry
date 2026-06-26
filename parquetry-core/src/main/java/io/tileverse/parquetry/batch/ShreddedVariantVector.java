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
import io.tileverse.parquetry.schema.ColumnPath;

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
    private final Selection selection;
    private StructVector structView;

    public ShreddedVariantVector(
            @NonNull BinaryVector metadataColumn,
            @NonNull ShreddedVariant model,
            @NonNull VariantInput root,
            @NonNull Validity validity,
            int size) {
        this(metadataColumn, model, root, validity, size, new MemorySegment[size], Selection.ALL);
    }

    // S107: private; only the public constructor and select() call it
    @SuppressWarnings("java:S107")
    private ShreddedVariantVector(
            @NonNull BinaryVector metadataColumn,
            @NonNull ShreddedVariant model,
            @NonNull VariantInput root,
            @NonNull Validity validity,
            int size,
            MemorySegment[] cache,
            @NonNull Selection selection) {
        this.metadataColumn = metadataColumn;
        this.model = model;
        this.root = root;
        this.validity = validity;
        this.size = size;
        this.cache = cache;
        this.selection = selection;
    }

    /** The {@code metadata} leaf column, one Variant metadata dictionary per row. */
    public BinaryVector metadataColumn() {
        return metadataColumn;
    }

    /** The shredded shape this column's typed subtree was classified into. */
    public ShreddedVariant model() {
        return model;
    }

    /** The root node's assembled inputs: the residual {@code value} leaf and the typed representation. */
    public VariantInput root() {
        return root;
    }

    /**
     * A read-only nested view of this shredded Variant column as the struct-of-leaves the Parquet Variant group schema
     * declares: a {@link StructVector} {@code v} whose children are the {@code metadata} leaf, the optional residual
     * {@code value} leaf, and the typed representation under {@code typed_value} (a scalar leaf, an object struct, or
     * an array list). The view wraps the existing leaf vectors without copying, letting the write-side striper navigate
     * a shredded Variant column with the same struct and list machinery it uses for ordinary nested columns. Children
     * absent from the model (a missing {@code value} or {@code typed_value}) are omitted to match the schema's leaves.
     */
    public StructVector asStructView() {
        requireUnselected();
        // Built once over immutable leaf vectors; single-threaded during striping, no synchronization needed.
        if (structView == null) {
            structView = buildStructView();
        }
        return structView;
    }

    private StructVector buildStructView() {
        Map<ColumnPath, ColumnVector> children = new LinkedHashMap<>();
        children.put(ColumnPath.of("metadata"), metadataColumn);
        putNodeChildren(children, root);
        return new StructVector(children, validity, size);
    }

    private void putNodeChildren(Map<ColumnPath, ColumnVector> children, VariantInput input) {
        if (input.value() != null) {
            children.put(ColumnPath.of("value"), input.value());
        }
        if (input.typed() != null) {
            children.put(ColumnPath.of("typed_value"), typedView(input.typed()));
        }
    }

    private ColumnVector typedView(TypedInput typed) {
        return switch (typed) {
            case ScalarInput(ColumnVector vector) -> vector;
            case ObjectInput object -> objectView(object);
            case ArrayInput array -> arrayView(array);
        };
    }

    private StructVector objectView(ObjectInput object) {
        Map<ColumnPath, ColumnVector> fields = new LinkedHashMap<>();
        for (Map.Entry<String, VariantInput> field : object.fields().entrySet()) {
            fields.put(ColumnPath.of(field.getKey()), fieldGroupView(field.getValue()));
        }
        return new StructVector(fields, object.presence(), size);
    }

    private ListVector arrayView(ArrayInput array) {
        int elementCount = array.offsets()[size];
        StructVector elementView = fieldGroupView(array.element(), elementCount);
        return new ListVector(array.offsets(), elementView, array.presence(), size);
    }

    /**
     * The struct view of one REQUIRED field or element group: its optional {@code value} and {@code typed_value}
     * children, both keyed by name. The group is REQUIRED in the schema and its rows are therefore all valid; per-row
     * absence rides on the OPTIONAL {@code value} / {@code typed_value} leaf validity, not the group validity.
     */
    private StructVector fieldGroupView(VariantInput field) {
        return fieldGroupView(field, size);
    }

    private StructVector fieldGroupView(VariantInput field, int rows) {
        Map<ColumnPath, ColumnVector> children = new LinkedHashMap<>();
        putNodeChildren(children, field);
        return new StructVector(children, Validity.allValid(rows), rows);
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
    public Selection selection() {
        return selection;
    }

    @Override
    public int baseSize() {
        return size;
    }

    @Override
    public Validity validity() {
        return validity;
    }

    /**
     * Reconstructs the Variant at logical row {@code row}, or {@code null} when the row is null. Sequential-access
     * optimized on a selected view: the logical row is translated to its physical row, and reconstruction reuses the
     * physical-indexed metadata column, typed subtree, and per-row cache unchanged.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Variant get(int row) {
        if (validity.isNull(row)) {
            return null;
        }
        int physical = selection == Selection.ALL ? row : selection.physical(row);
        VariantMetadata metadata = new VariantMetadata(metadataColumn.get(physical));
        return Variant.of(cachedValue(metadata, physical), metadata);
    }

    /**
     * A selected shredded Variant keeps its whole physical-indexed reconstruction machinery (metadata column, typed
     * subtree, per-row cache) and only translates logical rows to physical in {@link #get(int)}; the validity is
     * projected and the size reduced to the selection length. The bulk {@link #asStructView()} /
     * {@link #toUnshredded()} forms reflect the whole physical domain and are rebuilt at the export/write boundary, so
     * they reject a selection.
     */
    @Override
    public ColumnVector select(Selection selection) {
        if (selection == Selection.ALL) {
            return this;
        }
        return new ShreddedVariantVector(
                metadataColumn, model, root, validity.select(selection), size, cache, selection);
    }

    private MemorySegment cachedValue(VariantMetadata metadata, int row) {
        MemorySegment value = cache[row];
        if (value == null) {
            value = reconstruct(metadata, row);
            cache[row] = value;
        }
        return value;
    }

    /**
     * Materializes this shredded column into its unshredded {@link VariantVector} form: the same metadata column,
     * paired with each row's canonical reconstructed value packed into one consolidated binary backing. The Arrow
     * export path needs the unshredded form because Arrow models a Variant as {@code struct<metadata, value>}; the
     * shredded representation has no Arrow buffer layout of its own.
     *
     * <p>Reconstruction reuses the per-row {@link #get(int)} path (and its cache), and the values are concatenated with
     * {@link MemorySegment#copy} rather than per-row heap arrays. A null row contributes a zero-length value run and
     * keeps its cleared validity bit.
     */
    public VariantVector toUnshredded() {
        requireUnselected();
        MemorySegment[] values = new MemorySegment[size];
        for (int row = 0; row < size; row++) {
            if (!validity.isNull(row)) {
                VariantMetadata metadata = new VariantMetadata(metadataColumn.get(row));
                values[row] = cachedValue(metadata, row);
            }
        }
        BinaryVector valueColumn = BinaryVector.materialized(values, validity);
        return new VariantVector(metadataColumn, valueColumn, validity, size);
    }

    private MemorySegment reconstruct(VariantMetadata metadata, int row) {
        ShreddedVariantReconstructor reconstructor =
                new ShreddedVariantReconstructor(metadata, ShreddedVariantReconstructor.Leniency.STRICT);
        return reconstructor.reconstruct(model, readerAt(root, row));
    }

    private NodeReader readerAt(VariantInput input, int row) {
        return new InputNodeReader(input, row);
    }

    private void requireUnselected() {
        if (selection != Selection.ALL) {
            throw new UnsupportedOperationException(
                    "the struct view and unshredded form of a selected shredded Variant are rebuilt at the "
                            + "export/write boundary; use get(int) for per-row access");
        }
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
