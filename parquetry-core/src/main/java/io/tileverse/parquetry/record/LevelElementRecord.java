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
package io.tileverse.parquetry.record;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.ElementMeta;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.Int96Vector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.LevelSource;
import io.tileverse.parquetry.batch.ListMeta;
import io.tileverse.parquetry.batch.ListMeta.FieldMeta;
import io.tileverse.parquetry.batch.ListMeta.StructMeta;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.MapMeta;
import io.tileverse.parquetry.data.UuidConverter;
import io.tileverse.parquetry.data.variant.Variant;
import io.tileverse.parquetry.data.variant.VariantMetadata;
import io.tileverse.parquetry.materializer.LeafWindows;
import io.tileverse.parquetry.materializer.LevelListMaterializer;
import io.tileverse.parquetry.materializer.LevelMapMaterializer;
import io.tileverse.parquetry.record.ParquetRecord.BinaryView;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;

/**
 * One struct element (or nested struct field) of a level-backed list, read directly from the descendant leaf vectors at
 * this element's per-leaf positions. The record holds no assembled child vectors: a field read indexes the field's
 * dense leaf at the position the navigation resolved for this element, mirroring {@code DefaultParquetRecord}'s
 * accessor semantics over the level form.
 *
 * <p>The record is a fresh, lazy view bound to one element; each access reads the leaves anew. It is valid only while
 * the owning batch is open, and {@link #detach()} copies every field out for a caller that retains it. Records compare
 * by identity.
 */
public final class LevelElementRecord implements ParquetRecord {

    private static final String READ_BINARY = "readBinary";
    private static final String ABSENT = "absent";

    private final LevelSource vec;
    private final int rowIndex;
    private final StructMeta struct;
    private final LeafWindows windows;

    public LevelElementRecord(LevelSource vec, int rowIndex, StructMeta struct, LeafWindows windows) {
        this.vec = vec;
        this.rowIndex = rowIndex;
        this.struct = struct;
        this.windows = windows;
    }

    /** Reads the Variant value at {@code variant}'s leaves for the element whose windows are {@code windows}. */
    public static Variant readVariant(LevelSource vec, ElementMeta.VariantLeaves variant, LeafWindows windows) {
        if (variant.metadataLeaf() == null || variant.valueLeaf() == null) {
            return null;
        }
        BinaryVector metadataLeaf = (BinaryVector) vec.leaf(variant.metadataLeaf());
        BinaryVector valueLeaf = (BinaryVector) vec.leaf(variant.valueLeaf());
        int metadataPos = windows.start(variant.metadataLeaf());
        int valuePos = windows.start(variant.valueLeaf());
        MemorySegment metadataBytes = metadataLeaf.get(metadataPos);
        MemorySegment valueBytes = valueLeaf.get(valuePos);
        if (metadataBytes == null || valueBytes == null) {
            return null;
        }
        return Variant.of(valueBytes, new VariantMetadata(metadataBytes));
    }

    @Override
    public ParquetSchema schema() {
        return vec.schema();
    }

    @Override
    public int columnCount() {
        return fields().size();
    }

    @Override
    public ColumnPath columnPath(int col) {
        return ColumnPath.of(field(col).name());
    }

    @Override
    public boolean isNull(int col) {
        return isNullField(field(col));
    }

    @Override
    public boolean getBoolean(int col) {
        FieldMeta field = field(col);
        if (field.element() instanceof ElementMeta.Scalar scalar && leafAt(scalar) instanceof BooleanVector vector) {
            return vector.getBoolean(positionOf(scalar));
        }
        throw mismatch(field, "getBoolean");
    }

    @Override
    public int getInt(int col) {
        FieldMeta field = field(col);
        if (field.element() instanceof ElementMeta.Scalar scalar && leafAt(scalar) instanceof IntVector vector) {
            return vector.getInt(positionOf(scalar));
        }
        throw mismatch(field, "getInt");
    }

    @Override
    public long getLong(int col) {
        FieldMeta field = field(col);
        if (field.element() instanceof ElementMeta.Scalar scalar && leafAt(scalar) instanceof LongVector vector) {
            return vector.getLong(positionOf(scalar));
        }
        throw mismatch(field, "getLong");
    }

    @Override
    public float getFloat(int col) {
        FieldMeta field = field(col);
        if (field.element() instanceof ElementMeta.Scalar scalar && leafAt(scalar) instanceof FloatVector vector) {
            return vector.getFloat(positionOf(scalar));
        }
        throw mismatch(field, "getFloat");
    }

    @Override
    public double getDouble(int col) {
        FieldMeta field = field(col);
        if (field.element() instanceof ElementMeta.Scalar scalar && leafAt(scalar) instanceof DoubleVector vector) {
            return vector.getDouble(positionOf(scalar));
        }
        throw mismatch(field, "getDouble");
    }

    @Override
    public String getString(int col) {
        return readBinary(
                col, (backing, offset, length) -> new String(toBytes(backing, offset, length), StandardCharsets.UTF_8));
    }

    @Override
    public byte[] getBinary(int col) {
        return readBinary(col, LevelElementRecord::toBytes);
    }

    @Override
    public UUID getUuid(int col) {
        FieldMeta field = field(col);
        if (field.element() instanceof ElementMeta.Absent) {
            return null;
        }
        if (field.element() instanceof ElementMeta.Scalar scalar
                && leafAt(scalar) instanceof FixedLenBinaryVector fb
                && fb.byteWidth() == UuidConverter.BYTES) {
            MemorySegment slice = fb.get(positionOf(scalar));
            return slice == null ? null : UuidConverter.fromSegment(slice);
        }
        throw mismatch(field, "getUuid");
    }

    @Override
    public <R> R readBinary(int col, BinaryView<R> view) {
        FieldMeta field = field(col);
        if (field.element() instanceof ElementMeta.Absent) {
            return null;
        }
        if (!(field.element() instanceof ElementMeta.Scalar scalar)) {
            throw mismatch(field, READ_BINARY);
        }
        int position = positionOf(scalar);
        return switch (leafAt(scalar)) {
            case BinaryVector bv -> readBinaryValue(bv, position, view);
            case FixedLenBinaryVector fb -> readSlice(fb.get(position), view);
            case Int96Vector iv -> readSlice(iv.get(position), view);
            case null, default -> throw mismatch(field, READ_BINARY);
        };
    }

    @Override
    public ParquetRecord readStruct(int col) {
        FieldMeta field = field(col);
        if (field.element() instanceof ElementMeta.Absent) {
            return null;
        }
        if (!(field.element() instanceof ElementMeta.Struct(StructMeta nested))) {
            throw new ParquetSchemaException("Column " + field.name() + " is not a struct column");
        }
        return structField(nested);
    }

    @Override
    @SuppressWarnings("java:S1168")
    public List<ParquetRecord> readList(int col) {
        FieldMeta field = field(col);
        if (field.element() instanceof ElementMeta.Absent) {
            return null;
        }
        if (!(field.element() instanceof ElementMeta.NestedList(ListMeta list))) {
            throw new ParquetSchemaException("Column " + field.name() + " is not a list column");
        }
        return listField(list);
    }

    @Override
    // S1168: null distinguishes a null map cell from a present empty map
    @SuppressWarnings({"java:S1452", "java:S1168"})
    public Map<?, ?> readMap(int col) {
        FieldMeta field = field(col);
        if (field.element() instanceof ElementMeta.Absent) {
            return null;
        }
        if (!(field.element() instanceof ElementMeta.MapEntry(MapMeta map))) {
            throw new ParquetSchemaException("Column " + field.name() + " is not a map column");
        }
        return mapField(map);
    }

    @Override
    public Object get(int col) {
        FieldMeta field = field(col);
        if (isNullField(field)) {
            return null;
        }
        return switch (field.element()) {
            case ElementMeta.Scalar scalar -> scalarValue(field, scalar);
            case ElementMeta.Struct(StructMeta nested) -> structField(nested);
            case ElementMeta.NestedList(ListMeta list) -> listField(list);
            case ElementMeta.VariantLeaves variant -> readVariant(vec, variant, windows);
            case ElementMeta.MapEntry(MapMeta map) -> mapField(map);
            case ElementMeta.Absent _ -> null;
        };
    }

    @Override
    public boolean isNull(ColumnPath col) {
        Resolved resolved = locate(col);
        return resolved == null || resolved.target().isNull(resolved.index());
    }

    @Override
    public boolean getBoolean(ColumnPath col) {
        Resolved resolved = require(col, "getBoolean");
        return resolved.target().getBoolean(resolved.index());
    }

    @Override
    public int getInt(ColumnPath col) {
        Resolved resolved = require(col, "getInt");
        return resolved.target().getInt(resolved.index());
    }

    @Override
    public long getLong(ColumnPath col) {
        Resolved resolved = require(col, "getLong");
        return resolved.target().getLong(resolved.index());
    }

    @Override
    public float getFloat(ColumnPath col) {
        Resolved resolved = require(col, "getFloat");
        return resolved.target().getFloat(resolved.index());
    }

    @Override
    public double getDouble(ColumnPath col) {
        Resolved resolved = require(col, "getDouble");
        return resolved.target().getDouble(resolved.index());
    }

    @Override
    public String getString(ColumnPath col) {
        Resolved resolved = require(col, "getString");
        return resolved.target().getString(resolved.index());
    }

    @Override
    public byte[] getBinary(ColumnPath col) {
        Resolved resolved = require(col, "getBinary");
        return resolved.target().getBinary(resolved.index());
    }

    @Override
    public UUID getUuid(ColumnPath col) {
        Resolved resolved = require(col, "getUuid");
        return resolved.target().getUuid(resolved.index());
    }

    @Override
    public <R> R readBinary(ColumnPath col, BinaryView<R> view) {
        Resolved resolved = require(col, READ_BINARY);
        return resolved.target().readBinary(resolved.index(), view);
    }

    @Override
    public ParquetRecord readStruct(ColumnPath col) {
        Resolved resolved = locate(col);
        return resolved == null ? null : resolved.target().readStruct(resolved.index());
    }

    @Override
    public List<ParquetRecord> readList(ColumnPath col) {
        Resolved resolved = locate(col);
        return resolved == null ? null : resolved.target().readList(resolved.index());
    }

    @Override
    @SuppressWarnings("java:S1452")
    public Map<?, ?> readMap(ColumnPath col) {
        Resolved resolved = locate(col);
        return resolved == null ? null : resolved.target().readMap(resolved.index());
    }

    @Override
    public Object get(ColumnPath col) {
        Resolved resolved = locate(col);
        return resolved == null ? null : resolved.target().get(resolved.index());
    }

    @Override
    public List<Object> multiValue(ColumnPath leafPath) {
        return MultiValues.flatten(this, leafPath);
    }

    @Override
    public int multiValueSize(ColumnPath leafPath) {
        return MultiValues.flattenSize(this, leafPath);
    }

    @Override
    public ParquetRecord detach() {
        int count = columnCount();
        ColumnPath[] paths = new ColumnPath[count];
        Object[] values = new Object[count];
        for (int i = 0; i < count; i++) {
            paths[i] = columnPath(i);
            values[i] = Detach.detach(get(i));
        }
        return new DetachedParquetRecord(schema(), paths, values);
    }

    // --- field resolution ---

    private List<FieldMeta> fields() {
        return struct.fields();
    }

    private FieldMeta field(int col) {
        return fields().get(col);
    }

    /**
     * Resolves a possibly-dotted path one segment at a time through nested struct fields. Returns {@code null} when the
     * path is absent or an intermediate struct is null, which the lenient accessors read as null and the typed
     * accessors reject through {@link #require}.
     */
    private Resolved locate(ColumnPath col) {
        int index = indexOfField(col.part(0));
        if (index < 0) {
            return null;
        }
        if (col.numParts() == 1) {
            return new Resolved(this, index);
        }
        FieldMeta field = field(index);
        if (!(field.element() instanceof ElementMeta.Struct(StructMeta nestedMeta))) {
            return null;
        }
        LevelElementRecord nested = structField(nestedMeta);
        if (nested == null) {
            return null;
        }
        return nested.locate(tail(col));
    }

    private int indexOfField(String name) {
        List<FieldMeta> fields = fields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private static ColumnPath tail(ColumnPath col) {
        String dotted = col.dot();
        return ColumnPath.parse(dotted.substring(dotted.indexOf('.') + 1));
    }

    private Resolved require(ColumnPath col, String accessor) {
        Resolved resolved = locate(col);
        if (resolved == null) {
            throw new ParquetSchemaException("Column " + col.dot()
                    + " is not present in the projected schema or is unreachable through a null struct ancestor (accessor "
                    + accessor + ")");
        }
        return resolved;
    }

    private record Resolved(LevelElementRecord target, int index) {}

    // --- per-field value reads ---

    private boolean isNullField(FieldMeta field) {
        return switch (field.element()) {
            case ElementMeta.Absent _ -> true;
            case ElementMeta.Scalar scalar -> scalarLeafOf(field, scalar).isNull(positionOf(scalar));
            case ElementMeta.NestedList(ListMeta list) ->
                nullAtStructureLeaf(list.structureLeaf(), list.groupDefLevel());
            case ElementMeta.Struct(StructMeta nested) ->
                nullAtStructureLeaf(nested.structureLeaf(), nested.presenceDef());
            case ElementMeta.VariantLeaves variant ->
                nullAtStructureLeaf(variantStructureLeaf(variant), variant.presenceDef());
            case ElementMeta.MapEntry(MapMeta map) -> nullAtStructureLeaf(map.structureLeaf(), map.groupDefLevel());
        };
    }

    /**
     * Whether the group node read through {@code structureLeaf} holds a null value at this element's position: true
     * when the projection dropped every descendant, or when the structure leaf's definition level at the window start
     * stays below the node's presence level.
     */
    private boolean nullAtStructureLeaf(ColumnPath structureLeaf, int presenceDef) {
        if (structureLeaf == null) {
            return true;
        }
        int def = vec.levels(structureLeaf).defLevels().get(windows.start(structureLeaf));
        return def < presenceDef;
    }

    private Object scalarValue(FieldMeta field, ElementMeta.Scalar scalar) {
        ColumnVector leaf = leafAt(scalar);
        int position = positionOf(scalar);
        return switch (leaf) {
            case IntVector v -> v.getInt(position);
            case LongVector v -> v.getLong(position);
            case FloatVector v -> v.getFloat(position);
            case DoubleVector v -> v.getDouble(position);
            case BooleanVector v -> v.getBoolean(position);
            case BinaryVector v -> v.get(position);
            case FixedLenBinaryVector v -> v.get(position);
            case Int96Vector v -> v.get(position);
            default -> throw mismatch(field, "get");
        };
    }

    private LevelElementRecord structField(StructMeta nested) {
        ColumnPath structureLeaf = nested.structureLeaf();
        if (structureLeaf == null) {
            return null;
        }
        int def = vec.levels(structureLeaf).defLevels().get(windows.start(structureLeaf));
        if (def < nested.presenceDef()) {
            return null;
        }
        return new LevelElementRecord(vec, rowIndex, nested, windows);
    }

    /**
     * The inner list's entries lie within this element's window in every descendant leaf, which lets a multi-leaf inner
     * subtree reuse this record's windows directly (sibling-leaf windows in them are unused and harmless); a
     * scalar-at-every-depth inner subtree reads on through its single structure leaf.
     *
     * <p>The window can open with a phantom entry (a null first element) yet still hold real elements after it: in an
     * unannotated structural list the element wrapper adds a definition level below the repeated child, opening a
     * definition gap the opener can sit in without terminating the list. Emptiness is therefore left to the returned
     * view's element count (an empty view equals {@link List#of()}); only the null check on the inner container stays.
     */
    @SuppressWarnings({"unchecked", "java:S1168"})
    private List<ParquetRecord> listField(ListMeta list) {
        ColumnPath structureLeaf = list.structureLeaf();
        if (structureLeaf == null) {
            return null;
        }
        int start = windows.start(structureLeaf);
        int def = vec.levels(structureLeaf).defLevels().get(start);
        if (def < list.groupDefLevel()) {
            return null;
        }
        if (list.multiLeaf()) {
            return (List<ParquetRecord>)
                    (List<?>) LevelListMaterializer.nestedListView(vec, rowIndex, list, structureLeaf, windows);
        }
        return (List<ParquetRecord>) (List<?>) LevelListMaterializer.nestedListView(
                vec, rowIndex, list, structureLeaf, start, windows.end(structureLeaf));
    }

    /**
     * The map field's entries lie within this element's window in every descendant leaf, the same pass-through a nested
     * list field uses; the map materializer reads null-vs-empty-vs-present from the structure leaf at the window start.
     */
    private Map<?, ?> mapField(MapMeta map) {
        return LevelMapMaterializer.materialize(vec, rowIndex, map, windows);
    }

    // --- leaf / position helpers ---

    private ColumnVector leafAt(ElementMeta.Scalar scalar) {
        return vec.leaf(scalar.leaf());
    }

    private ColumnVector scalarLeafOf(FieldMeta field, ElementMeta.Scalar scalar) {
        ColumnVector leaf = leafAt(scalar);
        if (leaf == null) {
            throw new IllegalStateException("Scalar field " + field.name() + " has no backing leaf vector");
        }
        return leaf;
    }

    private int positionOf(ElementMeta.Scalar scalar) {
        return windows.startAt(scalar.leafOrdinal());
    }

    private static ColumnPath variantStructureLeaf(ElementMeta.VariantLeaves variant) {
        return variant.metadataLeaf() != null ? variant.metadataLeaf() : variant.valueLeaf();
    }

    private <R> R readBinaryValue(BinaryVector bv, int position, BinaryView<R> view) {
        if (bv.isNull(position)) {
            return null;
        }
        if (bv.isDictionary()) {
            MemorySegment entry = bv.dictionaryEntries()[bv.dictionaryIndices().get(position)];
            return view.read(entry, 0L, entry.byteSize());
        }
        MemorySegment backing = bv.consolidatedBacking();
        int start = bv.consolidatedOffsets().get(position);
        int length = bv.consolidatedOffsets().get(position + 1) - start;
        return view.read(backing, start, length);
    }

    private static <R> R readSlice(MemorySegment slice, BinaryView<R> view) {
        if (slice == null) {
            return null;
        }
        return view.read(slice, 0L, slice.byteSize());
    }

    private ParquetSchemaException mismatch(FieldMeta field, String accessor) {
        return new ParquetSchemaException(
                "Column " + field.name() + " is " + kindLabel(field) + "; requested " + accessor);
    }

    private String kindLabel(FieldMeta field) {
        return switch (field.element()) {
            case ElementMeta.Scalar scalar -> scalarKindLabel(leafAt(scalar));
            case ElementMeta.NestedList _ -> "LIST";
            case ElementMeta.MapEntry _ -> "MAP";
            case ElementMeta.Struct _ -> "STRUCT";
            case ElementMeta.VariantLeaves _ -> "VARIANT";
            case ElementMeta.Absent _ -> ABSENT;
        };
    }

    private static String scalarKindLabel(ColumnVector leaf) {
        return switch (leaf) {
            case IntVector _ -> "INT32";
            case LongVector _ -> "INT64";
            case FloatVector _ -> "FLOAT";
            case DoubleVector _ -> "DOUBLE";
            case BooleanVector _ -> "BOOLEAN";
            case BinaryVector _ -> "BYTE_ARRAY";
            case FixedLenBinaryVector _ -> "FIXED_LEN_BYTE_ARRAY";
            case Int96Vector _ -> "INT96";
            case null -> ABSENT;
            default -> ABSENT;
        };
    }

    private static byte[] toBytes(MemorySegment backing, long offset, long length) {
        byte[] out = new byte[(int) length];
        MemorySegment.copy(backing, JAVA_BYTE, offset, out, 0, (int) length);
        return out;
    }
}
