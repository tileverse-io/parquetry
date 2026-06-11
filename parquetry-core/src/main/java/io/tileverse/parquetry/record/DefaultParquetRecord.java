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

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.Int96Vector;
import io.tileverse.parquetry.batch.IntSequence;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.MapVector;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.VariantVector;
import io.tileverse.parquetry.materializer.ListMaterializer;
import io.tileverse.parquetry.materializer.MapMaterializer;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;

/**
 * The live {@link ParquetRecord}: one row of a batch, addressed positionally over a shared {@link RowColumns}. The same
 * type represents a top-level row and a nested struct cell. Holds only the column set and a row index; values are read
 * straight from the column vectors with no boxing.
 *
 * <p>The view is valid only while the producing batch is open. A caller that needs values past batch close calls
 * {@link #detach()}, which returns an owned {@link DetachedParquetRecord}.
 */
public final class DefaultParquetRecord implements ParquetRecord {

    private final RowColumns columns;
    private final int rowIndex;

    /** Wraps row {@code rowIndex} of {@code columns}. */
    public DefaultParquetRecord(RowColumns columns, int rowIndex) {
        this.columns = columns;
        this.rowIndex = rowIndex;
    }

    @Override
    public ParquetSchema schema() {
        return columns.schema();
    }

    @Override
    public int columnCount() {
        return columns.columnCount();
    }

    @Override
    public ColumnPath columnPath(int col) {
        return columns.path(col);
    }

    @Override
    public boolean isNull(int col) {
        ColumnVector vec = columns.vector(col);
        return vec == null || vec.isNull(rowIndex);
    }

    @Override
    public boolean getBoolean(int col) {
        if (columns.vector(col) instanceof BooleanVector vec) {
            return vec.getBoolean(rowIndex);
        }
        throw mismatch(col, "getBoolean");
    }

    @Override
    public int getInt(int col) {
        if (columns.vector(col) instanceof IntVector vec) {
            return vec.getInt(rowIndex);
        }
        throw mismatch(col, "getInt");
    }

    @Override
    public long getLong(int col) {
        if (columns.vector(col) instanceof LongVector vec) {
            return vec.getLong(rowIndex);
        }
        throw mismatch(col, "getLong");
    }

    @Override
    public float getFloat(int col) {
        if (columns.vector(col) instanceof FloatVector vec) {
            return vec.getFloat(rowIndex);
        }
        throw mismatch(col, "getFloat");
    }

    @Override
    public double getDouble(int col) {
        if (columns.vector(col) instanceof DoubleVector vec) {
            return vec.getDouble(rowIndex);
        }
        throw mismatch(col, "getDouble");
    }

    @Override
    public String getString(int col) {
        return readBinary(
                col,
                "getString",
                (backing, offset, length) -> new String(toBytes(backing, offset, length), StandardCharsets.UTF_8));
    }

    @Override
    public byte[] getBinary(int col) {
        return readBinary(col, "getBinary", DefaultParquetRecord::toBytes);
    }

    @Override
    public <R> R readBinary(int col, BinaryView<R> view) {
        return readBinary(col, "readBinary", view);
    }

    private <R> R readBinary(int col, String accessor, BinaryView<R> view) {
        ColumnVector vec = columns.vector(col);
        if (vec == null || vec.isNull(rowIndex)) {
            return null;
        }
        return switch (vec) {
            case BinaryVector bv -> readBinaryValue(bv, view);
            case FixedLenBinaryVector fb -> readSlice(fb.get(rowIndex), view);
            case Int96Vector iv -> readSlice(iv.get(rowIndex), view);
            default -> throw mismatch(col, accessor);
        };
    }

    private <R> R readBinaryValue(BinaryVector bv, BinaryView<R> view) {
        if (bv.isDictionary()) {
            MemorySegment entry = bv.dictionaryEntries()[bv.dictionaryIndices().get(rowIndex)];
            return view.read(entry, 0L, entry.byteSize());
        }
        MemorySegment backing = bv.consolidatedBacking();
        IntSequence offsets = bv.consolidatedOffsets();
        int start = offsets.get(rowIndex);
        int length = offsets.get(rowIndex + 1) - start;
        return view.read(backing, start, length);
    }

    private static <R> R readSlice(MemorySegment slice, BinaryView<R> view) {
        return view.read(slice, 0L, slice.byteSize());
    }

    @Override
    public ParquetRecord readStruct(int col) {
        ColumnVector vec = columns.vector(col);
        if (vec == null) {
            return null;
        }
        if (!(vec instanceof StructVector struct)) {
            throw new ParquetSchemaException("Column " + columns.path(col).dot() + " is not a struct column");
        }
        if (struct.isNull(rowIndex)) {
            return null;
        }
        return new DefaultParquetRecord(columns.structColumns(col), rowIndex);
    }

    // null distinguishes a null row from an empty list per the empty-vs-null contract
    @Override
    @SuppressWarnings({"unchecked", "java:S1168"})
    public List<ParquetRecord> readList(int col) {
        ColumnVector vec = columns.vector(col);
        if (vec == null) {
            return null;
        }
        if (!(vec instanceof ListVector list)) {
            throw new ParquetSchemaException("Column " + columns.path(col).dot() + " is not a list column");
        }
        return (List<ParquetRecord>) ListMaterializer.materializeAt(list, rowIndex, columns.schema());
    }

    // S1168: null distinguishes a null map cell from a present empty map
    @Override
    @SuppressWarnings("java:S1168")
    public Map<?, ?> readMap(int col) {
        ColumnVector vec = columns.vector(col);
        if (vec == null) {
            return null;
        }
        if (!(vec instanceof MapVector map)) {
            throw new ParquetSchemaException("Column " + columns.path(col).dot() + " is not a map column");
        }
        return MapMaterializer.materializeAt(map, rowIndex, columns.schema());
    }

    @Override
    public Object get(int col) {
        ColumnVector vec = columns.vector(col);
        if (vec == null || vec.isNull(rowIndex)) {
            return null;
        }
        return switch (vec) {
            case IntVector iv -> iv.getInt(rowIndex);
            case LongVector lv -> lv.getLong(rowIndex);
            case FloatVector fv -> fv.getFloat(rowIndex);
            case DoubleVector dv -> dv.getDouble(rowIndex);
            case BooleanVector bv -> bv.getBoolean(rowIndex);
            case BinaryVector bv -> bv.get(rowIndex);
            case FixedLenBinaryVector fb -> fb.get(rowIndex);
            case Int96Vector iv -> iv.get(rowIndex);
            case ListVector list -> ListMaterializer.materializeAt(list, rowIndex, columns.schema());
            case MapVector map -> MapMaterializer.materializeAt(map, rowIndex, columns.schema());
            case StructVector _ -> new DefaultParquetRecord(columns.structColumns(col), rowIndex);
            case VariantVector variant -> variant.get(rowIndex);
        };
    }

    @Override
    public boolean isNull(ColumnPath col) {
        Resolved leaf = locate(col);
        return leaf == null || leaf.record().isNull(leaf.index());
    }

    @Override
    public boolean getBoolean(ColumnPath col) {
        Resolved leaf = require(col, "getBoolean");
        return leaf.record().getBoolean(leaf.index());
    }

    @Override
    public int getInt(ColumnPath col) {
        Resolved leaf = require(col, "getInt");
        return leaf.record().getInt(leaf.index());
    }

    @Override
    public long getLong(ColumnPath col) {
        Resolved leaf = require(col, "getLong");
        return leaf.record().getLong(leaf.index());
    }

    @Override
    public float getFloat(ColumnPath col) {
        Resolved leaf = require(col, "getFloat");
        return leaf.record().getFloat(leaf.index());
    }

    @Override
    public double getDouble(ColumnPath col) {
        Resolved leaf = require(col, "getDouble");
        return leaf.record().getDouble(leaf.index());
    }

    @Override
    public String getString(ColumnPath col) {
        Resolved leaf = require(col, "getString");
        return leaf.record().getString(leaf.index());
    }

    @Override
    public byte[] getBinary(ColumnPath col) {
        Resolved leaf = require(col, "getBinary");
        return leaf.record().getBinary(leaf.index());
    }

    @Override
    public <R> R readBinary(ColumnPath col, BinaryView<R> view) {
        Resolved leaf = require(col, "readBinary");
        return leaf.record().readBinary(leaf.index(), view);
    }

    @Override
    public ParquetRecord readStruct(ColumnPath col) {
        Resolved leaf = locate(col);
        return leaf == null ? null : leaf.record().readStruct(leaf.index());
    }

    @Override
    public List<ParquetRecord> readList(ColumnPath col) {
        Resolved leaf = locate(col);
        return leaf == null ? null : leaf.record().readList(leaf.index());
    }

    @Override
    public Map<?, ?> readMap(ColumnPath col) {
        Resolved leaf = locate(col);
        return leaf == null ? null : leaf.record().readMap(leaf.index());
    }

    @Override
    public Object get(ColumnPath col) {
        Resolved leaf = locate(col);
        return leaf == null ? null : leaf.record().get(leaf.index());
    }

    /**
     * Locates a possibly-nested leaf: a column held directly at this level, or one reached by descending struct
     * sub-records one path segment at a time. The descent reuses each struct's cached child layout and resolves
     * segments by name, allocating no intermediate {@link ColumnPath}. Returns {@code null} when the path is absent or
     * an intermediate struct cell is null, which the accessors read as a null leaf.
     */
    private Resolved locate(ColumnPath col) {
        int index = columns.indexOf(col);
        if (index >= 0) {
            return new Resolved(this, index);
        }
        if (col.numParts() == 1) {
            return null;
        }
        return descend(col, 0);
    }

    private Resolved descend(ColumnPath col, int segmentStart) {
        String dotted = col.dot();
        int separator = dotted.indexOf('.', segmentStart);
        int segmentEnd = separator < 0 ? dotted.length() : separator;
        int index = columns.indexOfSegment(dotted, segmentStart, segmentEnd);
        if (index < 0) {
            return null;
        }
        if (separator < 0) {
            return new Resolved(this, index);
        }
        if (columns.vector(index) instanceof StructVector struct && !struct.isNull(rowIndex)) {
            DefaultParquetRecord nested = new DefaultParquetRecord(columns.structColumns(index), rowIndex);
            return nested.descend(col, segmentEnd + 1);
        }
        return null;
    }

    private Resolved require(ColumnPath col, String accessor) {
        Resolved leaf = locate(col);
        if (leaf == null) {
            throw new ParquetSchemaException(
                    "Column " + col.dot() + " is not present in the projected schema (accessor " + accessor + ")");
        }
        return leaf;
    }

    /** A located leaf: the (possibly nested) record that directly holds it, and its index within that record. */
    private record Resolved(DefaultParquetRecord record, int index) {}

    @Override
    public ParquetRecord detach() {
        int count = columns.columnCount();
        ColumnPath[] paths = new ColumnPath[count];
        Object[] values = new Object[count];
        for (int i = 0; i < count; i++) {
            paths[i] = columns.path(i);
            values[i] = Detach.detach(get(i));
        }
        return new DetachedParquetRecord(columns.schema(), paths, values);
    }

    private ParquetSchemaException mismatch(int col, String accessor) {
        ColumnVector vec = columns.vector(col);
        String kind = vec == null ? "absent" : kindLabel(vec);
        return new ParquetSchemaException(
                "Column " + columns.path(col).dot() + " is " + kind + "; requested " + accessor);
    }

    private static String kindLabel(ColumnVector vec) {
        return switch (vec) {
            case IntVector _ -> "INT32";
            case LongVector _ -> "INT64";
            case FloatVector _ -> "FLOAT";
            case DoubleVector _ -> "DOUBLE";
            case BooleanVector _ -> "BOOLEAN";
            case BinaryVector _ -> "BYTE_ARRAY";
            case FixedLenBinaryVector _ -> "FIXED_LEN_BYTE_ARRAY";
            case Int96Vector _ -> "INT96";
            case ListVector _ -> "LIST";
            case MapVector _ -> "MAP";
            case StructVector _ -> "STRUCT";
            case VariantVector _ -> "VARIANT";
        };
    }

    private static byte[] toBytes(MemorySegment backing, long offset, long length) {
        byte[] out = new byte[(int) length];
        MemorySegment.copy(backing, JAVA_BYTE, offset, out, 0, (int) length);
        return out;
    }
}
