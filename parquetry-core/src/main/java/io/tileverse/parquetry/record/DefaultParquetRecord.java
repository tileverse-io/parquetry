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

    private static final String READ_BINARY = "readBinary";

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
        return isNullAt(columns, col);
    }

    @Override
    public boolean getBoolean(int col) {
        return getBooleanAt(columns, col);
    }

    @Override
    public int getInt(int col) {
        return getIntAt(columns, col);
    }

    @Override
    public long getLong(int col) {
        return getLongAt(columns, col);
    }

    @Override
    public float getFloat(int col) {
        return getFloatAt(columns, col);
    }

    @Override
    public double getDouble(int col) {
        return getDoubleAt(columns, col);
    }

    @Override
    public String getString(int col) {
        return getStringAt(columns, col);
    }

    @Override
    public byte[] getBinary(int col) {
        return getBinaryAt(columns, col);
    }

    @Override
    public <R> R readBinary(int col, BinaryView<R> view) {
        return readBinaryAt(columns, col, READ_BINARY, view);
    }

    @Override
    public ParquetRecord readStruct(int col) {
        return readStructAt(columns, col);
    }

    @Override
    public List<ParquetRecord> readList(int col) {
        return readListAt(columns, col);
    }

    @Override
    public Map<?, ?> readMap(int col) {
        return readMapAt(columns, col);
    }

    @Override
    public Object get(int col) {
        return getAt(columns, col);
    }

    @Override
    public boolean isNull(ColumnPath col) {
        Resolved leaf = locate(col);
        return leaf == null || isNullAt(leaf.columns(), leaf.index());
    }

    @Override
    public boolean getBoolean(ColumnPath col) {
        Resolved leaf = require(col, "getBoolean");
        return getBooleanAt(leaf.columns(), leaf.index());
    }

    @Override
    public int getInt(ColumnPath col) {
        Resolved leaf = require(col, "getInt");
        return getIntAt(leaf.columns(), leaf.index());
    }

    @Override
    public long getLong(ColumnPath col) {
        Resolved leaf = require(col, "getLong");
        return getLongAt(leaf.columns(), leaf.index());
    }

    @Override
    public float getFloat(ColumnPath col) {
        Resolved leaf = require(col, "getFloat");
        return getFloatAt(leaf.columns(), leaf.index());
    }

    @Override
    public double getDouble(ColumnPath col) {
        Resolved leaf = require(col, "getDouble");
        return getDoubleAt(leaf.columns(), leaf.index());
    }

    @Override
    public String getString(ColumnPath col) {
        Resolved leaf = require(col, "getString");
        return getStringAt(leaf.columns(), leaf.index());
    }

    @Override
    public byte[] getBinary(ColumnPath col) {
        Resolved leaf = require(col, "getBinary");
        return getBinaryAt(leaf.columns(), leaf.index());
    }

    @Override
    public <R> R readBinary(ColumnPath col, BinaryView<R> view) {
        Resolved leaf = require(col, READ_BINARY);
        return readBinaryAt(leaf.columns(), leaf.index(), READ_BINARY, view);
    }

    @Override
    public ParquetRecord readStruct(ColumnPath col) {
        Resolved leaf = locate(col);
        return leaf == null ? null : readStructAt(leaf.columns(), leaf.index());
    }

    @Override
    public List<ParquetRecord> readList(ColumnPath col) {
        Resolved leaf = locate(col);
        return leaf == null ? null : readListAt(leaf.columns(), leaf.index());
    }

    @Override
    public Map<?, ?> readMap(ColumnPath col) {
        Resolved leaf = locate(col);
        return leaf == null ? null : readMapAt(leaf.columns(), leaf.index());
    }

    @Override
    public Object get(ColumnPath col) {
        Resolved leaf = locate(col);
        return leaf == null ? null : getAt(leaf.columns(), leaf.index());
    }

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

    /**
     * Locates a possibly-nested leaf: a column held directly at this level, or one reached by walking struct child
     * layouts one path segment at a time. The walk reuses each level's cached child layout and allocates only the final
     * {@link Resolved}. Returns {@code null} when the path is absent or an intermediate struct cell is null, which the
     * lenient accessors read as a null leaf and the typed accessors reject through {@link #require}.
     */
    private Resolved locate(ColumnPath col) {
        int index = columns.indexOf(col);
        if (index >= 0) {
            return new Resolved(columns, index);
        }
        if (col.numParts() == 1) {
            return null;
        }
        String dotted = col.dot();
        RowColumns level = columns;
        int segmentStart = 0;
        while (true) {
            int separator = dotted.indexOf('.', segmentStart);
            int segmentEnd = separator < 0 ? dotted.length() : separator;
            int segmentIndex = level.indexOfSegment(dotted, segmentStart, segmentEnd);
            if (segmentIndex < 0) {
                return null;
            }
            if (separator < 0) {
                return new Resolved(level, segmentIndex);
            }
            if (!(level.vector(segmentIndex) instanceof StructVector struct) || struct.isNull(rowIndex)) {
                return null;
            }
            level = level.structColumns(segmentIndex);
            segmentStart = segmentEnd + 1;
        }
    }

    private Resolved require(ColumnPath col, String accessor) {
        Resolved leaf = locate(col);
        if (leaf == null) {
            throw new ParquetSchemaException("Column " + col.dot()
                    + " is not present in the projected schema or is unreachable through a null struct ancestor (accessor "
                    + accessor + ")");
        }
        return leaf;
    }

    /** A located leaf: the (possibly nested) column layout that directly holds it, and its index within that layout. */
    private record Resolved(RowColumns columns, int index) {}

    private boolean isNullAt(RowColumns cols, int col) {
        ColumnVector vec = cols.vector(col);
        return vec == null || vec.isNull(rowIndex);
    }

    private boolean getBooleanAt(RowColumns cols, int col) {
        if (cols.vector(col) instanceof BooleanVector vec) {
            return vec.getBoolean(rowIndex);
        }
        throw mismatch(cols, col, "getBoolean");
    }

    private int getIntAt(RowColumns cols, int col) {
        if (cols.vector(col) instanceof IntVector vec) {
            return vec.getInt(rowIndex);
        }
        throw mismatch(cols, col, "getInt");
    }

    private long getLongAt(RowColumns cols, int col) {
        if (cols.vector(col) instanceof LongVector vec) {
            return vec.getLong(rowIndex);
        }
        throw mismatch(cols, col, "getLong");
    }

    private float getFloatAt(RowColumns cols, int col) {
        if (cols.vector(col) instanceof FloatVector vec) {
            return vec.getFloat(rowIndex);
        }
        throw mismatch(cols, col, "getFloat");
    }

    private double getDoubleAt(RowColumns cols, int col) {
        if (cols.vector(col) instanceof DoubleVector vec) {
            return vec.getDouble(rowIndex);
        }
        throw mismatch(cols, col, "getDouble");
    }

    private String getStringAt(RowColumns cols, int col) {
        return readBinaryAt(
                cols,
                col,
                "getString",
                (backing, offset, length) -> new String(toBytes(backing, offset, length), StandardCharsets.UTF_8));
    }

    private byte[] getBinaryAt(RowColumns cols, int col) {
        return readBinaryAt(cols, col, "getBinary", DefaultParquetRecord::toBytes);
    }

    private <R> R readBinaryAt(RowColumns cols, int col, String accessor, BinaryView<R> view) {
        ColumnVector vec = cols.vector(col);
        if (vec == null || vec.isNull(rowIndex)) {
            return null;
        }
        return switch (vec) {
            case BinaryVector bv -> readBinaryValue(bv, view);
            case FixedLenBinaryVector fb -> readSlice(fb.get(rowIndex), view);
            case Int96Vector iv -> readSlice(iv.get(rowIndex), view);
            default -> throw mismatch(cols, col, accessor);
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

    private ParquetRecord readStructAt(RowColumns cols, int col) {
        ColumnVector vec = cols.vector(col);
        if (vec == null) {
            return null;
        }
        if (!(vec instanceof StructVector struct)) {
            throw new ParquetSchemaException("Column " + cols.path(col).dot() + " is not a struct column");
        }
        if (struct.isNull(rowIndex)) {
            return null;
        }
        return new DefaultParquetRecord(cols.structColumns(col), rowIndex);
    }

    // null distinguishes a null row from an empty list per the empty-vs-null contract
    @SuppressWarnings({"unchecked", "java:S1168"})
    private List<ParquetRecord> readListAt(RowColumns cols, int col) {
        ColumnVector vec = cols.vector(col);
        if (vec == null) {
            return null;
        }
        if (!(vec instanceof ListVector list)) {
            throw new ParquetSchemaException("Column " + cols.path(col).dot() + " is not a list column");
        }
        return (List<ParquetRecord>) ListMaterializer.materializeAt(list, rowIndex, cols.schema());
    }

    // S1168: null distinguishes a null map cell from a present empty map
    @SuppressWarnings("java:S1168")
    private Map<?, ?> readMapAt(RowColumns cols, int col) {
        ColumnVector vec = cols.vector(col);
        if (vec == null) {
            return null;
        }
        if (!(vec instanceof MapVector map)) {
            throw new ParquetSchemaException("Column " + cols.path(col).dot() + " is not a map column");
        }
        return MapMaterializer.materializeAt(map, rowIndex, cols.schema());
    }

    private Object getAt(RowColumns cols, int col) {
        ColumnVector vec = cols.vector(col);
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
            case ListVector list -> ListMaterializer.materializeAt(list, rowIndex, cols.schema());
            case MapVector map -> MapMaterializer.materializeAt(map, rowIndex, cols.schema());
            case StructVector _ -> new DefaultParquetRecord(cols.structColumns(col), rowIndex);
            case VariantVector variant -> variant.get(rowIndex);
        };
    }

    private ParquetSchemaException mismatch(RowColumns cols, int col, String accessor) {
        ColumnVector vec = cols.vector(col);
        String kind = vec == null ? "absent" : kindLabel(vec);
        return new ParquetSchemaException("Column " + cols.path(col).dot() + " is " + kind + "; requested " + accessor);
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
