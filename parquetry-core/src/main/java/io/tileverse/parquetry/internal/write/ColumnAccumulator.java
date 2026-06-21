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
package io.tileverse.parquetry.internal.write;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.BitSet;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.IntSequence;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Growable, single-column write buffer. The batch builder stages one cell per row through the typed {@code set*}
 * setters (or {@link #setNull()}), closes each row with {@link #endRow()}, and finally turns the buffered cells into an
 * immutable {@link ColumnVector} via {@link #freeze()}.
 *
 * <p>Each accumulator accepts exactly one Java type matching its Parquet kind. A setter that does not match the
 * column's kind fails with a {@link ParquetWriteException}, catching authoring mistakes at the call site rather than
 * producing a corrupt column.
 */
public sealed interface ColumnAccumulator
        permits ColumnAccumulator.BooleanAccumulator,
                ColumnAccumulator.IntAccumulator,
                ColumnAccumulator.LongAccumulator,
                ColumnAccumulator.FloatAccumulator,
                ColumnAccumulator.DoubleAccumulator,
                ColumnAccumulator.BinaryAccumulator,
                ColumnAccumulator.FixedLenBinaryAccumulator {

    /**
     * Creates an accumulator for the given primitive kind. {@code byteWidth} applies only to
     * {@link PrimitiveKind#FIXED_LEN_BYTE_ARRAY} and is ignored otherwise.
     */
    static ColumnAccumulator forKind(PrimitiveKind kind, int byteWidth) {
        return switch (kind) {
            case BOOLEAN -> new BooleanAccumulator();
            case INT32 -> new IntAccumulator();
            case INT64 -> new LongAccumulator();
            case FLOAT -> new FloatAccumulator();
            case DOUBLE -> new DoubleAccumulator();
            case BYTE_ARRAY -> new BinaryAccumulator();
            case FIXED_LEN_BYTE_ARRAY -> new FixedLenBinaryAccumulator(byteWidth);
            case INT96 -> throw new ParquetWriteException("INT96 columns are not supported by the writer");
        };
    }

    default void setBoolean(boolean value) {
        throw wrongSetter("boolean");
    }

    default void setInt(int value) {
        throw wrongSetter("int");
    }

    default void setLong(long value) {
        throw wrongSetter("long");
    }

    default void setFloat(float value) {
        throw wrongSetter("float");
    }

    default void setDouble(double value) {
        throw wrongSetter("double");
    }

    default void setBinary(MemorySegment value) {
        throw wrongSetter("binary");
    }

    /** Stages the current row as null. */
    void setNull();

    /** Closes the current row, committing the staged cell or a null into the column buffer. */
    void endRow();

    /**
     * Turns the buffered cells into an immutable column vector. The accumulator may be reused for a new batch after
     * {@link #clear()}; the returned vector is independent of the accumulator's buffers.
     */
    ColumnVector freeze();

    /**
     * Resets the row count and validity for a new batch while keeping the backing arrays at their current capacity.
     * This lets a writer-bound builder reuse one set of accumulators across auto-flush batches without re-allocating.
     */
    void clear();

    private ParquetWriteException wrongSetter(String javaType) {
        return new ParquetWriteException("Column does not accept a " + javaType + " value");
    }

    /**
     * A defensive copy of the validity bits. {@link Validity#of(BitSet, int)} takes ownership of and may mutate the
     * BitSet it is given; because accumulators reuse their {@code valid} field across batches after {@link #clear()},
     * each {@code freeze()} hands {@code Validity} a copy rather than the live field.
     */
    private static BitSet copyValidity(BitSet valid) {
        return (BitSet) valid.clone();
    }

    final class BooleanAccumulator implements ColumnAccumulator {
        private boolean[] values = new boolean[16];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private boolean staged;

        @Override
        public void setBoolean(boolean value) {
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            if (rows == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            if (pending) {
                values[rows] = staged;
                valid.set(rows);
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            return BooleanVector.materialized(Arrays.copyOf(values, rows), Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            rows = 0;
            pending = false;
            valid.clear();
        }
    }

    final class IntAccumulator implements ColumnAccumulator {
        private int[] values = new int[16];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private int staged;

        @Override
        public void setInt(int value) {
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            if (rows == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            if (pending) {
                values[rows] = staged;
                valid.set(rows);
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            return IntVector.materialized(Arrays.copyOf(values, rows), Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            rows = 0;
            pending = false;
            valid.clear();
        }
    }

    final class LongAccumulator implements ColumnAccumulator {
        private long[] values = new long[16];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private long staged;

        @Override
        public void setLong(long value) {
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            if (rows == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            if (pending) {
                values[rows] = staged;
                valid.set(rows);
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            return LongVector.materialized(Arrays.copyOf(values, rows), Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            rows = 0;
            pending = false;
            valid.clear();
        }
    }

    final class FloatAccumulator implements ColumnAccumulator {
        private float[] values = new float[16];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private float staged;

        @Override
        public void setFloat(float value) {
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            if (rows == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            if (pending) {
                values[rows] = staged;
                valid.set(rows);
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            return FloatVector.materialized(Arrays.copyOf(values, rows), Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            rows = 0;
            pending = false;
            valid.clear();
        }
    }

    final class DoubleAccumulator implements ColumnAccumulator {
        private double[] values = new double[16];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private double staged;

        @Override
        public void setDouble(double value) {
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            if (rows == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            if (pending) {
                values[rows] = staged;
                valid.set(rows);
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            return DoubleVector.materialized(Arrays.copyOf(values, rows), Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            rows = 0;
            pending = false;
            valid.clear();
        }
    }

    /**
     * Buffers variable-length binary cells into one growable backing array rather than one wrapper segment per cell.
     * The per-row {@code offsets} delimit each cell's bytes; a null row repeats the running offset as a zero-length
     * run, matching {@link BinaryVector}'s consolidated layout.
     */
    final class BinaryAccumulator implements ColumnAccumulator {
        private byte[] backing = new byte[64];
        private int[] offsets = new int[17];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private MemorySegment staged;

        @Override
        public void setBinary(MemorySegment value) {
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            ensureOffsetsCapacity(rows + 1);
            int start = offsets[rows];
            if (pending) {
                int length = Math.toIntExact(staged.byteSize());
                ensureBackingCapacity(start + length);
                MemorySegment.copy(staged, 0L, MemorySegment.ofArray(backing), start, length);
                offsets[rows + 1] = start + length;
                valid.set(rows);
            } else {
                offsets[rows + 1] = start;
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            int usedBytes = offsets[rows];
            MemorySegment backingSegment = MemorySegment.ofArray(Arrays.copyOf(backing, usedBytes));
            IntSequence rowOffsets = IntSequence.of(Arrays.copyOf(offsets, rows + 1));
            return BinaryVector.of(backingSegment, rowOffsets, Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            rows = 0;
            pending = false;
            offsets[0] = 0;
            valid.clear();
        }

        private void ensureOffsetsCapacity(int neededLength) {
            if (neededLength >= offsets.length) {
                offsets = Arrays.copyOf(offsets, Math.max(offsets.length * 2, neededLength + 1));
            }
        }

        private void ensureBackingCapacity(int neededLength) {
            if (neededLength > backing.length) {
                int grown = backing.length * 2;
                backing = Arrays.copyOf(backing, Math.max(grown, neededLength));
            }
        }
    }

    /**
     * Buffers fixed-width binary cells into one growable backing array, each cell occupying its {@code byteWidth} slot
     * in row order. A null row leaves its slot zeroed, matching {@link FixedLenBinaryVector}'s full-slot layout.
     */
    final class FixedLenBinaryAccumulator implements ColumnAccumulator {
        private final int byteWidth;
        private byte[] backing = new byte[64];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private MemorySegment staged;

        FixedLenBinaryAccumulator(int byteWidth) {
            this.byteWidth = byteWidth;
        }

        @Override
        public void setBinary(MemorySegment value) {
            if (value.byteSize() != byteWidth) {
                throw new ParquetWriteException(
                        "fixed-length column width " + byteWidth + " does not match value width " + value.byteSize());
            }
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            int slotStart = slotBytes();
            ensureBackingCapacity(slotStart + byteWidth);
            if (pending) {
                MemorySegment.copy(staged, 0L, MemorySegment.ofArray(backing), slotStart, byteWidth);
                valid.set(rows);
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            MemorySegment backingSegment = MemorySegment.ofArray(Arrays.copyOf(backing, slotBytes()));
            return FixedLenBinaryVector.of(backingSegment, byteWidth, Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            zeroSlots();
            rows = 0;
            pending = false;
            valid.clear();
        }

        /**
         * Zeroes the slots a previous batch wrote, because a reused backing must present null slots as zero (a null row
         * does not write its slot in {@link #endRow()}).
         */
        private void zeroSlots() {
            Arrays.fill(backing, 0, slotBytes(), (byte) 0);
        }

        private int slotBytes() {
            return Math.multiplyExact(rows, byteWidth);
        }

        private void ensureBackingCapacity(int neededLength) {
            if (neededLength > backing.length) {
                int grown = backing.length * 2;
                backing = Arrays.copyOf(backing, Math.max(grown, neededLength));
            }
        }
    }
}
