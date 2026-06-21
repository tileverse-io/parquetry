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
package io.tileverse.parquetry.data;

import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.internal.write.ColumnAccumulator;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Accumulates authored rows into one columnar buffer per leaf column and freezes them into an immutable
 * {@link ParquetRecordBatch}.
 *
 * <p>Cells are staged column by column through the typed {@code set*} setters (addressed by leaf index or by
 * {@link ColumnPath}), and each authored row is closed with {@link #endRow()}. A column left unset for a row defaults
 * to null when the leaf is {@link Repetition#OPTIONAL}, while an unset {@link Repetition#REQUIRED} leaf fails the row.
 * {@link #build()} turns the buffers into a heap-backed batch.
 *
 * <p>This builder handles flat schemas where every leaf is a top-level primitive. A {@link Repetition#REPEATED} leaf is
 * rejected at construction.
 *
 * <p>Two terminal modes exist. A standalone builder ({@link #forSchema}) accumulates every authored row and produces
 * one heap-backed batch through {@link #build()}; the caller decides when to stop. A builder bound to a
 * {@link ParquetWriter} ({@link ParquetWriter#appender()}) auto-flushes a batch to its writer once it reaches
 * {@code flushThresholdRows} rows or its accumulated cells reach {@code flushThresholdBytes}, and on an explicit
 * {@link #flush()}, keeping heap bounded for unbounded row-at-a-time producers. The byte threshold bounds the transient
 * authoring heap and the per-batch row-group overshoot independently of the per-cell size. The two terminals are
 * mutually exclusive: {@link #build()} rejects a writer-bound builder, and {@link #flush()} rejects a standalone
 * builder.
 */
public final class ParquetRecordBatchBuilder implements AutoCloseable {

    static final int DEFAULT_BATCH_ROWS = 8192;
    static final long DEFAULT_BATCH_BYTES = 16L * 1024 * 1024;
    // Check for interrupt every 1024 rows: a cheap bitmask cadence that bounds cancellation latency without a per-row
    // branch cost.
    private static final int INTERRUPT_CHECK_ROW_MASK = 1023;

    private final ParquetSchema schema;
    private final List<ColumnPath> leaves;
    private final Map<ColumnPath, Integer> indexByPath;
    private final ColumnAccumulator[] accumulators;
    private final boolean[] required;
    private final boolean[] setThisRow;
    private final ParquetWriter boundWriter;
    private final int flushThresholdRows;
    private final long flushThresholdBytes;
    private int rows;

    // Counts authored value bytes only (not validity, offsets, or accumulator overhead); a soft lower bound used purely
    // to trigger an auto-flush.
    private long approxBatchBytes;

    public static ParquetRecordBatchBuilder forSchema(ParquetSchema schema) {
        return new ParquetRecordBatchBuilder(schema, null, 0, DEFAULT_BATCH_BYTES);
    }

    static ParquetRecordBatchBuilder boundTo(
            ParquetWriter writer, ParquetSchema schema, int flushThresholdRows, long flushThresholdBytes) {
        return new ParquetRecordBatchBuilder(schema, writer, flushThresholdRows, flushThresholdBytes);
    }

    private ParquetRecordBatchBuilder(
            ParquetSchema schema, ParquetWriter boundWriter, int flushThresholdRows, long flushThresholdBytes) {
        this.schema = schema;
        this.boundWriter = boundWriter;
        this.flushThresholdRows = flushThresholdRows;
        this.flushThresholdBytes = flushThresholdBytes;
        this.leaves = List.copyOf(schema.leafColumns());
        int leafCount = leaves.size();
        this.indexByPath = HashMap.newHashMap(leafCount);
        this.accumulators = new ColumnAccumulator[leafCount];
        this.required = new boolean[leafCount];
        this.setThisRow = new boolean[leafCount];
        for (int i = 0; i < leafCount; i++) {
            ColumnPath path = leaves.get(i);
            SchemaNode.Primitive leaf = resolvePrimitive(path);
            rejectRepeated(path, leaf);
            PrimitiveKind kind = leaf.kind();
            int width = fixedWidthOf(leaf);
            this.accumulators[i] = ColumnAccumulator.forKind(kind, width);
            this.required[i] = leaf.repetition() == Repetition.REQUIRED;
            this.indexByPath.put(path, i);
        }
    }

    public ParquetRecordBatchBuilder setBoolean(int col, boolean value) {
        accumulators[col].setBoolean(value);
        setThisRow[col] = true;
        approxBatchBytes += 1;
        return this;
    }

    public ParquetRecordBatchBuilder setInt(int col, int value) {
        accumulators[col].setInt(value);
        setThisRow[col] = true;
        approxBatchBytes += 4;
        return this;
    }

    public ParquetRecordBatchBuilder setLong(int col, long value) {
        accumulators[col].setLong(value);
        setThisRow[col] = true;
        approxBatchBytes += 8;
        return this;
    }

    public ParquetRecordBatchBuilder setFloat(int col, float value) {
        accumulators[col].setFloat(value);
        setThisRow[col] = true;
        approxBatchBytes += 4;
        return this;
    }

    public ParquetRecordBatchBuilder setDouble(int col, double value) {
        accumulators[col].setDouble(value);
        setThisRow[col] = true;
        approxBatchBytes += 8;
        return this;
    }

    public ParquetRecordBatchBuilder setBinary(int col, MemorySegment value) {
        accumulators[col].setBinary(value);
        setThisRow[col] = true;
        approxBatchBytes += value.byteSize();
        return this;
    }

    public ParquetRecordBatchBuilder setString(int col, String value) {
        return setBinary(col, MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8)));
    }

    public ParquetRecordBatchBuilder setUuid(int col, UUID value) {
        return setBinary(col, UuidConverter.toReadOnlySegment(value));
    }

    public ParquetRecordBatchBuilder setNull(int col) {
        if (required[col]) {
            throw new ParquetWriteException("Required column " + leaves.get(col).dot() + " cannot be set to null");
        }
        accumulators[col].setNull();
        setThisRow[col] = true;
        return this;
    }

    public ParquetRecordBatchBuilder setBoolean(ColumnPath path, boolean value) {
        return setBoolean(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setInt(ColumnPath path, int value) {
        return setInt(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setLong(ColumnPath path, long value) {
        return setLong(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setFloat(ColumnPath path, float value) {
        return setFloat(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setDouble(ColumnPath path, double value) {
        return setDouble(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setBinary(ColumnPath path, MemorySegment value) {
        return setBinary(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setString(ColumnPath path, String value) {
        return setString(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setUuid(ColumnPath path, UUID value) {
        return setUuid(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setNull(ColumnPath path) {
        return setNull(indexOf(path));
    }

    /**
     * Closes the current row. An unset REQUIRED column throws {@link ParquetWriteException}; after such a programming
     * error the builder is left partially advanced and must be discarded.
     */
    public void endRow() {
        for (int i = 0; i < accumulators.length; i++) {
            if (!setThisRow[i]) {
                fillUnsetColumn(i);
            }
            accumulators[i].endRow();
            setThisRow[i] = false;
        }
        rows++;
        if (boundWriter == null) {
            return;
        }
        if ((rows & INTERRUPT_CHECK_ROW_MASK) == 0) {
            boundWriter.checkInterrupt();
        }
        if (rows >= flushThresholdRows || approxBatchBytes >= flushThresholdBytes) {
            flush();
        }
    }

    public int rowCount() {
        return rows;
    }

    /**
     * Freezes the accumulated rows into one immutable heap-backed batch. Valid only for a standalone builder; a
     * writer-bound appender must use {@link #flush()} instead, which would otherwise bypass the writer.
     */
    public ParquetRecordBatch build() {
        if (boundWriter != null) {
            throw new ParquetWriteException("build() is for a standalone batch; a writer-bound appender uses flush()");
        }
        return freezeBatch();
    }

    /**
     * Writes the pending rows as one batch to the bound writer and resets the accumulators for the next batch. A no-op
     * when no rows are pending. Valid only for a writer-bound appender; a standalone builder has no writer to flush to
     * and must use {@link #build()}.
     */
    public void flush() {
        if (boundWriter == null) {
            throw new ParquetWriteException(
                    "flush() requires an appender bound to a writer; use build() for a standalone batch");
        }
        drainTo(boundWriter::writeBatch);
    }

    /**
     * Drains the pending rows into the bound writer's close-time append path, which skips the open check that
     * {@link #flush()} relies on. The writer calls this while finalizing, after it has marked itself closed.
     */
    void flushOnClose() {
        drainTo(boundWriter::writeClosingBatch);
    }

    private void drainTo(Consumer<ParquetRecordBatch> batchSink) {
        if (rows == 0) {
            return;
        }
        ParquetRecordBatch batch = freezeBatch();
        try {
            batchSink.accept(batch);
        } catch (UncheckedIOException e) {
            throw new ParquetWriteException("Appender flush failed", e.getCause());
        }
        resetAccumulators();
    }

    /**
     * Flushes a writer-bound appender's pending rows. A standalone builder (created via {@link #forSchema}) has no
     * writer to flush to and close() is a no-op; call {@link #build()} to obtain its batch.
     */
    @Override
    public void close() {
        if (boundWriter != null) {
            flush();
        }
    }

    private ParquetRecordBatch freezeBatch() {
        Map<ColumnPath, ColumnVector> columns = new HashMap<>();
        for (int i = 0; i < accumulators.length; i++) {
            columns.put(leaves.get(i), accumulators[i].freeze());
        }
        return DefaultParquetRecordBatch.ofHeap(schema, columns, rows);
    }

    private void resetAccumulators() {
        for (int i = 0; i < accumulators.length; i++) {
            accumulators[i].clear();
        }
        rows = 0;
        approxBatchBytes = 0;
    }

    private void fillUnsetColumn(int col) {
        if (required[col]) {
            throw new ParquetWriteException(
                    "Required column " + leaves.get(col).dot() + " was not set for row " + rows);
        }
        // Make the unset-optional-as-null intent explicit, independent of accumulator row-reset internals.
        accumulators[col].setNull();
    }

    private int indexOf(ColumnPath path) {
        Integer index = indexByPath.get(path);
        if (index == null) {
            throw new ParquetWriteException("No such column: " + path.dot());
        }
        return index;
    }

    private SchemaNode.Primitive resolvePrimitive(ColumnPath path) {
        SchemaNode node =
                schema.find(path).orElseThrow(() -> new ParquetWriteException("No such column: " + path.dot()));
        if (!(node instanceof SchemaNode.Primitive primitive)) {
            throw new ParquetWriteException("Column " + path.dot() + " is a group, not a primitive leaf");
        }
        return primitive;
    }

    private void rejectRepeated(ColumnPath path, SchemaNode.Primitive leaf) {
        if (leaf.repetition() == Repetition.REPEATED) {
            throw new ParquetWriteException("Repeated leaf columns are not supported by the writer: " + path.dot());
        }
    }

    private int fixedWidthOf(SchemaNode.Primitive leaf) {
        return leaf.typeLength().orElse(0);
    }
}
