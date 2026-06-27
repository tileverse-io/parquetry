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
package io.tileverse.parquetry.cli.arrow;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.BooleanVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Repacks a stream of decoded {@link ParquetRecord}s into column-oriented {@link ParquetRecordBatch}es of a target row
 * count, the shape the Arrow IPC encoder consumes. This bridges the record-level read path (which already applies the
 * row filter and limit) to the columnar output without re-running the columnar decode.
 */
public final class RecordBatchPacker {

    private RecordBatchPacker() {}

    public static Stream<ParquetRecordBatch> pack(
            Stream<ParquetRecord> records, ParquetSchema projectedSchema, int targetRows) {
        if (targetRows <= 0) {
            throw new IllegalArgumentException("targetRows must be > 0, got " + targetRows);
        }
        List<LeafColumn> leaves = resolveLeaves(projectedSchema);
        Iterator<ParquetRecordBatch> batches = batchIterator(records.iterator(), projectedSchema, leaves, targetRows);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(batches, Spliterator.ORDERED), false);
    }

    private static List<LeafColumn> resolveLeaves(ParquetSchema projectedSchema) {
        List<LeafColumn> leaves = new ArrayList<>();
        for (ColumnPath path : projectedSchema.leafColumns()) {
            leaves.add(new LeafColumn(path, primitiveOf(projectedSchema, path)));
        }
        return leaves;
    }

    private static SchemaNode.Primitive primitiveOf(ParquetSchema schema, ColumnPath path) {
        SchemaNode node =
                schema.find(path).orElseThrow(() -> new IllegalArgumentException("No schema node for column " + path));
        if (node instanceof SchemaNode.Primitive primitive) {
            return primitive;
        }
        throw new UnsupportedFeatureException("Arrow output supports leaf columns only; " + path + " is a group");
    }

    private static Iterator<ParquetRecordBatch> batchIterator(
            Iterator<ParquetRecord> records, ParquetSchema projectedSchema, List<LeafColumn> leaves, int targetRows) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return records.hasNext();
            }

            @Override
            public ParquetRecordBatch next() {
                if (!records.hasNext()) {
                    throw new NoSuchElementException();
                }
                List<ParquetRecord> chunk = nextChunk(records, targetRows);
                return batchOf(projectedSchema, leaves, chunk);
            }
        };
    }

    private static List<ParquetRecord> nextChunk(Iterator<ParquetRecord> records, int targetRows) {
        List<ParquetRecord> chunk = new ArrayList<>(targetRows);
        while (chunk.size() < targetRows && records.hasNext()) {
            chunk.add(records.next());
        }
        return chunk;
    }

    private static ParquetRecordBatch batchOf(
            ParquetSchema projectedSchema, List<LeafColumn> leaves, List<ParquetRecord> chunk) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        for (LeafColumn leaf : leaves) {
            columns.put(leaf.path(), vectorFor(leaf, chunk));
        }
        return new DefaultParquetRecordBatch(projectedSchema, columns, chunk.size(), Arena.ofShared());
    }

    private static ColumnVector vectorFor(LeafColumn leaf, List<ParquetRecord> chunk) {
        PrimitiveKind kind = leaf.primitive().kind();
        return switch (kind) {
            case BOOLEAN -> booleanVector(leaf.path(), chunk);
            case INT32 -> intVector(leaf.path(), chunk);
            case INT64 -> longVector(leaf.path(), chunk);
            case FLOAT -> floatVector(leaf.path(), chunk);
            case DOUBLE -> doubleVector(leaf.path(), chunk);
            case BYTE_ARRAY -> binaryVector(leaf.path(), chunk);
            case FIXED_LEN_BYTE_ARRAY -> fixedLenBinaryVector(leaf, chunk);
            case INT96 ->
                throw new UnsupportedFeatureException("Arrow output does not support INT96 column " + leaf.path());
        };
    }

    private static ColumnVector booleanVector(ColumnPath path, List<ParquetRecord> chunk) {
        boolean[] values = new boolean[chunk.size()];
        BitSet validity = new BitSet(chunk.size());
        for (int row = 0; row < chunk.size(); row++) {
            ParquetRecord r = chunk.get(row);
            if (r.isNull(path)) {
                continue;
            }
            validity.set(row);
            values[row] = r.getBoolean(path);
        }
        return BooleanVector.materialized(values, Validity.of(validity, chunk.size()));
    }

    private static ColumnVector intVector(ColumnPath path, List<ParquetRecord> chunk) {
        int[] values = new int[chunk.size()];
        BitSet validity = new BitSet(chunk.size());
        for (int row = 0; row < chunk.size(); row++) {
            ParquetRecord r = chunk.get(row);
            if (r.isNull(path)) {
                continue;
            }
            validity.set(row);
            values[row] = r.getInt(path);
        }
        return IntVector.materialized(values, Validity.of(validity, chunk.size()));
    }

    private static ColumnVector longVector(ColumnPath path, List<ParquetRecord> chunk) {
        long[] values = new long[chunk.size()];
        BitSet validity = new BitSet(chunk.size());
        for (int row = 0; row < chunk.size(); row++) {
            ParquetRecord r = chunk.get(row);
            if (r.isNull(path)) {
                continue;
            }
            validity.set(row);
            values[row] = r.getLong(path);
        }
        return LongVector.materialized(values, Validity.of(validity, chunk.size()));
    }

    private static ColumnVector floatVector(ColumnPath path, List<ParquetRecord> chunk) {
        float[] values = new float[chunk.size()];
        BitSet validity = new BitSet(chunk.size());
        for (int row = 0; row < chunk.size(); row++) {
            ParquetRecord r = chunk.get(row);
            if (r.isNull(path)) {
                continue;
            }
            validity.set(row);
            values[row] = r.getFloat(path);
        }
        return FloatVector.materialized(values, Validity.of(validity, chunk.size()));
    }

    private static ColumnVector doubleVector(ColumnPath path, List<ParquetRecord> chunk) {
        double[] values = new double[chunk.size()];
        BitSet validity = new BitSet(chunk.size());
        for (int row = 0; row < chunk.size(); row++) {
            ParquetRecord r = chunk.get(row);
            if (r.isNull(path)) {
                continue;
            }
            validity.set(row);
            values[row] = r.getDouble(path);
        }
        return DoubleVector.materialized(values, Validity.of(validity, chunk.size()));
    }

    private static ColumnVector binaryVector(ColumnPath path, List<ParquetRecord> chunk) {
        MemorySegment[] values = new MemorySegment[chunk.size()];
        BitSet validity = new BitSet(chunk.size());
        for (int row = 0; row < chunk.size(); row++) {
            ParquetRecord r = chunk.get(row);
            if (r.isNull(path)) {
                continue;
            }
            validity.set(row);
            values[row] = MemorySegment.ofArray(r.getBinary(path));
        }
        return BinaryVector.materialized(values, Validity.of(validity, chunk.size()));
    }

    private static ColumnVector fixedLenBinaryVector(LeafColumn leaf, List<ParquetRecord> chunk) {
        ColumnPath path = leaf.path();
        int byteWidth = leaf.primitive()
                .typeLength()
                .orElseThrow(() -> new UnsupportedFeatureException(
                        "FIXED_LEN_BYTE_ARRAY column " + path + " has no declared type length"));
        MemorySegment[] values = new MemorySegment[chunk.size()];
        BitSet validity = new BitSet(chunk.size());
        for (int row = 0; row < chunk.size(); row++) {
            ParquetRecord r = chunk.get(row);
            if (r.isNull(path)) {
                continue;
            }
            validity.set(row);
            values[row] = MemorySegment.ofArray(r.getBinary(path));
        }
        return FixedLenBinaryVector.materialized(values, byteWidth, Validity.of(validity, chunk.size()));
    }

    private record LeafColumn(ColumnPath path, SchemaNode.Primitive primitive) {}
}
