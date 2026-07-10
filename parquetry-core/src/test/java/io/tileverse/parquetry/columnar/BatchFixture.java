/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.columnar;

import java.lang.foreign.Arena;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/** Builds tiny single-column {@link ParquetRecordBatch} instances for batch-level evaluator tests. */
final class BatchFixture {

    private BatchFixture() {}

    /**
     * Builds a one-column batch whose single {@link IntVector} holds {@code values}, with validity taken from
     * {@code validityBits} (a string of '1'/'0' where index i is row i's non-null flag).
     */
    static ParquetRecordBatch intColumn(ColumnPath col, int[] values, String validityBits) {
        Validity validity = validity(validityBits);
        Map<ColumnPath, ColumnVector> columns = Map.of(col, IntVector.materialized(values, validity));
        return new DefaultParquetRecordBatch(singleColumnSchema(col), columns, values.length, Arena.ofConfined());
    }

    private static Validity validity(String validityBits) {
        BitSet validity = new BitSet(validityBits.length());
        for (int i = 0; i < validityBits.length(); i++) {
            if (validityBits.charAt(i) == '1') {
                validity.set(i);
            }
        }
        return Validity.of(validity, validityBits.length());
    }

    private static ParquetSchema singleColumnSchema(ColumnPath col) {
        String leafName = col.name();
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                leafName, Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1));
    }
}
