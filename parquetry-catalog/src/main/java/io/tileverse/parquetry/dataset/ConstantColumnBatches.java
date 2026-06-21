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
package io.tileverse.parquetry.dataset;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.ConstantVectors;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.filter.ConstantColumn;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/** Appends constant output columns to a read's batches and flattens a batch into records. */
final class ConstantColumnBatches {

    private ConstantColumnBatches() {}

    static ParquetRecordBatch append(ParquetRecordBatch base, List<ConstantColumn> constants) {
        List<SchemaNode.Primitive> leaves = new ArrayList<>(constants.size());
        Map<ColumnPath, ColumnVector> vectors = new LinkedHashMap<>();
        int fieldId = nextFieldId(base);
        for (ConstantColumn constant : constants) {
            leaves.add(leafFor(constant, fieldId++));
            vectors.put(constant.path(), ConstantVectors.of(constant.value(), base.rowCount()));
        }
        return DefaultParquetRecordBatch.withConstantColumns(base, leaves, vectors);
    }

    static Stream<ParquetRecord> rows(ParquetRecordBatch batch) {
        return IntStream.range(0, batch.rowCount()).mapToObj(batch::materialize).onClose(batch::close);
    }

    /**
     * Returns one past the highest field id already present in the base schema to keep a synthesized leaf from reusing
     * a physical leaf's id. Synthesized field ids are presentational only; column resolution is by path, not by id.
     */
    private static int nextFieldId(ParquetRecordBatch base) {
        return maxFieldId(base.projectedSchema().root()) + 1;
    }

    private static int maxFieldId(SchemaNode node) {
        int max = node.fieldId();
        if (node instanceof SchemaNode.Group group) {
            for (SchemaNode child : group.children()) {
                max = Math.max(max, maxFieldId(child));
            }
        }
        return max;
    }

    private static SchemaNode.Primitive leafFor(ConstantColumn constant, int fieldId) {
        String name = constant.path().name();
        return switch (constant.value()) {
            case Value.IntVal ignored -> primitive(name, PrimitiveKind.INT32, Optional.empty(), fieldId);
            case Value.LongVal ignored -> primitive(name, PrimitiveKind.INT64, Optional.empty(), fieldId);
            case Value.DoubleVal ignored -> primitive(name, PrimitiveKind.DOUBLE, Optional.empty(), fieldId);
            case Value.FloatVal ignored -> primitive(name, PrimitiveKind.FLOAT, Optional.empty(), fieldId);
            case Value.BoolVal ignored -> primitive(name, PrimitiveKind.BOOLEAN, Optional.empty(), fieldId);
            case Value.DateVal ignored ->
                primitive(name, PrimitiveKind.INT32, Optional.of(new LogicalType.DateType()), fieldId);
            case Value.StringVal ignored ->
                primitive(name, PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()), fieldId);
            default -> throw new IllegalArgumentException("unsupported constant column value " + constant.value());
        };
    }

    private static SchemaNode.Primitive primitive(
            String name, PrimitiveKind kind, Optional<LogicalType> logicalType, int fieldId) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logicalType, fieldId);
    }
}
