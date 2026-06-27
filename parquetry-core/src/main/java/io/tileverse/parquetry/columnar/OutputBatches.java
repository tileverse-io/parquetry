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
package io.tileverse.parquetry.columnar;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.filter.OutputColumn;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Reshapes a decoded batch into a read's ordered output shape. Each {@link OutputColumn} describes one result column:
 * pass a physical column through (renamed, reordered, or dropped), widen one to a broader primitive type, fill a
 * constant for every row, or add an all-null column. Physical passthrough reuses the decoded vector zero-copy; closing
 * the shaped batch closes the base it was built from.
 *
 * <p>The output schema is name-addressed, not field-id-addressed, hence every output leaf gets a fresh synthetic field
 * id (its position in the output order). Two output columns sourced from one physical column therefore stay distinct.
 * Duplicate output names are rejected.
 */
public final class OutputBatches {

    private OutputBatches() {}

    /** The output-ordered batch built from {@code base} per the {@code output} columns, in their given order. */
    public static ParquetRecordBatch shape(ParquetRecordBatch base, List<OutputColumn> output) {
        Map<ColumnPath, ColumnVector> columnsByOutputName = new LinkedHashMap<>();
        List<SchemaNode> outputLeaves = new ArrayList<>(output.size());

        for (int outputFieldId = 0; outputFieldId < output.size(); outputFieldId++) {
            OutputColumn column = output.get(outputFieldId);
            rejectDuplicateName(columnsByOutputName.put(column.name(), vectorFor(column, base)), column.name());
            outputLeaves.add(leafFor(column, base, outputFieldId));
        }

        ParquetSchema outputSchema = schemaFrom(base.projectedSchema(), outputLeaves);
        DefaultParquetRecordBatch result =
                DefaultParquetRecordBatch.ofHeap(outputSchema, columnsByOutputName, base.rowCount());
        result.attachReleaseAction(base::close);
        return result;
    }

    private static void rejectDuplicateName(ColumnVector previous, ColumnPath name) {
        if (previous != null) {
            throw new IllegalArgumentException("duplicate output column name " + name);
        }
    }

    private static ColumnVector vectorFor(OutputColumn column, ParquetRecordBatch base) {
        return switch (column) {
            case OutputColumn.Physical(ColumnPath ignored, ColumnPath source) ->
                base.columns().get(source);
            case OutputColumn.Promoted(ColumnPath ignored, ColumnPath source, PrimitiveKind target) ->
                VectorWidening.widen(base.columns().get(source), target);
            case OutputColumn.Constant(ColumnPath ignored, Value value) -> ConstantVectors.of(value, base.rowCount());
            case OutputColumn.Null(ColumnPath ignored, Value typeOf) -> ConstantVectors.ofNull(typeOf, base.rowCount());
        };
    }

    private static SchemaNode.Primitive leafFor(OutputColumn column, ParquetRecordBatch base, int outputFieldId) {
        return switch (column) {
            case OutputColumn.Physical(ColumnPath name, ColumnPath source) ->
                renamed(sourceLeaf(base, source), name.name(), outputFieldId);
            case OutputColumn.Promoted(ColumnPath name, ColumnPath ignored, PrimitiveKind target) ->
                promotedLeaf(name.name(), target, outputFieldId);
            case OutputColumn.Constant(ColumnPath name, Value value) ->
                ConstantLeaves.primitiveFor(name.name(), value, outputFieldId);
            case OutputColumn.Null(ColumnPath name, Value typeOf) ->
                ConstantLeaves.primitiveFor(name.name(), typeOf, outputFieldId);
        };
    }

    private static SchemaNode.Primitive sourceLeaf(ParquetRecordBatch base, ColumnPath source) {
        SchemaNode node = base.projectedSchema()
                .find(source)
                .orElseThrow(() -> new IllegalArgumentException("no source leaf for " + source));
        if (node instanceof SchemaNode.Primitive primitive) {
            return primitive;
        }
        throw new IllegalArgumentException("source " + source + " is not a leaf column");
    }

    private static SchemaNode.Primitive renamed(SchemaNode.Primitive leaf, String outputName, int outputFieldId) {
        return new SchemaNode.Primitive(
                outputName, leaf.repetition(), leaf.kind(), leaf.typeLength(), leaf.logicalType(), outputFieldId);
    }

    private static SchemaNode.Primitive promotedLeaf(String outputName, PrimitiveKind target, int outputFieldId) {
        return new SchemaNode.Primitive(
                outputName, Repetition.OPTIONAL, target, OptionalInt.empty(), Optional.empty(), outputFieldId);
    }

    private static ParquetSchema schemaFrom(ParquetSchema baseSchema, List<SchemaNode> outputLeaves) {
        SchemaNode.Group baseRoot = baseSchema.root();
        SchemaNode.Group outputRoot = new SchemaNode.Group(
                baseRoot.name(),
                baseRoot.repetition(),
                List.copyOf(outputLeaves),
                baseRoot.logicalType(),
                baseRoot.fieldId());
        return new ParquetSchema(outputRoot);
    }
}
