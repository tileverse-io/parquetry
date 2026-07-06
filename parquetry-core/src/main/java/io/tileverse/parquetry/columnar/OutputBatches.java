/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Produces a read's result columns from a decoded physical batch, and selects a subset by name.
 *
 * <p>{@link #produce} reshapes a decoded batch into a {@link Projection.Of} produce set: each {@link Projection.Column}
 * passes a physical column through (renamed, reordered, or dropped), widens one to a broader primitive type, fills a
 * constant for every row, or adds an all-null column. {@link #select} narrows and reorders an already-produced batch to
 * a list of output names. Physical passthrough reuses the decoded vector zero-copy; closing the produced or selected
 * batch closes the base it was built from.
 *
 * <p>The produced schema is name-addressed, not field-id-addressed, hence every produced leaf gets a fresh synthetic
 * field id (its position in the produce/select order). Two columns sourced from one physical column therefore stay
 * distinct. Duplicate names are rejected.
 */
public final class OutputBatches {

    private OutputBatches() {}

    /** The produce-ordered schema for {@code of}, with leaf types resolved against {@code sourceSchema}. */
    public static ParquetSchema producedSchema(ParquetSchema sourceSchema, Projection.Of of) {
        List<SchemaNode> leaves = new ArrayList<>(of.columns().size());
        int fieldId = 0;
        for (Projection.Column column : of.columns()) {
            leaves.add(leafFor(column, sourceSchema, fieldId++));
        }
        return schemaFrom(sourceSchema, leaves);
    }

    /**
     * The produced batch built from {@code base} per {@code of}, in produce order, under the precomputed
     * {@code producedSchema} (see {@link #producedSchema(ParquetSchema, Projection.Of)}).
     */
    public static ParquetRecordBatch produce(ParquetRecordBatch base, Projection.Of of, ParquetSchema producedSchema) {
        Map<ColumnPath, ColumnVector> byName =
                LinkedHashMap.newLinkedHashMap(of.columns().size());
        for (Projection.Column column : of.columns()) {
            byName.put(column.name(), vectorFor(column, base));
        }
        DefaultParquetRecordBatch result = DefaultParquetRecordBatch.ofHeap(producedSchema, byName, base.rowCount());
        result.attachReleaseAction(base::close);
        return result;
    }

    /** Narrows and reorders {@code produced}'s columns to {@code outputColumns} by name (fresh field ids). */
    public static ParquetRecordBatch select(ParquetRecordBatch produced, List<ColumnPath> outputColumns) {
        Map<ColumnPath, ColumnVector> byName = LinkedHashMap.newLinkedHashMap(outputColumns.size());
        List<SchemaNode> leaves = new ArrayList<>(outputColumns.size());
        for (int fieldId = 0; fieldId < outputColumns.size(); fieldId++) {
            ColumnPath name = outputColumns.get(fieldId);
            ColumnVector vector = produced.columns().get(name);
            if (vector == null) {
                throw new IllegalArgumentException("output column not produced: " + name);
            }
            rejectDuplicateName(byName.put(name, vector), name);
            leaves.add(renamed(sourceLeaf(produced.projectedSchema(), name), name.name(), fieldId));
        }
        ParquetSchema schema = schemaFrom(produced.projectedSchema(), leaves);
        DefaultParquetRecordBatch result = DefaultParquetRecordBatch.ofHeap(schema, byName, produced.rowCount());
        result.attachReleaseAction(produced::close);
        return result;
    }

    private static void rejectDuplicateName(ColumnVector previous, ColumnPath name) {
        if (previous != null) {
            throw new IllegalArgumentException("duplicate output column name " + name);
        }
    }

    private static ColumnVector vectorFor(Projection.Column column, ParquetRecordBatch base) {
        return switch (column) {
            case Projection.Column.Physical(ColumnPath ignored, ColumnPath source) ->
                base.columns().get(source);
            case Projection.Column.Promoted(ColumnPath ignored, ColumnPath source, PrimitiveKind target) ->
                VectorWidening.widen(base.columns().get(source), target);
            case Projection.Column.Constant(ColumnPath ignored, Value value) ->
                ConstantVectors.of(value, base.rowCount());
            case Projection.Column.Null(ColumnPath ignored, Value typeOf) ->
                ConstantVectors.ofNull(typeOf, base.rowCount());
            case Projection.Column.RowPosition(ColumnPath name, long _) -> synthesizedVector(name, base);
            case Projection.Column.Coalesce(ColumnPath name, ColumnPath _, Projection.Column.Coalesce.Fallback _) ->
                synthesizedVector(name, base);
        };
    }

    private static ColumnVector synthesizedVector(ColumnPath name, ParquetRecordBatch base) {
        ColumnVector synthesized = base.columns().get(name);
        if (synthesized == null) {
            throw new IllegalStateException("synthesized column not built: " + name);
        }
        return synthesized;
    }

    private static SchemaNode.Primitive leafFor(Projection.Column column, ParquetSchema sourceSchema, int fieldId) {
        return switch (column) {
            case Projection.Column.Physical(ColumnPath name, ColumnPath source) ->
                renamed(sourceLeaf(sourceSchema, source), name.name(), fieldId);
            case Projection.Column.Promoted(ColumnPath name, ColumnPath ignored, PrimitiveKind target) ->
                promotedLeaf(name.name(), target, fieldId);
            case Projection.Column.Constant(ColumnPath name, Value value) ->
                ConstantLeaves.primitiveFor(name.name(), value, fieldId);
            case Projection.Column.Null(ColumnPath name, Value typeOf) ->
                ConstantLeaves.primitiveFor(name.name(), typeOf, fieldId);
            case Projection.Column.RowPosition(ColumnPath name, long _) -> rowPositionLeaf(name.name(), fieldId);
            // The coalesced column is a never-null INT64: the fallback fills every cell the source leaves null.
            case Projection.Column.Coalesce(ColumnPath name, ColumnPath _, Projection.Column.Coalesce.Fallback _) ->
                rowPositionLeaf(name.name(), fieldId);
        };
    }

    /** The schema leaf for a synthesized row-position column: a never-null {@code INT64} top-level column. */
    public static SchemaNode.Primitive rowPositionLeaf(String outputName, int outputFieldId) {
        return new SchemaNode.Primitive(
                outputName,
                Repetition.REQUIRED,
                PrimitiveKind.INT64,
                OptionalInt.empty(),
                Optional.empty(),
                outputFieldId);
    }

    private static SchemaNode.Primitive sourceLeaf(ParquetSchema sourceSchema, ColumnPath source) {
        SchemaNode node = sourceSchema
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
