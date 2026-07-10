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
package io.tileverse.parquetry.arrow.columnar;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Converts a whole {@link ParquetRecordBatch} to and from its Arrow buffer layout, one {@link EncodedNode} per
 * top-level column. The batch's {@code columns()} map holds both the wrapper vector at a nested column's group path and
 * the row-aligned leaves of a struct under their own paths; encoding walks the projected schema's top-level fields
 * instead, which selects each column's vector exactly once and in a deterministic order.
 *
 * <p>Decoding reverses the walk: it resolves each top-level field's recursive {@link ColumnType} from the schema (via
 * the nested {@link ColumnTypes}) and hands it to the {@link ArrowBufferCodec} together with the column's node. The
 * rebuilt vectors are heap-backed and outlive the batch, matching the read path's batch contract.
 */
public final class BatchArrowLayout {

    private BatchArrowLayout() {
        // utility
    }

    /** Encodes every top-level column of {@code batch} into an {@link EncodedBatch} node tree. */
    public static EncodedBatch encode(ParquetRecordBatch batch) {
        Map<ColumnPath, ColumnVector> columns = batch.columns();
        List<ColumnPath> columnPaths = new ArrayList<>();
        List<EncodedNode> nodes = new ArrayList<>();
        for (SchemaNode field : batch.projectedSchema().root().children()) {
            ColumnPath path = ColumnPath.of(field.name());
            ColumnVector vector = requireColumn(columns, path);
            columnPaths.add(path);
            nodes.add(ArrowBufferCodec.encode(vector));
        }
        return new EncodedBatch(batch.rowCount(), columnPaths, nodes);
    }

    /**
     * Decodes {@code encoded} back into a heap-backed {@link ParquetRecordBatch}. Each column's recursive type is
     * resolved from {@code schema}, which gives the codec the nested shape an {@link EncodedNode} does not record. The
     * {@code schema} must be the same projected schema the batch was encoded against; its top-level fields are looked
     * up by the encoded column paths.
     */
    public static ParquetRecordBatch decode(EncodedBatch encoded, ParquetSchema schema) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        List<ColumnPath> columnPaths = encoded.columns();
        List<EncodedNode> nodes = encoded.nodes();
        for (int i = 0; i < columnPaths.size(); i++) {
            ColumnPath path = columnPaths.get(i);
            ColumnType type = ColumnTypes.resolve(resolveField(schema, path));
            columns.put(path, ArrowBufferCodec.decode(nodes.get(i), type));
        }
        return DefaultParquetRecordBatch.ofHeap(schema, columns, encoded.rowCount());
    }

    private static ColumnVector requireColumn(Map<ColumnPath, ColumnVector> columns, ColumnPath path) {
        ColumnVector vector = columns.get(path);
        if (vector == null) {
            throw new IllegalStateException("Batch has no column vector for top-level column " + path.dot());
        }
        return vector;
    }

    private static SchemaNode resolveField(ParquetSchema schema, ColumnPath path) {
        return schema.find(path)
                .orElseThrow(() -> new IllegalStateException(
                        "Schema has no top-level column " + path.dot() + " to decode against"));
    }

    /**
     * Resolves a schema subtree into the recursive {@link ColumnType} descriptor the {@link ArrowBufferCodec} decoder
     * follows. The descriptor mirrors the shape of the column vector the read path assembles for the same subtree,
     * which lets a decoded {@link EncodedNode} be paired back with the structural information it does not record on its
     * own.
     *
     * <p>The group classification and the descent into list elements, map key/value entries, struct fields, and Variant
     * children follow the same rules the read-side assembler applies, which keeps the encoded form and the rebuilt
     * vector in agreement. A group's kind is read from its logical-type annotation when present (LIST / MAP / Variant),
     * otherwise from its repetition (a {@code REPEATED} group is a list, a non-repeated group is a struct).
     */
    private static final class ColumnTypes {

        private ColumnTypes() {
            // utility
        }

        /** Resolves the {@link ColumnType} for one top-level (or nested) schema field. */
        static ColumnType resolve(SchemaNode field) {
            return switch (field) {
                case SchemaNode.Primitive primitive -> new ColumnType.Primitive(leafType(primitive.kind()));
                case SchemaNode.Group group -> resolveGroup(group);
            };
        }

        private static ColumnType resolveGroup(SchemaNode.Group group) {
            return switch (classify(group)) {
                case LIST -> new ColumnType.ListType(resolve(listElement(group)));
                case MAP -> resolveMap(group);
                case STRUCT -> resolveStruct(group);
                case VARIANT -> new ColumnType.VariantType();
            };
        }

        /**
         * Resolves a struct's fields, keyed by the single-segment {@link ColumnPath} the read path uses for a
         * {@code StructVector}'s children. The fields are inserted in the group's declared child order, which is the
         * order the codec walks a struct's children on both encode and decode; keeping that order pairs each decoded
         * child node with its own field.
         */
        private static ColumnType resolveStruct(SchemaNode.Group group) {
            SequencedMap<ColumnPath, ColumnType> fields = new LinkedHashMap<>();
            for (SchemaNode child : group.children()) {
                fields.put(ColumnPath.of(child.name()), resolve(child));
            }
            return new ColumnType.StructType(fields);
        }

        private static ColumnType resolveMap(SchemaNode.Group group) {
            SchemaNode.Group keyValue = (SchemaNode.Group) group.children().get(0);
            SchemaNode keyNode = keyValue.children().get(0);
            SchemaNode valueNode = keyValue.children().get(1);
            return new ColumnType.MapType(resolve(keyNode), resolve(valueNode));
        }

        /**
         * The element field of a list group. The standard three-level encoding wraps the element in a repeated group
         * whose single child is the element; the legacy two-level encoding places a repeated primitive directly under
         * the list group, in which case that primitive is the element.
         */
        private static SchemaNode listElement(SchemaNode.Group listGroup) {
            SchemaNode repeated = listGroup.children().get(0);
            return switch (repeated) {
                case SchemaNode.Group wrapper -> wrapper.children().get(0);
                case SchemaNode.Primitive element -> element;
            };
        }

        private static GroupKind classify(SchemaNode.Group group) {
            LogicalType annotation = group.logicalType().orElse(null);
            return switch (annotation) {
                case null -> structuralKind(group);
                case LogicalType.Variant _ -> GroupKind.VARIANT;
                case LogicalType.ListType _ -> GroupKind.LIST;
                case LogicalType.MapType _ -> GroupKind.MAP;
                default ->
                    throw new ParquetSchemaException("Group %s has an unsupported logical-type annotation: %s"
                            .formatted(group.name(), annotation));
            };
        }

        /**
         * A group with no logical-type annotation: a repeated group is a list (legacy two-level), otherwise a struct.
         */
        private static GroupKind structuralKind(SchemaNode.Group group) {
            return group.repetition() == Repetition.REPEATED ? GroupKind.LIST : GroupKind.STRUCT;
        }

        private static LeafType leafType(PrimitiveKind kind) {
            return switch (kind) {
                case BOOLEAN -> LeafType.BOOLEAN;
                case INT32 -> LeafType.INT32;
                case INT64 -> LeafType.INT64;
                case INT96 -> LeafType.INT96;
                case FLOAT -> LeafType.FLOAT;
                case DOUBLE -> LeafType.DOUBLE;
                case BYTE_ARRAY -> LeafType.BINARY;
                case FIXED_LEN_BYTE_ARRAY -> LeafType.FIXED_LEN_BINARY;
            };
        }

        private enum GroupKind {
            LIST,
            MAP,
            STRUCT,
            VARIANT
        }
    }
}
