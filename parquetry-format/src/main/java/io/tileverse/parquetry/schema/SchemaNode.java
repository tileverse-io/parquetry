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
package io.tileverse.parquetry.schema;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.LogicalType;

/**
 * A node in a Parquet schema tree.
 *
 * <p>Exactly two cases:
 *
 * <ul>
 *   <li>{@link Primitive} - a leaf column with a physical {@link PrimitiveKind}, an optional logical-type annotation,
 *       and (for {@code FIXED_LEN_BYTE_ARRAY}) a byte length.
 *   <li>{@link Group} - an interior node grouping child {@code SchemaNode}s. The root of every Parquet schema is a
 *       {@code Group}; nested groups model {@code LIST} / {@code MAP} / arbitrary structs.
 * </ul>
 *
 * <p>The schema is built once from the file footer's flat list of Thrift {@code SchemaElement}s (see
 * {@code SchemaBuilder}) and remains immutable for the lifetime of the dataset. {@link ParquetSchema} wraps the root
 * {@code Group} and indexes leaves by {@link ColumnPath}; callers look up a node by its column path through
 * {@link ParquetSchema#find(ColumnPath)}.
 *
 * <p>Consumers typically pattern-match on the case to drive type-dependent behavior:
 *
 * <pre>{@code
 * SchemaNode node = schema.find(path).orElseThrow();
 * switch (node) {
 *     case SchemaNode.Primitive p -> emitColumn(p);
 *     case SchemaNode.Group g     -> walk(g.children());
 * }
 * }</pre>
 *
 * <p>{@link #fieldId()} is a stable per-schema identifier the writer assigns and the reader carries through. It is
 * independent of the column path; downstream pipelines that rename a column keep the same id. {@link #repetition()} is
 * the Parquet repetition kind (REQUIRED / OPTIONAL / REPEATED). {@link #logicalType()} is the optional annotation that
 * lifts the physical kind into a logical type (STRING, DATE, DECIMAL, GEOMETRY, etc.).
 *
 * <p>Read-side consumers: {@code SchemaBuilder} constructs the tree; {@code PageDecoder} / {@code BatchColumnReader} /
 * {@code Materializer} traverse it to drive decoding. Write-side consumers: {@code ColumnChunkWriter} /
 * {@code RowGroupWriter} / {@code ParquetFileWriter} traverse the caller-supplied tree and emit one column chunk per
 * {@link Primitive} leaf.
 */
public sealed interface SchemaNode permits SchemaNode.Primitive, SchemaNode.Group {

    String name();

    Repetition repetition();

    Optional<LogicalType> logicalType();

    int fieldId();

    /**
     * A leaf column with a physical encoding type and an optional logical type annotation.
     *
     * @param typeLength byte length for {@link PrimitiveKind#FIXED_LEN_BYTE_ARRAY}; empty otherwise.
     */
    record Primitive(
            String name,
            Repetition repetition,
            PrimitiveKind kind,
            OptionalInt typeLength,
            Optional<LogicalType> logicalType,
            int fieldId)
            implements SchemaNode {}

    /**
     * An interior node grouping child fields.
     *
     * <p>The children list is defensively copied and immutable after construction.
     */
    record Group(
            String name,
            Repetition repetition,
            List<SchemaNode> children,
            Optional<LogicalType> logicalType,
            int fieldId)
            implements SchemaNode {

        public Group {
            children = List.copyOf(children);
        }
    }
}
