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

import java.util.SequencedMap;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Recursive type descriptor the decoder follows to rebuild a {@code ColumnVector} from an {@link EncodedNode}. A flat
 * leaf type cannot name the children of a list, struct, map, or Variant column; this descriptor mirrors the nested
 * vector shape one level at a time and lets {@link ArrowBufferCodec#decode(EncodedNode, ColumnType)} recurse into each
 * child node with the matching child type.
 */
public sealed interface ColumnType
        permits ColumnType.Primitive,
                ColumnType.ListType,
                ColumnType.StructType,
                ColumnType.MapType,
                ColumnType.VariantType {

    /** A leaf column of one of the eight physical kinds the {@link LeafType} discriminator names. */
    record Primitive(LeafType leaf) implements ColumnType {}

    /** A list column whose every row is a slice of the single {@code element} child vector. */
    record ListType(ColumnType element) implements ColumnType {}

    /**
     * A struct column with an ordered set of named fields. The field order is the iteration order of {@code fields} and
     * must match the order the encoder walked the struct's children; the decoder pairs the i-th field with the i-th
     * child node. Both are the struct's declared field order.
     */
    record StructType(SequencedMap<ColumnPath, ColumnType> fields) implements ColumnType {}

    /** A map column laid out as Arrow's {@code List<Struct<key, value>>}. */
    record MapType(ColumnType key, ColumnType value) implements ColumnType {}

    /** A Parquet Variant column with metadata and value binary leaves. */
    record VariantType() implements ColumnType {}
}
