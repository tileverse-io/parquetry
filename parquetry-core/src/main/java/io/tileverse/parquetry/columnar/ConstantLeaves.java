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

import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The single mapping from a constant {@link Value} to the Parquet schema leaf that represents it. A dataset both
 * advertises a schema leaf for a constant column and materializes a batch leaf of the same value; routing both through
 * this one switch keeps the advertised leaf and the materialized leaf from drifting in physical kind or logical type.
 */
public final class ConstantLeaves {

    private ConstantLeaves() {}

    /** The physical {@link PrimitiveKind} and optional {@link LogicalType} a constant {@link Value} maps to. */
    public record LeafType(PrimitiveKind kind, Optional<LogicalType> logicalType) {}

    /** An {@link Repetition#OPTIONAL} leaf for {@code value} with the given {@code name} and {@code fieldId}. */
    public static SchemaNode.Primitive primitiveFor(String name, Value value, int fieldId) {
        LeafType leafType = kindAndLogicalType(value);
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, leafType.kind(), OptionalInt.empty(), leafType.logicalType(), fieldId);
    }

    /** The physical kind and logical type for {@code value}, for callers building a leaf with their own name and id. */
    public static LeafType kindAndLogicalType(Value value) {
        return switch (value) {
            case Value.IntVal ignored -> new LeafType(PrimitiveKind.INT32, Optional.empty());
            case Value.LongVal ignored -> new LeafType(PrimitiveKind.INT64, Optional.empty());
            case Value.DoubleVal ignored -> new LeafType(PrimitiveKind.DOUBLE, Optional.empty());
            case Value.FloatVal ignored -> new LeafType(PrimitiveKind.FLOAT, Optional.empty());
            case Value.BoolVal ignored -> new LeafType(PrimitiveKind.BOOLEAN, Optional.empty());
            case Value.DateVal ignored -> new LeafType(PrimitiveKind.INT32, Optional.of(new LogicalType.DateType()));
            case Value.StringVal ignored ->
                new LeafType(PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
            default -> throw new IllegalArgumentException("unsupported constant column value " + value);
        };
    }
}
