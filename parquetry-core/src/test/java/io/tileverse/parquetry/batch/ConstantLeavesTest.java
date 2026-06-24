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
package io.tileverse.parquetry.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Pins the shared {@code Value}-to-leaf mapping to the exact results the two former synthesis switches (the
 * materialized batch leaf in {@code ConstantColumnBatches} and the advertised schema leaf in {@code HivePartitioning})
 * produced for each value.
 */
class ConstantLeavesTest {

    @Test
    void intValMapsToInt32() {
        assertKindAndLogicalType(new Value.IntVal(1), PrimitiveKind.INT32, Optional.empty());
    }

    @Test
    void longValMapsToInt64() {
        assertKindAndLogicalType(new Value.LongVal(1L), PrimitiveKind.INT64, Optional.empty());
    }

    @Test
    void doubleValMapsToDouble() {
        assertKindAndLogicalType(new Value.DoubleVal(1.0), PrimitiveKind.DOUBLE, Optional.empty());
    }

    @Test
    void floatValMapsToFloat() {
        assertKindAndLogicalType(new Value.FloatVal(1.0f), PrimitiveKind.FLOAT, Optional.empty());
    }

    @Test
    void boolValMapsToBoolean() {
        assertKindAndLogicalType(new Value.BoolVal(true), PrimitiveKind.BOOLEAN, Optional.empty());
    }

    @Test
    void dateValMapsToInt32WithDateLogicalType() {
        assertKindAndLogicalType(
                new Value.DateVal(LocalDate.of(2026, 6, 21)),
                PrimitiveKind.INT32,
                Optional.of(new LogicalType.DateType()));
    }

    @Test
    void stringValMapsToByteArrayWithStringLogicalType() {
        assertKindAndLogicalType(
                new Value.StringVal("x"), PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
    }

    @Test
    void primitiveForBuildsAnOptionalLeafWithGivenNameAndFieldId() {
        SchemaNode.Primitive leaf = ConstantLeaves.primitiveFor("c", new Value.StringVal("x"), 7);
        assertThat(leaf.name()).isEqualTo("c");
        assertThat(leaf.repetition()).isEqualTo(Repetition.OPTIONAL);
        assertThat(leaf.typeLength()).isEqualTo(OptionalInt.empty());
        assertThat(leaf.fieldId()).isEqualTo(7);
    }

    @Test
    void unsupportedValueIsRejectedTheSameWayTheOldSwitchRejectedIt() {
        Value unsupported = new Value.BinaryVal(java.lang.foreign.MemorySegment.ofArray(new byte[] {1}));
        assertThatThrownBy(() -> ConstantLeaves.primitiveFor("c", unsupported, 7))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertKindAndLogicalType(
            Value value, PrimitiveKind expectedKind, Optional<LogicalType> expectedLogicalType) {
        SchemaNode.Primitive leaf = ConstantLeaves.primitiveFor("c", value, 7);
        assertThat(leaf.kind()).isEqualTo(expectedKind);
        assertThat(leaf.logicalType()).isEqualTo(expectedLogicalType);

        ConstantLeaves.LeafType leafType = ConstantLeaves.kindAndLogicalType(value);
        assertThat(leafType.kind()).isEqualTo(expectedKind);
        assertThat(leafType.logicalType()).isEqualTo(expectedLogicalType);
    }
}
