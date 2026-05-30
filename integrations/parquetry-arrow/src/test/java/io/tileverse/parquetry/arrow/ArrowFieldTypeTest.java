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
package io.tileverse.parquetry.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ArrowFieldTypeTest {

    private static SchemaNode.Primitive leaf(PrimitiveKind kind, Optional<LogicalType> logical) {
        return new SchemaNode.Primitive("c", Repetition.OPTIONAL, kind, OptionalInt.empty(), logical, 0);
    }

    @Test
    void mapsInt32ToSignedInt32() {
        ArrowFieldType type = ArrowFieldType.of(leaf(PrimitiveKind.INT32, Optional.empty()));
        assertThat(type.kind()).isEqualTo(ArrowFieldType.Kind.INT);
        assertThat(type.bitWidth()).isEqualTo(32);
    }

    @Test
    void mapsByteArrayWithStringLogicalToUtf8() {
        ArrowFieldType type =
                ArrowFieldType.of(leaf(PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType())));
        assertThat(type.kind()).isEqualTo(ArrowFieldType.Kind.UTF8);
    }

    @Test
    void mapsBareByteArrayToBinary() {
        ArrowFieldType type = ArrowFieldType.of(leaf(PrimitiveKind.BYTE_ARRAY, Optional.empty()));
        assertThat(type.kind()).isEqualTo(ArrowFieldType.Kind.BINARY);
    }

    @Test
    void rejectsInt96() {
        SchemaNode.Primitive int96Leaf = leaf(PrimitiveKind.INT96, Optional.empty());
        assertThatThrownBy(() -> ArrowFieldType.of(int96Leaf))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("INT96");
    }

    @Test
    void rejectsDecimal() {
        SchemaNode.Primitive decimalLeaf = leaf(PrimitiveKind.INT32, Optional.of(new LogicalType.Decimal(2, 9)));
        assertThatThrownBy(() -> ArrowFieldType.of(decimalLeaf))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("DECIMAL");
    }
}
