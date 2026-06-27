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
package io.tileverse.parquetry.variant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ShreddedVariantClassifierTest {

    @Test
    void requiredObjectFieldGroupClassifiesAsAShreddedObject() {
        SchemaNode.Group variantGroup = variantGroupWithObjectField(Repetition.REQUIRED);

        ShreddedVariant classified = ShreddedVariantClassifier.classify(variantGroup);

        assertThat(classified).isInstanceOf(ShreddedVariant.ShreddedObject.class);
    }

    @Test
    void optionalObjectFieldGroupIsRejected() {
        SchemaNode.Group variantGroup = variantGroupWithObjectField(Repetition.OPTIONAL);

        assertThatThrownBy(() -> ShreddedVariantClassifier.classify(variantGroup))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("must be a required group");
    }

    private static SchemaNode.Group variantGroupWithObjectField(Repetition fieldRepetition) {
        SchemaNode.Group field = objectField("a", fieldRepetition);
        SchemaNode.Group typedValue =
                new SchemaNode.Group("typed_value", Repetition.OPTIONAL, List.of(field), Optional.empty(), -1);
        return new SchemaNode.Group("var", Repetition.OPTIONAL, List.of(typedValue), Optional.empty(), -1);
    }

    private static SchemaNode.Group objectField(String name, Repetition repetition) {
        SchemaNode.Primitive value = optionalBinaryLeaf("value");
        SchemaNode.Primitive typedValue = new SchemaNode.Primitive(
                "typed_value", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        return new SchemaNode.Group(name, repetition, List.of(value, typedValue), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive optionalBinaryLeaf(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.<LogicalType>empty(),
                -1);
    }
}
