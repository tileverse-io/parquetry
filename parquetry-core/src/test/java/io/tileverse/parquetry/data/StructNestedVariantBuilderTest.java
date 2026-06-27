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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.variant.VariantEncoder;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.Variant;
import io.tileverse.parquetry.variant.VariantMetadata;

/**
 * Proves a Variant column nested inside a struct is authorable with {@code setVariant}, symmetric with the nested-list
 * and nested-map verbs. Authoring a struct-nested unshredded Variant marks the enclosing struct present and stages the
 * Variant bytes into the nested column; the round-trip oracle is the reconstructed Variant's {@code serialize()}
 * equality.
 */
class StructNestedVariantBuilderTest {

    @Test
    void structNestedUnshreddedVariantRoundTrips() {
        ParquetSchema schema = personWithNestedVariantSchema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        Variant variant = variantOf(new VariantEncoder().addInt(42));
        builder.beginStruct("person")
                .setString(ColumnPath.of("person", "name"), "alice")
                .setVariant(ColumnPath.of("person", "payload"), variant)
                .endStruct()
                .endRow();

        ParquetRecordBatch batch = builder.build();

        StructVector person = (StructVector) batch.columns().get(ColumnPath.of("person"));
        assertThat(person.validity().isNull(0))
                .as("the authored struct is present")
                .isFalse();

        BinaryVector name = (BinaryVector) person.children().get(ColumnPath.of("name"));
        assertThat(stringOf(name, 0)).isEqualTo("alice");

        VariantVector payload = (VariantVector) person.children().get(ColumnPath.of("payload"));
        Variant reconstructed = payload.get(0);
        assertThat(reconstructed).isNotNull();
        assertThat(reconstructed.serialize()).isEqualTo(variant.serialize());
    }

    private static String stringOf(BinaryVector vector, int row) {
        MemorySegment segment = vector.get(row);
        byte[] bytes = segment.toArray(ValueLayout.JAVA_BYTE);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static Variant variantOf(VariantEncoder encoder) {
        VariantEncoder.Encoded encoded = encoder.encode();
        return Variant.of(encoded.value(), new VariantMetadata(encoded.metadata()));
    }

    /**
     * {@code optional group person { required binary name (STRING); optional group payload (VARIANT) { required binary
     * metadata; optional binary value; } }} - an unshredded Variant (no typed_value child) nested inside a struct.
     */
    private static ParquetSchema personWithNestedVariantSchema() {
        SchemaNode.Primitive name = new SchemaNode.Primitive(
                "name",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group payload = new SchemaNode.Group(
                "payload", Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), -1);
        SchemaNode.Group person =
                new SchemaNode.Group("person", Repetition.OPTIONAL, List.of(name, payload), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(person), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
