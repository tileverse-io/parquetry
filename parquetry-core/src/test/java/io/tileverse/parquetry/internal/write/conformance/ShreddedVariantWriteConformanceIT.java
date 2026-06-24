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
package io.tileverse.parquetry.internal.write.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.variant.Variant;
import io.tileverse.parquetry.data.variant.VariantEncoder;
import io.tileverse.parquetry.data.variant.VariantEncoder.Encoded;
import io.tileverse.parquetry.data.variant.VariantMetadata;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Conformance gate for shredded Variant write output. parquetry writes a file with one shredded Variant column
 * {@code optional group v (VARIANT) { required binary metadata; optional binary value; <typed_value> }}, the typed
 * subtree shredded into a scalar leaf, an object group, or an array list. Each shape is verified two ways: parquetry
 * reads every row back as a {@link Variant} that serializes byte-for-byte to what was authored (a null Variant row
 * reading back as null), and parquet-java opens the footer to confirm the shredded group structure is well-formed.
 *
 * <p>Rows under test cover a matching scalar, a type-mismatched scalar that falls back to the residual {@code value}
 * leaf, an object with both shredded and residual fields, an array of ints, and a null Variant column.
 */
@Tag("conformance")
class ShreddedVariantWriteConformanceIT {

    private static final ColumnPath V_COLUMN = ColumnPath.of("v");
    private static final ColumnPath ID_COLUMN = ColumnPath.of("id");

    @TempDir
    Path tempDir;

    @Test
    void scalarShreddingRoundTripsAndReadsCleanlyThroughParquetJava() throws Exception {
        ParquetSchema schema = schemaWith(scalarInt64TypedValue());
        List<Encoded> authored = new ArrayList<>();
        // A matching int64 scalar lands in the typed leaf.
        authored.add(new VariantEncoder().addLong(42L).encode());
        // A type-mismatched string scalar falls back to the residual value leaf.
        authored.add(new VariantEncoder().addString("not-a-long").encode());

        Path file = tempDir.resolve("scalar.parquet");
        writeVariantRowsThenNull(file, schema, authored);

        assertRoundTrip(file, authored);
        assertShreddedFooter(file, "optional int64 typed_value");
    }

    @Test
    void objectShreddingWithResidualFieldsRoundTrips() throws Exception {
        ParquetSchema schema = schemaWith(objectTwoScalarFieldsTypedValue());
        List<Encoded> authored = new ArrayList<>();
        // id and name match the shredded fields; extra spills to the residual value leaf.
        authored.add(new VariantEncoder()
                .startObject()
                .field("id")
                .addLong(7L)
                .field("name")
                .addString("alice")
                .field("extra")
                .addInt(99)
                .endObject()
                .encode());
        // An object that only populates one shredded field.
        authored.add(new VariantEncoder()
                .startObject()
                .field("name")
                .addString("bob")
                .endObject()
                .encode());

        Path file = tempDir.resolve("object.parquet");
        writeVariantRowsThenNull(file, schema, authored);

        assertRoundTrip(file, authored);
        assertShreddedFooter(file, "optional group typed_value");
    }

    @Test
    void arrayShreddingRoundTrips() throws Exception {
        ParquetSchema schema = schemaWith(arrayOfInt32TypedValue());
        List<Encoded> authored = new ArrayList<>();
        authored.add(new VariantEncoder()
                .startArray()
                .addInt(1)
                .addInt(2)
                .addInt(3)
                .endArray()
                .encode());
        // An empty array.
        authored.add(new VariantEncoder().startArray().endArray().encode());

        Path file = tempDir.resolve("array.parquet");
        writeVariantRowsThenNull(file, schema, authored);

        assertRoundTrip(file, authored);
        assertShreddedFooter(file, "repeated group list");
    }

    // --- shared write + assert ---

    private void writeVariantRowsThenNull(Path file, ParquetSchema schema, List<Encoded> authored) throws IOException {
        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            int id = 0;
            for (Encoded encoded : authored) {
                appender.setInt(ID_COLUMN, id++)
                        .setVariant(V_COLUMN, variantOf(encoded))
                        .endRow();
            }
            // A null Variant column.
            appender.setInt(ID_COLUMN, id).setNull(V_COLUMN).endRow();
        }
    }

    /**
     * Asserts each authored row reads back as a Variant whose serialized bytes match what was authored, plus a trailing
     * null Variant row. The serialized form is metadata-then-value, independent of how the row shredded, which makes it
     * a sound round-trip oracle across the residual and typed leaves.
     */
    private void assertRoundTrip(Path file, List<Encoded> authored) {
        List<ParquetRecord> rows = WriteConformanceSupport.readAll(file);
        assertThat(rows).hasSize(authored.size() + 1);

        for (int row = 0; row < authored.size(); row++) {
            Object value = rows.get(row).get(V_COLUMN);
            assertThat(value).as("row %d is a non-null Variant", row).isInstanceOf(Variant.class);
            assertThat(((Variant) value).serialize())
                    .as("row %d serializes to the authored bytes", row)
                    .isEqualTo(variantOf(authored.get(row)).serialize());
        }

        assertThat(rows.get(authored.size()).get(V_COLUMN))
                .as("the absent Variant row reads back as null")
                .isNull();
    }

    private void assertShreddedFooter(Path file, String typedValueFragment) throws IOException {
        ParquetMetadata footer = WriteConformanceSupport.readFooterViaParquetJava(file);
        MessageType messageType = footer.getFileMetaData().getSchema();
        String printed = messageType.toString();
        assertThat(printed).contains("optional group v (VARIANT");
        assertThat(printed).contains("required binary metadata");
        assertThat(printed).contains("optional binary value");
        assertThat(printed).contains(typedValueFragment);
    }

    // --- parquetry read-back ---

    private static Variant variantOf(Encoded encoded) {
        VariantMetadata metadata = new VariantMetadata(encoded.metadata());
        return Variant.of(encoded.value(), metadata);
    }

    // --- shredded schemas: message schema { required int32 id; optional group v (VARIANT) { ... } } ---

    private static ParquetSchema schemaWith(SchemaNode typedValue) {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group variant = variantGroup(typedValue);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, variant), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Group variantGroup(SchemaNode typedValue) {
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive value = optionalBinary("value");
        return new SchemaNode.Group(
                "v",
                Repetition.OPTIONAL,
                List.of(metadata, value, typedValue),
                Optional.of(new LogicalType.Variant()),
                -1);
    }

    private static SchemaNode.Primitive scalarInt64TypedValue() {
        return new SchemaNode.Primitive(
                "typed_value", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Group objectTwoScalarFieldsTypedValue() {
        SchemaNode.Group idField = scalarField("id", PrimitiveKind.INT64, Optional.empty());
        Optional<LogicalType> string = Optional.of(new LogicalType.StringType());
        SchemaNode.Group nameField = scalarField("name", PrimitiveKind.BYTE_ARRAY, string);
        return new SchemaNode.Group(
                "typed_value", Repetition.OPTIONAL, List.of(idField, nameField), Optional.empty(), -1);
    }

    private static SchemaNode.Group arrayOfInt32TypedValue() {
        SchemaNode.Primitive elementTyped = new SchemaNode.Primitive(
                "typed_value", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group element = new SchemaNode.Group(
                "element", Repetition.REQUIRED, List.of(optionalBinary("value"), elementTyped), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        return new SchemaNode.Group(
                "typed_value", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
    }

    private static SchemaNode.Group scalarField(String name, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        SchemaNode.Primitive typedValue = new SchemaNode.Primitive(
                "typed_value", Repetition.OPTIONAL, kind, OptionalInt.empty(), logicalType, -1);
        return new SchemaNode.Group(
                name, Repetition.REQUIRED, List.of(optionalBinary("value"), typedValue), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive optionalBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }
}
