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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.ShreddedVariantVector;
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
 * Proves the shredded Variant write path without the writer: each authored Variant shreds into the column's typed
 * leaves and residual, freezes into a {@link ShreddedVariantVector}, and reconstructs back to the exact bytes it was
 * authored from. The round-trip oracle is {@code serialize()} equality, which exercises the accumulator, the freeze,
 * and the {@link ShreddedVariantVector.VariantInput} assembly together.
 */
class ShreddedVariantAccumulatorTest {

    @Test
    void shreddedScalarRoundTrips() {
        Variant variant = scalarLong(42L);

        ShreddedVariantVector vector = authorOne(scalarInt64Schema(), variant);

        assertRoundTrips(vector, 0, variant);
    }

    @Test
    void shreddedObjectRoundTripsShreddedAndResidualFields() {
        Variant variant = objectWithIdNameAndExtra(7L, "alice", true);

        ShreddedVariantVector vector = authorOne(objectIdNameSchema(), variant);

        assertRoundTrips(vector, 0, variant);
    }

    @Test
    void shreddedArrayRoundTrips() {
        Variant variant = arrayOfInts(10, 20, 30);

        ShreddedVariantVector vector = authorOne(arrayOfInt32Schema(), variant);

        assertRoundTrips(vector, 0, variant);
    }

    @Test
    void nullVariantRowFreezesAbsent() {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(scalarInt64Schema());
        builder.setNull(ColumnPath.of("v")).endRow();
        Variant present = scalarLong(99L);
        builder.setVariant("v", present).endRow();

        ShreddedVariantVector vector = column(builder.build());

        assertThat(vector.validity().isNull(0)).isTrue();
        assertThat(vector.get(0)).isNull();
        assertRoundTrips(vector, 1, present);
    }

    @Test
    void mismatchedScalarKeepsResidual() {
        Variant stringIntoLongSlot = scalarString("not a long");

        ShreddedVariantVector vector = authorOne(scalarInt64Schema(), stringIntoLongSlot);

        assertRoundTrips(vector, 0, stringIntoLongSlot);
    }

    @Test
    void severalRowsRoundTripTogether() {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(scalarInt64Schema());
        Variant matched = scalarLong(1L);
        Variant mismatched = scalarString("residual");
        builder.setVariant("v", matched).endRow();
        builder.setNull(ColumnPath.of("v")).endRow();
        builder.setVariant("v", mismatched).endRow();

        ShreddedVariantVector vector = column(builder.build());

        assertRoundTrips(vector, 0, matched);
        assertThat(vector.get(1)).isNull();
        assertRoundTrips(vector, 2, mismatched);
    }

    @Test
    void shreddedVariantUnderListIsRejected() {
        ParquetSchema schema = listOfShreddedVariantSchema();
        assertThatThrownBy(() -> ParquetRecordBatchBuilder.forSchema(schema))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("shredded Variant nested under a list or map is not supported");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void typedScalarRoundTrips(TypedScalarCase scalarCase) {
        Variant variant = scalarCase.variant();

        ShreddedVariantVector vector = authorOne(variantSchema(scalarCase.typedValue()), variant);

        assertRoundTrips(vector, 0, variant);
    }

    static List<TypedScalarCase> typedScalarRoundTrips() {
        return List.of(
                scalarCase("BOOLEAN", PrimitiveKind.BOOLEAN, Optional.empty(), variant(e -> e.addBoolean(true))),
                scalarCase(
                        "INT8",
                        PrimitiveKind.INT32,
                        Optional.of(new LogicalType.IntType((byte) 8, true)),
                        variant(e -> e.addByte((byte) -7))),
                scalarCase(
                        "INT16",
                        PrimitiveKind.INT32,
                        Optional.of(new LogicalType.IntType((byte) 16, true)),
                        variant(e -> e.addShort((short) -1234))),
                scalarCase("INT32", PrimitiveKind.INT32, Optional.empty(), variant(e -> e.addInt(123456))),
                scalarCase("INT64", PrimitiveKind.INT64, Optional.empty(), variant(e -> e.addLong(9876543210L))),
                scalarCase("FLOAT", PrimitiveKind.FLOAT, Optional.empty(), variant(e -> e.addFloat(3.5f))),
                scalarCase("DOUBLE", PrimitiveKind.DOUBLE, Optional.empty(), variant(e -> e.addDouble(2.718281828))),
                scalarCase(
                        "BINARY",
                        PrimitiveKind.BYTE_ARRAY,
                        Optional.empty(),
                        variant(e -> e.addBinary(bytes(1, 2, 3)))),
                scalarCase(
                        "STRING",
                        PrimitiveKind.BYTE_ARRAY,
                        Optional.of(new LogicalType.StringType()),
                        variant(e -> e.addString("hello"))),
                scalarCase(
                        "UUID",
                        PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                        16,
                        Optional.of(new LogicalType.UuidType()),
                        variant(e -> e.addUuid(UUID.fromString("12345678-1234-5678-1234-567812345678")))),
                scalarCase(
                        "DECIMAL4",
                        PrimitiveKind.INT32,
                        Optional.of(new LogicalType.Decimal(3, 9)),
                        variant(e -> e.addBigDecimal(new BigDecimal(BigInteger.valueOf(-12345), 3)))),
                scalarCase(
                        "DECIMAL8",
                        PrimitiveKind.INT64,
                        Optional.of(new LogicalType.Decimal(6, 18)),
                        variant(e -> e.addBigDecimal(new BigDecimal(BigInteger.valueOf(-987654321098L), 6)))),
                scalarCase(
                        "DECIMAL16",
                        PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                        16,
                        Optional.of(new LogicalType.Decimal(10, 38)),
                        variant(e ->
                                e.addBigDecimal(new BigDecimal(new BigInteger("-1234567890123456789012345"), 10)))),
                scalarCase(
                        "DATE",
                        PrimitiveKind.INT32,
                        Optional.of(new LogicalType.DateType()),
                        rawScalar(11, littleEndian(19876, 4))),
                scalarCase(
                        "TIME",
                        PrimitiveKind.INT64,
                        Optional.of(new LogicalType.Time(false, LogicalType.TimeUnit.MICROS)),
                        rawScalar(17, littleEndian(45_123_456_789L, 8))),
                scalarCase(
                        "TIMESTAMP_TZ_MICROS",
                        PrimitiveKind.INT64,
                        Optional.of(new LogicalType.Timestamp(true, LogicalType.TimeUnit.MICROS)),
                        rawScalar(12, littleEndian(1_700_000_000_000_000L, 8))),
                scalarCase(
                        "TIMESTAMP_NTZ_MICROS",
                        PrimitiveKind.INT64,
                        Optional.of(new LogicalType.Timestamp(false, LogicalType.TimeUnit.MICROS)),
                        rawScalar(13, littleEndian(1_700_000_000_000_000L, 8))),
                scalarCase(
                        "TIMESTAMP_TZ_NANOS",
                        PrimitiveKind.INT64,
                        Optional.of(new LogicalType.Timestamp(true, LogicalType.TimeUnit.NANOS)),
                        rawScalar(18, littleEndian(1_700_000_000_123_456_789L, 8))),
                scalarCase(
                        "TIMESTAMP_NTZ_NANOS",
                        PrimitiveKind.INT64,
                        Optional.of(new LogicalType.Timestamp(false, LogicalType.TimeUnit.NANOS)),
                        rawScalar(19, littleEndian(1_700_000_000_123_456_789L, 8))));
    }

    @Test
    void valuelessFieldRejectsNonConformingValue() {
        Variant variant = objectWithStringNamedId("not an int");

        ParquetSchema schema = objectWithValuelessTypedFieldSchema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        assertThatThrownBy(() -> builder.setVariant("v", variant))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("no value residual leaf");
    }

    @Test
    void valuelessFieldRoundTripsConformingValue() {
        Variant variant = objectWithLongNamedId(42L);

        ShreddedVariantVector vector = authorOne(objectWithValuelessTypedFieldSchema(), variant);

        assertRoundTrips(vector, 0, variant);
    }

    @Test
    void nestedObjectTypedFieldGivenScalarKeepsResidual() {
        Variant variant = objectWithScalarAddr();

        ShreddedVariantVector vector = authorOne(objectWithNestedObjectTypedFieldSchema(), variant);

        assertRoundTrips(vector, 0, variant);
    }

    @Test
    void nestedArrayTypedFieldGivenScalarKeepsResidual() {
        Variant variant = objectWithScalarAddr();

        ShreddedVariantVector vector = authorOne(objectWithNestedArrayTypedFieldSchema(), variant);

        assertRoundTrips(vector, 0, variant);
    }

    @Test
    void valuelessNestedObjectFieldRejectsScalar() {
        Variant variant = objectWithScalarAddr();

        ParquetSchema schema = objectWithValuelessNestedObjectTypedFieldSchema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        assertThatThrownBy(() -> builder.setVariant("v", variant))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("no value residual leaf");
    }

    // --- typed scalar fixtures ---

    record TypedScalarCase(String name, SchemaNode.Primitive typedValue, Variant variant) {
        @Override
        public String toString() {
            return name;
        }
    }

    private static TypedScalarCase scalarCase(
            String name, PrimitiveKind kind, Optional<LogicalType> logicalType, Variant variant) {
        return new TypedScalarCase(name, leaf("typed_value", kind, logicalType), variant);
    }

    private static TypedScalarCase scalarCase(
            String name, PrimitiveKind kind, int byteWidth, Optional<LogicalType> logicalType, Variant variant) {
        SchemaNode.Primitive typedValue = new SchemaNode.Primitive(
                "typed_value", Repetition.OPTIONAL, kind, OptionalInt.of(byteWidth), logicalType, -1);
        return new TypedScalarCase(name, typedValue, variant);
    }

    private interface EncoderStep {
        void apply(VariantEncoder encoder);
    }

    private static Variant variant(EncoderStep step) {
        VariantEncoder encoder = new VariantEncoder();
        step.apply(encoder);
        return variantOf(encoder);
    }

    /**
     * A Variant scalar the {@link VariantEncoder} has no verb for (date, time, timestamp), built from its canonical
     * value bytes: a one-byte primitive header holding the type id, then the little-endian payload, read against an
     * empty metadata dictionary.
     */
    private static Variant rawScalar(int typeId, byte[] payload) {
        byte[] valueBytes = new byte[1 + payload.length];
        valueBytes[0] = (byte) (typeId << 2);
        System.arraycopy(payload, 0, valueBytes, 1, payload.length);
        return Variant.of(MemorySegment.ofArray(valueBytes).asReadOnly(), emptyMetadata());
    }

    private static VariantMetadata emptyMetadata() {
        return new VariantMetadata(new VariantEncoder().addNull().encode().metadata());
    }

    private static byte[] littleEndian(long value, int width) {
        byte[] bytes = new byte[width];
        for (int i = 0; i < width; i++) {
            bytes[i] = (byte) ((value >>> (8 * i)) & 0xFF);
        }
        return bytes;
    }

    private static MemorySegment bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return MemorySegment.ofArray(out).asReadOnly();
    }

    // --- value-less field fixtures (F1) ---

    private static Variant objectWithStringNamedId(String value) {
        return variantOf(
                new VariantEncoder().startObject().field("id").addString(value).endObject());
    }

    private static Variant objectWithLongNamedId(long value) {
        return variantOf(
                new VariantEncoder().startObject().field("id").addLong(value).endObject());
    }

    /**
     * A shredded object whose single {@code id} field declares only a {@code typed_value} (INT64) and no {@code value}
     * residual leaf. A non-conforming value for {@code id} has nowhere to be stored, the case F1 must reject.
     */
    private static ParquetSchema objectWithValuelessTypedFieldSchema() {
        SchemaNode.Primitive idTyped = leaf("typed_value", PrimitiveKind.INT64, Optional.empty());
        SchemaNode.Group idField = group("id", Repetition.REQUIRED, List.of(idTyped), Optional.empty());
        SchemaNode.Group typedValue = group("typed_value", Repetition.OPTIONAL, List.of(idField), Optional.empty());
        return variantSchema(typedValue);
    }

    // --- nested-typed-field fixtures ---

    /**
     * A shredded object whose single {@code addr} field has a {@code value} residual leaf and a {@code typed_value}
     * that is itself a nested object (one {@code street} string field). A scalar authored into {@code addr} does not
     * match the nested object and must be lifted to the field's {@code value} leaf.
     */
    private static ParquetSchema objectWithNestedObjectTypedFieldSchema() {
        return variantSchema(objectTypedValueWith(addrField(nestedObjectTypedValue())));
    }

    /**
     * Like {@link #objectWithNestedObjectTypedFieldSchema()} but the {@code addr} field's {@code typed_value} is a
     * nested array of INT32 elements.
     */
    private static ParquetSchema objectWithNestedArrayTypedFieldSchema() {
        return variantSchema(objectTypedValueWith(addrField(nestedArrayTypedValue())));
    }

    /**
     * Like {@link #objectWithNestedObjectTypedFieldSchema()} but the {@code addr} field has no {@code value} residual
     * leaf. A scalar authored into {@code addr} has nowhere to be stored and must be rejected.
     */
    private static ParquetSchema objectWithValuelessNestedObjectTypedFieldSchema() {
        SchemaNode.Group addrField =
                group("addr", Repetition.REQUIRED, List.of(nestedObjectTypedValue()), Optional.empty());
        return variantSchema(objectTypedValueWith(addrField));
    }

    private static SchemaNode.Group objectTypedValueWith(SchemaNode.Group field) {
        return group("typed_value", Repetition.OPTIONAL, List.of(field), Optional.empty());
    }

    private static SchemaNode.Group addrField(SchemaNode.Group typedValue) {
        SchemaNode.Primitive value = leaf("value", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        return group("addr", Repetition.REQUIRED, List.of(value, typedValue), Optional.empty());
    }

    private static SchemaNode.Group nestedObjectTypedValue() {
        SchemaNode.Group streetField =
                scalarField("street", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
        return group("typed_value", Repetition.OPTIONAL, List.of(streetField), Optional.empty());
    }

    private static SchemaNode.Group nestedArrayTypedValue() {
        SchemaNode.Group element = valueAndTypedValue(
                "element", Repetition.REQUIRED, leaf("typed_value", PrimitiveKind.INT32, Optional.empty()));
        SchemaNode.Group repeated = group("list", Repetition.REPEATED, List.of(element), Optional.empty());
        return group("typed_value", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()));
    }

    // --- round-trip helpers ---

    private static ShreddedVariantVector authorOne(ParquetSchema schema, Variant variant) {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        builder.setVariant("v", variant).endRow();
        return column(builder.build());
    }

    private static ShreddedVariantVector column(ParquetRecordBatch batch) {
        return (ShreddedVariantVector) batch.columns().get(ColumnPath.of("v"));
    }

    private static void assertRoundTrips(ShreddedVariantVector vector, int row, Variant expected) {
        Variant reconstructed = vector.get(row);
        assertThat(reconstructed).isNotNull();
        assertThat(reconstructed.serialize()).isEqualTo(expected.serialize());
    }

    // --- Variant fixtures ---

    private static Variant scalarLong(long value) {
        return variantOf(new VariantEncoder().addLong(value));
    }

    private static Variant scalarString(String value) {
        return variantOf(new VariantEncoder().addString(value));
    }

    private static Variant objectWithIdNameAndExtra(long id, String name, boolean extra) {
        VariantEncoder encoder = new VariantEncoder()
                .startObject()
                .field("id")
                .addLong(id)
                .field("name")
                .addString(name)
                .field("active")
                .addBoolean(extra)
                .endObject();
        return variantOf(encoder);
    }

    private static Variant objectWithScalarAddr() {
        return variantOf(
                new VariantEncoder().startObject().field("addr").addInt(5).endObject());
    }

    private static Variant arrayOfInts(int... values) {
        VariantEncoder encoder = new VariantEncoder().startArray();
        for (int value : values) {
            encoder.addInt(value);
        }
        encoder.endArray();
        return variantOf(encoder);
    }

    private static Variant variantOf(VariantEncoder encoder) {
        VariantEncoder.Encoded encoded = encoder.encode();
        return Variant.of(encoded.value(), new VariantMetadata(encoded.metadata()));
    }

    // --- schema fixtures ---

    private static ParquetSchema scalarInt64Schema() {
        SchemaNode.Primitive typedValue = leaf("typed_value", PrimitiveKind.INT64, Optional.empty());
        return variantSchema(typedValue);
    }

    private static ParquetSchema objectIdNameSchema() {
        SchemaNode.Group idField = scalarField("id", PrimitiveKind.INT64, Optional.empty());
        SchemaNode.Group nameField =
                scalarField("name", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
        SchemaNode.Group typedValue =
                group("typed_value", Repetition.OPTIONAL, List.of(idField, nameField), Optional.empty());
        return variantSchema(typedValue);
    }

    private static ParquetSchema arrayOfInt32Schema() {
        SchemaNode.Group element = valueAndTypedValue(
                "element", Repetition.REQUIRED, leaf("typed_value", PrimitiveKind.INT32, Optional.empty()));
        SchemaNode.Group repeated = group("list", Repetition.REPEATED, List.of(element), Optional.empty());
        SchemaNode.Group typedValue =
                group("typed_value", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()));
        return variantSchema(typedValue);
    }

    private static ParquetSchema listOfShreddedVariantSchema() {
        SchemaNode.Group variant = variantGroup("element", leaf("typed_value", PrimitiveKind.INT64, Optional.empty()));
        SchemaNode.Group repeated = group("list", Repetition.REPEATED, List.of(variant), Optional.empty());
        SchemaNode.Group listGroup =
                group("v", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()));
        return rootOf(listGroup);
    }

    private static SchemaNode.Group scalarField(String name, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return valueAndTypedValue(name, Repetition.REQUIRED, leaf("typed_value", kind, logicalType));
    }

    private static SchemaNode.Group valueAndTypedValue(
            String name, Repetition repetition, SchemaNode.Primitive typedValue) {
        SchemaNode.Primitive value = leaf("value", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        return group(name, repetition, List.of(value, typedValue), Optional.empty());
    }

    private static ParquetSchema variantSchema(SchemaNode typedValue) {
        return rootOf(variantGroup("v", typedValue));
    }

    private static SchemaNode.Group variantGroup(String name, SchemaNode typedValue) {
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive value = leaf("value", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        return group(
                name,
                Repetition.OPTIONAL,
                List.of(metadata, value, typedValue),
                Optional.of(new LogicalType.Variant()));
    }

    private static SchemaNode.Group group(
            String name, Repetition repetition, List<SchemaNode> children, Optional<LogicalType> logicalType) {
        return new SchemaNode.Group(name, repetition, children, logicalType, -1);
    }

    private static SchemaNode.Primitive leaf(String name, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logicalType, -1);
    }

    private static ParquetSchema rootOf(SchemaNode topLevel) {
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(topLevel), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
