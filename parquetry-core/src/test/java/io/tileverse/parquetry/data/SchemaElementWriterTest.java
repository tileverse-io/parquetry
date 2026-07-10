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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.ConvertedType;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The footer schema must record the deprecated {@code converted_type} alongside the modern {@code logicalType} for
 * every kind that predates the logical-type union. Readers that honor only the legacy annotation (duckdb, older
 * parquet-mr) otherwise see a string as raw binary and a LIST/MAP group as its bare intermediate repeated group.
 */
class SchemaElementWriterTest {

    @Test
    void backfillsConvertedTypeForLegacyMappableKinds() {
        List<SchemaElement> elements = SchemaElementWriter.flatten(sampleSchema());

        assertThat(convertedTypeOf(elements, "id")).contains(ConvertedType.UTF8);
        assertThat(convertedTypeOf(elements, "tags")).contains(ConvertedType.LIST);
        assertThat(convertedTypeOf(elements, "props")).contains(ConvertedType.MAP);
        assertThat(convertedTypeOf(elements, "small")).contains(ConvertedType.UINT_16);
        assertThat(convertedTypeOf(elements, "day")).contains(ConvertedType.DATE);
        assertThat(convertedTypeOf(elements, "ts")).contains(ConvertedType.TIMESTAMP_MICROS);
    }

    @Test
    void recordsDecimalScaleAndPrecisionForTheLegacyReader() {
        List<SchemaElement> elements = SchemaElementWriter.flatten(sampleSchema());

        SchemaElement price = elementNamed(elements, "price");
        assertThat(price.convertedType()).contains(ConvertedType.DECIMAL);
        assertThat(price.scale()).hasValue(2);
        assertThat(price.precision()).hasValue(9);
    }

    @Test
    void leavesConvertedTypeEmptyForKindsWithoutALegacyEquivalent() {
        List<SchemaElement> elements = SchemaElementWriter.flatten(sampleSchema());

        // No logical type at all.
        assertThat(convertedTypeOf(elements, "raw")).isEmpty();
        // Local-time timestamp: the legacy TIMESTAMP_* only ever meant UTC-adjusted.
        assertThat(convertedTypeOf(elements, "tsLocal")).isEmpty();
        // UUID postdates the converted-type enum.
        SchemaElement uid = elementNamed(elements, "uid");
        assertThat(uid.convertedType()).isEmpty();
        assertThat(uid.logicalType()).contains(new LogicalType.UuidType());
        // The LIST/MAP intermediate repeated groups stay unannotated, matching modern writers.
        assertThat(convertedTypeOf(elements, "list")).isEmpty();
        assertThat(convertedTypeOf(elements, "key_value")).isEmpty();
    }

    @Test
    void stillEmitsTheModernLogicalTypeNextToTheBackfilledConvertedType() {
        List<SchemaElement> elements = SchemaElementWriter.flatten(sampleSchema());

        SchemaElement id = elementNamed(elements, "id");
        assertThat(id.convertedType()).contains(ConvertedType.UTF8);
        assertThat(id.logicalType()).contains(new LogicalType.StringType());
    }

    private static ParquetSchema sampleSchema() {
        SchemaNode.Primitive id = string("id");
        SchemaNode.Group tags = group("tags", new LogicalType.ListType(), group("list", null, string("element")));
        SchemaNode.Group props =
                group("props", new LogicalType.MapType(), group("key_value", null, string("key"), string("value")));
        SchemaNode.Primitive price = primitive("price", PrimitiveKind.INT32, new LogicalType.Decimal(2, 9));
        SchemaNode.Primitive small = primitive("small", PrimitiveKind.INT32, new LogicalType.IntType((byte) 16, false));
        SchemaNode.Primitive day = primitive("day", PrimitiveKind.INT32, new LogicalType.DateType());
        SchemaNode.Primitive ts =
                primitive("ts", PrimitiveKind.INT64, new LogicalType.Timestamp(true, LogicalType.TimeUnit.MICROS));
        SchemaNode.Primitive tsLocal = primitive(
                "tsLocal", PrimitiveKind.INT64, new LogicalType.Timestamp(false, LogicalType.TimeUnit.MICROS));
        SchemaNode.Primitive raw = primitive("raw", PrimitiveKind.DOUBLE, null);
        SchemaNode.Primitive uid = new SchemaNode.Primitive(
                "uid",
                Repetition.OPTIONAL,
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(16),
                Optional.of(new LogicalType.UuidType()),
                -1);
        SchemaNode.Group root = new SchemaNode.Group(
                "schema",
                Repetition.REQUIRED,
                List.of(id, tags, props, price, small, day, ts, tsLocal, raw, uid),
                Optional.empty(),
                -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive string(String name) {
        return primitive(name, PrimitiveKind.BYTE_ARRAY, new LogicalType.StringType());
    }

    private static SchemaNode.Primitive primitive(String name, PrimitiveKind kind, LogicalType logicalType) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.ofNullable(logicalType), -1);
    }

    private static SchemaNode.Group group(String name, LogicalType logicalType, SchemaNode... children) {
        Repetition repetition = logicalType == null ? Repetition.REPEATED : Repetition.OPTIONAL;
        return new SchemaNode.Group(name, repetition, List.of(children), Optional.ofNullable(logicalType), -1);
    }

    private static Optional<ConvertedType> convertedTypeOf(List<SchemaElement> elements, String name) {
        return elementNamed(elements, name).convertedType();
    }

    private static SchemaElement elementNamed(List<SchemaElement> elements, String name) {
        return elements.stream()
                .filter(element -> element.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no schema element named " + name));
    }
}
