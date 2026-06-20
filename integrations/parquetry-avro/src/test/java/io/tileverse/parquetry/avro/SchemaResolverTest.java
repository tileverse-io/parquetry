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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Resolution plans and resolved decoding for (writer, reader) schema pairs, per the Avro spec's resolution rules. */
class SchemaResolverTest {

    private static Object decode(AvroSchema writer, AvroSchema reader, byte[] datum) {
        Resolved plan = SchemaResolver.resolve(writer, reader);
        return decode(plan, datum);
    }

    private static Object decode(Resolved plan, byte[] datum) {
        AvroBinaryDecoder in = new AvroBinaryDecoder(MemorySegment.ofArray(datum));
        return new ResolvingDatumDecoder().decode(plan, in);
    }

    private static AvroSchema recordOf(String name, String fieldsJson) {
        return AvroSchema.parse("{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":[" + fieldsJson + "]}");
    }

    @Test
    void projectionSkipsDroppedWriterField() {
        AvroSchema writer = recordOf(
                "Row",
                "{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"c\",\"type\":\"long\"}");
        AvroSchema reader = recordOf("Row", "{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"c\",\"type\":\"long\"}");

        Resolved plan = SchemaResolver.resolve(writer, reader);
        Resolved.RecordPlan recordPlan = (Resolved.RecordPlan) plan;
        assertThat(recordPlan.steps().get(1).readerPosition()).isEqualTo(-1);

        byte[] datum =
                new AvroTestEncoder().intValue(1).string("dropped").longValue(7).toBytes();
        AvroRecord decoded = (AvroRecord) decode(plan, datum);
        assertThat(decoded.get("a")).isEqualTo(1);
        assertThat(decoded.get("c")).isEqualTo(7L);
    }

    @Test
    void readerOnlyFieldFilledFromDefault() {
        AvroSchema writer = recordOf("Row", "{\"name\":\"a\",\"type\":\"int\"}");
        AvroSchema reader = recordOf(
                "Row", "{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"d\",\"type\":\"string\",\"default\":\"x\"}");

        byte[] datum = new AvroTestEncoder().intValue(3).toBytes();
        AvroRecord decoded = (AvroRecord) decode(writer, reader, datum);
        assertThat(decoded.get("a")).isEqualTo(3);
        assertThat(decoded.get("d")).isEqualTo("x");
    }

    static Stream<Arguments> numericPromotions() {
        byte[] intDatum = new AvroTestEncoder().intValue(41).toBytes();
        byte[] longDatum = new AvroTestEncoder().longValue(10).toBytes();
        byte[] floatDatum = new AvroTestEncoder().floatValue(2.5f).toBytes();
        return Stream.of(
                Arguments.of("int", "long", intDatum, 41L),
                Arguments.of("int", "float", intDatum, 41.0f),
                Arguments.of("int", "double", intDatum, 41.0d),
                Arguments.of("long", "float", longDatum, 10.0f),
                Arguments.of("long", "double", longDatum, 10.0d),
                Arguments.of("float", "double", floatDatum, 2.5d));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("numericPromotions")
    void numericPromotionWidensTheBox(String writerType, String readerType, byte[] datum, Object expected) {
        AvroSchema writer = AvroSchema.parse("\"" + writerType + "\"");
        AvroSchema reader = AvroSchema.parse("\"" + readerType + "\"");
        assertThat(decode(writer, reader, datum)).isEqualTo(expected);
    }

    @Test
    void stringPromotesToBytes() {
        byte[] datum = new AvroTestEncoder().string("hi").toBytes();
        Object decoded = decode(AvroSchema.STRING, AvroSchema.BYTES, datum);
        MemorySegment segment = (MemorySegment) decoded;
        assertThat(segment.toArray(ValueLayout.JAVA_BYTE)).isEqualTo("hi".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void bytesPromoteToString() {
        byte[] datum = new AvroTestEncoder()
                .bytes("hola".getBytes(StandardCharsets.UTF_8))
                .toBytes();
        assertThat(decode(AvroSchema.BYTES, AvroSchema.STRING, datum)).isEqualTo("hola");
    }

    @Test
    void recordNameMatchesThroughReaderAlias() {
        AvroSchema writer = AvroSchema.parse(
                "{\"type\":\"record\",\"name\":\"Old\",\"namespace\":\"ns\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}");
        AvroSchema reader =
                AvroSchema.parse("{\"type\":\"record\",\"name\":\"Fresh\",\"namespace\":\"ns\",\"aliases\":[\"Old\"],"
                        + "\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}");

        byte[] datum = new AvroTestEncoder().intValue(9).toBytes();
        AvroRecord decoded = (AvroRecord) decode(writer, reader, datum);
        assertThat(decoded.schema().fullName()).isEqualTo("ns.Fresh");
        assertThat(decoded.get("a")).isEqualTo(9);
    }

    @Test
    void fieldMatchesThroughReaderFieldAlias() {
        AvroSchema writer = recordOf("Row", "{\"name\":\"name\",\"type\":\"string\"}");
        AvroSchema reader = recordOf("Row", "{\"name\":\"title\",\"type\":\"string\",\"aliases\":[\"name\"]}");

        byte[] datum = new AvroTestEncoder().string("ada").toBytes();
        AvroRecord decoded = (AvroRecord) decode(writer, reader, datum);
        assertThat(decoded.get("title")).isEqualTo("ada");
    }

    @Test
    void readerUnionPicksTheFirstMatchingBranch() {
        AvroSchema reader = AvroSchema.parse("[\"long\",\"double\"]");

        Resolved plan = SchemaResolver.resolve(AvroSchema.INT, reader);
        assertThat(plan).isInstanceOf(Resolved.Promote.class);
        Resolved.Promote promote = (Resolved.Promote) plan;
        assertThat(promote.reader().type()).isEqualTo(AvroSchema.Type.LONG);

        byte[] datum = new AvroTestEncoder().intValue(5).toBytes();
        assertThat(decode(plan, datum)).isEqualTo(5L);
    }

    @Test
    void readerUnionWithNoMatchFailsOnlyAtDecode() {
        AvroSchema reader = AvroSchema.parse("[\"string\",\"boolean\"]");

        Resolved plan = SchemaResolver.resolve(AvroSchema.INT, reader);
        assertThat(plan).isInstanceOf(Resolved.Failure.class);

        byte[] datum = new AvroTestEncoder().intValue(5).toBytes();
        assertThatThrownBy(() -> decode(plan, datum))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("No reader union branch matches");
    }

    @Test
    void writerUnionBranchesResolveIndependently() {
        AvroSchema writer = AvroSchema.parse("[\"int\",\"string\"]");

        Resolved plan = SchemaResolver.resolve(writer, AvroSchema.LONG);
        assertThat(plan).isInstanceOf(Resolved.WriterUnionPlan.class);

        byte[] intBranch = new AvroTestEncoder().intValue(0).intValue(5).toBytes();
        assertThat(decode(plan, intBranch)).isEqualTo(5L);

        byte[] stringBranch = new AvroTestEncoder().intValue(1).string("x").toBytes();
        assertThatThrownBy(() -> decode(plan, stringBranch))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("does not resolve");
    }

    private static AvroSchema colorEnum(String symbolsJson, String defaultJson) {
        String defaultAttribute = defaultJson == null ? "" : ",\"default\":" + defaultJson;
        return AvroSchema.parse(
                "{\"type\":\"enum\",\"name\":\"Color\",\"symbols\":" + symbolsJson + defaultAttribute + "}");
    }

    @Test
    void enumSymbolsRemapToReaderIndexes() {
        AvroSchema writer = colorEnum("[\"RED\",\"GREEN\",\"BLUE\"]", null);
        AvroSchema reader = colorEnum("[\"BLUE\",\"RED\"]", null);

        Resolved plan = SchemaResolver.resolve(writer, reader);
        assertThat(decode(plan, new AvroTestEncoder().intValue(0).toBytes())).isEqualTo("RED");
        assertThat(decode(plan, new AvroTestEncoder().intValue(2).toBytes())).isEqualTo("BLUE");
    }

    @Test
    void unmappedEnumSymbolFallsBackToReaderDefault() {
        AvroSchema writer = colorEnum("[\"RED\",\"GREEN\",\"BLUE\"]", null);
        AvroSchema reader = colorEnum("[\"RED\",\"GREEN\"]", "\"RED\"");

        byte[] blue = new AvroTestEncoder().intValue(2).toBytes();
        assertThat(decode(writer, reader, blue)).isEqualTo("RED");
    }

    @Test
    void unmappedEnumSymbolWithoutDefaultFailsOnlyAtDecode() {
        AvroSchema writer = colorEnum("[\"RED\",\"GREEN\",\"BLUE\"]", null);
        AvroSchema reader = colorEnum("[\"RED\",\"GREEN\"]", null);

        Resolved plan = SchemaResolver.resolve(writer, reader);
        assertThat(plan).isInstanceOf(Resolved.EnumPlan.class);

        byte[] blue = new AvroTestEncoder().intValue(2).toBytes();
        assertThatThrownBy(() -> decode(plan, blue))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("no default");
    }

    @Test
    void missingDefaultErrorsOnlyWhenADatumIsDecoded() {
        AvroSchema writer = recordOf("Row", "{\"name\":\"a\",\"type\":\"int\"}");
        AvroSchema reader = recordOf("Row", "{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"req\",\"type\":\"string\"}");

        assertThatCode(() -> SchemaResolver.resolve(writer, reader)).doesNotThrowAnyException();

        Resolved plan = SchemaResolver.resolve(writer, reader);
        byte[] datum = new AvroTestEncoder().intValue(1).toBytes();
        assertThatThrownBy(() -> decode(plan, datum))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("Row.req");
    }

    @Test
    void malformedDefaultErrorsOnlyWhenADatumIsDecoded() {
        AvroSchema writer = recordOf("Row", "{\"name\":\"a\",\"type\":\"int\"}");
        AvroSchema reader = recordOf(
                "Row", "{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"d\",\"type\":\"int\",\"default\":\"oops\"}");

        assertThatCode(() -> SchemaResolver.resolve(writer, reader)).doesNotThrowAnyException();

        Resolved plan = SchemaResolver.resolve(writer, reader);
        assertThat(plan).isInstanceOf(Resolved.Failure.class);
        byte[] datum = new AvroTestEncoder().intValue(1).toBytes();
        assertThatThrownBy(() -> decode(plan, datum))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("Row.d");
    }

    private static final String NODE_TEMPLATE = "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
            + "{\"name\":\"value\",\"type\":\"%s\"},"
            + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"]}]}";

    @Test
    void recursiveSchemaPairClosesItsCycleThroughThePlanMemo() {
        AvroSchema writer = AvroSchema.parse(String.format(NODE_TEMPLATE, "int"));
        AvroSchema reader = AvroSchema.parse(String.format(NODE_TEMPLATE, "long"));

        Resolved plan = SchemaResolver.resolve(writer, reader);
        Resolved.RecordPlan recordPlan = (Resolved.RecordPlan) plan;
        Resolved.WriterUnionPlan nextPlan =
                (Resolved.WriterUnionPlan) recordPlan.steps().get(1).value();
        Resolved.PlanRef cycle = (Resolved.PlanRef) nextPlan.branches().get(1);
        assertThat(cycle.target()).isSameAs(plan);

        byte[] chain = new AvroTestEncoder()
                .intValue(1)
                .intValue(1) // next: union branch Node
                .intValue(2)
                .intValue(0) // next: union branch null
                .toBytes();
        AvroRecord head = (AvroRecord) decode(plan, chain);
        assertThat(head.get("value")).isEqualTo(1L);
        AvroRecord tail = (AvroRecord) head.get("next");
        assertThat(tail.get("value")).isEqualTo(2L);
        assertThat(tail.get("next")).isNull();
    }

    @Test
    void readerLogicalTypeConvertsTheDecodedValue() {
        AvroSchema reader = AvroSchema.parse("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}");

        byte[] datum = new AvroTestEncoder().longValue(1_000_000L).toBytes();
        assertThat(decode(AvroSchema.LONG, reader, datum)).isEqualTo(Instant.ofEpochSecond(1));
    }

    @Test
    void promotionAppliesTheReaderLogicalType() {
        AvroSchema reader = AvroSchema.parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");

        byte[] datum = new AvroTestEncoder().intValue(1000).toBytes();
        assertThat(decode(AvroSchema.INT, reader, datum)).isEqualTo(Instant.ofEpochMilli(1000));
    }

    private static AvroSchema fixedSchema(String name, int size, String aliasesJson) {
        String aliases = aliasesJson == null ? "" : ",\"aliases\":" + aliasesJson;
        return AvroSchema.parse("{\"type\":\"fixed\",\"name\":\"" + name + "\",\"size\":" + size + aliases + "}");
    }

    @Test
    void fixedMatchesByAliasAndEqualSize() {
        AvroSchema writer = fixedSchema("Hash", 4, null);
        AvroSchema reader = fixedSchema("Digest", 4, "[\"Hash\"]");

        byte[] datum = new byte[] {1, 2, 3, 4};
        MemorySegment decoded = (MemorySegment) decode(writer, reader, datum);
        assertThat(decoded.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(datum);
    }

    @Test
    void fixedSizeMismatchFailsOnlyAtDecode() {
        AvroSchema writer = fixedSchema("Hash", 4, null);
        AvroSchema reader = fixedSchema("Hash", 8, null);

        Resolved plan = SchemaResolver.resolve(writer, reader);
        assertThat(plan).isInstanceOf(Resolved.Failure.class);
        assertThatThrownBy(() -> decode(plan, new byte[] {1, 2, 3, 4}))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("size mismatch");
    }

    private static AvroSchema bytesDecimal(int precision, int scale) {
        return AvroSchema.parse("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":" + precision
                + ",\"scale\":" + scale + "}");
    }

    private static AvroSchema fixedDecimal(int precision, int scale) {
        return AvroSchema.parse(
                "{\"type\":\"fixed\",\"name\":\"Dec\",\"size\":2,\"logicalType\":\"decimal\",\"precision\":" + precision
                        + ",\"scale\":" + scale + "}");
    }

    /** The unscaled two's-complement big-endian bytes of 1234: 12.34 at scale 2, 1.234 at scale 3. */
    private static final byte[] UNSCALED_1234 = {0x04, (byte) 0xD2};

    @Test
    void decimalScaleMismatchFailsOnlyAtDecode() {
        AvroSchema writer = bytesDecimal(4, 2);
        AvroSchema reader = bytesDecimal(4, 3);

        assertThatCode(() -> SchemaResolver.resolve(writer, reader)).doesNotThrowAnyException();

        Resolved plan = SchemaResolver.resolve(writer, reader);
        assertThat(plan).isInstanceOf(Resolved.Failure.class);

        byte[] datum = new AvroTestEncoder().bytes(UNSCALED_1234).toBytes();
        assertThatThrownBy(() -> decode(plan, datum))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("Decimal mismatch")
                .hasMessageContaining("decimal(4,2)")
                .hasMessageContaining("decimal(4,3)");
    }

    @Test
    void equalDecimalsResolveDirectAndRoundTrip() {
        AvroSchema writer = bytesDecimal(4, 2);
        AvroSchema reader = bytesDecimal(4, 2);

        Resolved plan = SchemaResolver.resolve(writer, reader);
        assertThat(plan).isInstanceOf(Resolved.Direct.class);

        byte[] datum = new AvroTestEncoder().bytes(UNSCALED_1234).toBytes();
        assertThat(decode(plan, datum)).isEqualTo(new BigDecimal("12.34"));
    }

    @Test
    void fixedDecimalMismatchFailsOnlyAtDecode() {
        AvroSchema writer = fixedDecimal(4, 2);
        AvroSchema reader = fixedDecimal(4, 3);

        Resolved plan = SchemaResolver.resolve(writer, reader);
        assertThat(plan).isInstanceOf(Resolved.Failure.class);
        assertThatThrownBy(() -> decode(plan, UNSCALED_1234))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("Decimal mismatch")
                .hasMessageContaining("decimal(4,2)")
                .hasMessageContaining("decimal(4,3)");
    }

    @Test
    void arraysResolveTheirElementSchemas() {
        AvroSchema writer = AvroSchema.parse("{\"type\":\"array\",\"items\":\"int\"}");
        AvroSchema reader = AvroSchema.parse("{\"type\":\"array\",\"items\":\"long\"}");

        byte[] datum = new AvroTestEncoder()
                .longValue(2)
                .intValue(4)
                .intValue(5)
                .longValue(0)
                .toBytes();
        assertThat(decode(writer, reader, datum)).isEqualTo(List.of(4L, 5L));
    }

    @Test
    void mapsResolveTheirValueSchemas() {
        AvroSchema writer = AvroSchema.parse("{\"type\":\"map\",\"values\":\"int\"}");
        AvroSchema reader = AvroSchema.parse("{\"type\":\"map\",\"values\":\"double\"}");

        byte[] datum = new AvroTestEncoder()
                .longValue(1)
                .string("k")
                .intValue(3)
                .longValue(0)
                .toBytes();
        assertThat(decode(writer, reader, datum)).isEqualTo(Map.of("k", 3.0d));
    }

    @Test
    void plainTypeMismatchFailsOnlyAtDecode() {
        Resolved plan = SchemaResolver.resolve(AvroSchema.INT, AvroSchema.STRING);
        assertThat(plan).isInstanceOf(Resolved.Failure.class);

        byte[] datum = new AvroTestEncoder().intValue(5).toBytes();
        assertThatThrownBy(() -> decode(plan, datum))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("does not resolve");
    }

    @Test
    void recordNameMismatchWithoutAliasIsAFailure() {
        AvroSchema writer = recordOf("Old", "{\"name\":\"a\",\"type\":\"int\"}");
        AvroSchema reader = recordOf("Fresh", "{\"name\":\"a\",\"type\":\"int\"}");

        Resolved plan = SchemaResolver.resolve(writer, reader);
        assertThat(plan).isInstanceOf(Resolved.Failure.class);
    }

    @Test
    void arrayAgainstNonArrayIsAFailure() {
        AvroSchema writer = AvroSchema.parse("{\"type\":\"array\",\"items\":\"int\"}");

        Resolved plan = SchemaResolver.resolve(writer, AvroSchema.INT);
        assertThat(plan).isInstanceOf(Resolved.Failure.class);
    }
}
