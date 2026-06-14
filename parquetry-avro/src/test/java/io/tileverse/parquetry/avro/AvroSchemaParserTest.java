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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.testkit.TestCorpus;

class AvroSchemaParserTest {

    @Test
    void parsesRecordWithFieldIdAttribute() {
        AvroSchema schema = AvroSchemaParser.parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                + "{\"name\":\"status\",\"type\":\"int\",\"field-id\":0},"
                + "{\"name\":\"path\",\"type\":\"string\",\"field-id\":100}]}");
        AvroSchema.Record recordSchema = (AvroSchema.Record) schema;
        assertThat(recordSchema.fields()).hasSize(2);

        AvroSchema.Field status = recordSchema.fields().get(0);
        assertThat(status.name()).isEqualTo("status");
        assertThat(status.position()).isZero();
        assertThat(status.schema().type()).isEqualTo(AvroSchema.Type.INT);
        assertThat(status.fieldId()).isEqualTo(OptionalInt.of(0));

        assertThat(recordSchema.fields().get(1).fieldId()).isEqualTo(OptionalInt.of(100));
    }

    @Test
    void parsesNullableUnionAsTwoBranches() {
        AvroSchema schema = AvroSchemaParser.parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                + "{\"name\":\"id\",\"type\":[\"null\",\"long\"]}]}");
        AvroSchema.Record recordSchema = (AvroSchema.Record) schema;
        AvroSchema.Union union = (AvroSchema.Union) recordSchema.fields().get(0).schema();
        assertThat(union.branches()).hasSize(2);
        assertThat(union.branches().get(0).type()).isEqualTo(AvroSchema.Type.NULL);
        assertThat(union.branches().get(1).type()).isEqualTo(AvroSchema.Type.LONG);
    }

    @Test
    void rejectsEmptyUnionFieldType() {
        assertThatThrownBy(() -> AvroSchemaParser.parse(
                        "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"f\",\"type\":[]}]}"))
                .isInstanceOf(AvroFormatException.class)
                .hasMessage("Empty union");
    }

    @Test
    void unionModelRejectsEmptyBranchList() {
        List<AvroSchema> empty = List.of();
        assertThatThrownBy(() -> new AvroSchema.Union(empty))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Union must have at least one branch");
    }

    @Test
    void parsesMapOfBinaryBounds() {
        AvroSchema schema = AvroSchemaParser.parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                + "{\"name\":\"lower_bounds\",\"type\":{\"type\":\"map\",\"values\":\"bytes\"}}]}");
        AvroSchema.Record recordSchema = (AvroSchema.Record) schema;
        AvroSchema.Map map = (AvroSchema.Map) recordSchema.fields().get(0).schema();
        assertThat(map.values().type()).isEqualTo(AvroSchema.Type.BYTES);
    }

    @Test
    void parsesArrayFixedAndEnum() {
        AvroSchema schema = AvroSchemaParser.parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                + "{\"name\":\"offsets\",\"type\":{\"type\":\"array\",\"items\":\"long\"}},"
                + "{\"name\":\"key\",\"type\":{\"type\":\"fixed\",\"name\":\"k\",\"size\":16}},"
                + "{\"name\":\"st\",\"type\":{\"type\":\"enum\",\"name\":\"S\",\"symbols\":[\"A\",\"B\"]}}]}");
        AvroSchema.Record recordSchema = (AvroSchema.Record) schema;
        assertThat(((AvroSchema.Array) recordSchema.fields().get(0).schema())
                        .element()
                        .type())
                .isEqualTo(AvroSchema.Type.LONG);
        assertThat(((AvroSchema.Fixed) recordSchema.fields().get(1).schema()).size())
                .isEqualTo(16);
        assertThat(((AvroSchema.Enum) recordSchema.fields().get(2).schema()).symbols())
                .containsExactly("A", "B");
    }

    @Test
    void resolvesNamedTypeReference() {
        AvroSchema schema = AvroSchemaParser.parse("{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
                + "{\"name\":\"a\",\"type\":{\"type\":\"record\",\"name\":\"Inner\","
                + "\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}},"
                + "{\"name\":\"b\",\"type\":\"Inner\"}]}");
        AvroSchema.Record outer = (AvroSchema.Record) schema;
        AvroSchema.Record viaReference =
                (AvroSchema.Record) outer.fields().get(1).schema();
        assertThat(viaReference.name()).isEqualTo("Inner");
        assertThat(viaReference.fields().get(0).name()).isEqualTo("x");
    }

    @Test
    void treatsJsonNullAttributeAsAbsent() {
        AvroSchema schema = AvroSchemaParser.parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                + "{\"name\":\"f\",\"type\":\"int\",\"custom\":null}]}");
        AvroSchema.Field field = ((AvroSchema.Record) schema).fields().get(0);
        assertThat(field.attribute("custom")).isEmpty();
        assertThat(field.attributes()).doesNotContainKey("custom");
    }

    @Test
    void rejectsTypeObjectWithoutTypeKey() {
        assertThatThrownBy(() -> AvroSchemaParser.parse("{\"name\":\"r\"}")).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void rejectsRecordWithoutFields() {
        assertThatThrownBy(() -> AvroSchemaParser.parse("{\"type\":\"record\",\"name\":\"r\"}"))
                .isInstanceOf(AvroFormatException.class);
    }

    @Test
    void rejectsFixedWithoutSize() {
        assertThatThrownBy(() -> AvroSchemaParser.parse("{\"type\":\"fixed\",\"name\":\"k\"}"))
                .isInstanceOf(AvroFormatException.class);
    }

    @Test
    void rejectsNonIntegralFixedSize() {
        assertThatThrownBy(() -> AvroSchemaParser.parse("{\"type\":\"fixed\",\"name\":\"k\",\"size\":4.5}"))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("size");
    }

    @Test
    void rejectsNegativeFixedSize() {
        assertThatThrownBy(() -> AvroSchemaParser.parse("{\"type\":\"fixed\",\"name\":\"k\",\"size\":-1}"))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("Fixed size must be non-negative: -1");
    }

    @Test
    void rejectsNonStringFieldAlias() {
        assertThatThrownBy(() -> AvroSchemaParser.parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                        + "{\"name\":\"f\",\"type\":\"int\",\"aliases\":[7]}]}"))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("Alias");
    }

    @Test
    void resolvesNamespacedReferences() {
        AvroSchema schema = AvroSchemaParser.parse("""
                {"type":"record","name":"Outer","namespace":"com.example","fields":[
                  {"name":"a","type":{"type":"record","name":"Inner","fields":[
                    {"name":"x","type":"int"}]}},
                  {"name":"b","type":"Inner"},
                  {"name":"c","type":"com.example.Inner"}]}
                """);
        AvroSchema.Record outer = (AvroSchema.Record) schema;
        AvroSchema inner = resolve(outer.fields().get(0).schema());
        assertThat(((AvroSchema.Record) inner).fullName()).isEqualTo("com.example.Inner");
        assertThat(resolve(outer.fields().get(1).schema())).isSameAs(inner);
        assertThat(resolve(outer.fields().get(2).schema())).isSameAs(inner);
    }

    @Test
    void resolvesRecursiveReference() {
        AvroSchema schema = AvroSchemaParser.parse("""
                {"type":"record","name":"Node","fields":[
                  {"name":"value","type":"long"},
                  {"name":"next","type":["null","Node"]}]}
                """);
        AvroSchema.Record node = (AvroSchema.Record) schema;
        AvroSchema.Union next = (AvroSchema.Union) node.fields().get(1).schema();
        AvroSchema.Ref ref = (AvroSchema.Ref) next.branches().get(1);
        assertThat(ref.target()).isSameAs(node);
    }

    @Test
    void resolvesForwardReference() {
        AvroSchema schema = AvroSchemaParser.parse("""
                {"type":"record","name":"Outer","fields":[
                  {"name":"a","type":"Later"},
                  {"name":"b","type":{"type":"record","name":"Later","fields":[
                    {"name":"x","type":"int"}]}}]}
                """);
        AvroSchema.Record outer = (AvroSchema.Record) schema;
        AvroSchema.Ref ref = (AvroSchema.Ref) outer.fields().get(0).schema();
        assertThat(ref.target()).isSameAs(outer.fields().get(1).schema());
    }

    @Test
    void distinguishesSameSimpleNameAcrossNamespaces() {
        AvroSchema schema = AvroSchemaParser.parse("""
                {"type":"record","name":"Outer","namespace":"a","fields":[
                  {"name":"x","type":{"type":"record","name":"Inner","fields":[]}},
                  {"name":"y","type":{"type":"record","name":"Inner","namespace":"b","fields":[]}},
                  {"name":"p","type":"Inner"},
                  {"name":"q","type":"b.Inner"}]}
                """);
        AvroSchema.Record outer = (AvroSchema.Record) schema;
        AvroSchema.Record viaInherited =
                (AvroSchema.Record) resolve(outer.fields().get(2).schema());
        AvroSchema.Record viaQualified =
                (AvroSchema.Record) resolve(outer.fields().get(3).schema());
        assertThat(viaInherited.fullName()).isEqualTo("a.Inner");
        assertThat(viaQualified.fullName()).isEqualTo("b.Inner");
        assertThat(viaInherited).isSameAs(resolve(outer.fields().get(0).schema()));
        assertThat(viaQualified).isSameAs(resolve(outer.fields().get(1).schema()));
    }

    @Test
    void parsesLogicalTypes() {
        AvroSchema schema = AvroSchemaParser.parse("""
                {"type":"record","name":"L","fields":[
                  {"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}},
                  {"name":"u","type":{"type":"string","logicalType":"uuid"}},
                  {"name":"day","type":{"type":"int","logicalType":"date"}},
                  {"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"}},
                  {"name":"dur","type":{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}},
                  {"name":"mystery","type":{"type":"long","logicalType":"made-up"}}]}
                """);
        List<AvroSchema.Field> fields = ((AvroSchema.Record) schema).fields();
        assertThat(fields.get(0).schema().logicalType()).contains(new LogicalType.Decimal(9, 2));
        assertThat(fields.get(1).schema().logicalType()).contains(LogicalType.Uuid.INSTANCE);
        assertThat(fields.get(2).schema().logicalType()).contains(LogicalType.Date.INSTANCE);
        assertThat(fields.get(3).schema().logicalType()).contains(LogicalType.TimestampMicros.INSTANCE);
        assertThat(fields.get(4).schema().logicalType()).contains(LogicalType.Duration.INSTANCE);
        assertThat(fields.get(5).schema().logicalType()).contains(new LogicalType.Unknown("made-up"));
    }

    @Test
    void ignoresSpecInvalidLogicalTypes() {
        // decimal on string is invalid; the spec says ignore the annotation and use the underlying type
        AvroSchema schema = AvroSchemaParser.parse("""
                {"type":"record","name":"B","fields":[
                  {"name":"x","type":{"type":"string","logicalType":"decimal","precision":4,"scale":2}}]}
                """);
        AvroSchema.Field field = ((AvroSchema.Record) schema).fields().get(0);
        assertThat(field.schema().logicalType()).isEmpty();
        assertThat(field.schema().type()).isEqualTo(AvroSchema.Type.STRING);
    }

    @Test
    void capturesAliasesAndDefaults() {
        AvroSchema schema = AvroSchemaParser.parse("""
                {"type":"record","name":"R","namespace":"ns","aliases":["OldR","other.Legacy"],"fields":[
                  {"name":"n","type":"int","default":7,"aliases":["count"]},
                  {"name":"s","type":["null","string"],"default":null}]}
                """);
        AvroSchema.Record recordSchema = (AvroSchema.Record) schema;
        assertThat(recordSchema.aliases()).containsExactly("ns.OldR", "other.Legacy");
        AvroSchema.Field n = recordSchema.fields().get(0);
        assertThat(n.hasDefault()).isTrue();
        assertThat(n.defaultValue()).isEqualTo(7L);
        assertThat(n.aliases()).containsExactly("count");
        AvroSchema.Field s = recordSchema.fields().get(1);
        assertThat(s.hasDefault()).isTrue();
        assertThat(s.defaultValue()).isNull();
    }

    @Test
    void parsesEnumDefaultSymbol() {
        AvroSchema schema = AvroSchemaParser.parse("""
                {"type":"enum","name":"Suit","symbols":["SPADES","HEARTS"],"default":"SPADES"}
                """);
        assertThat(((AvroSchema.Enum) schema).defaultSymbol()).isEqualTo("SPADES");
    }

    @Test
    void rejectsInvalidNames() {
        assertThatThrownBy(() -> AvroSchemaParser.parse("{\"type\":\"record\",\"name\":\"9bad\",\"fields\":[]}"))
                .isInstanceOf(AvroFormatException.class);
    }

    @Test
    void rejectsDuplicateNamedType() {
        assertThatThrownBy(() -> AvroSchemaParser.parse("""
                        {"type":"record","name":"Outer","fields":[
                          {"name":"a","type":{"type":"fixed","name":"K","size":4}},
                          {"name":"b","type":{"type":"fixed","name":"K","size":4}}]}
                        """))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("Duplicate named type");
    }

    @Test
    void rejectsUnresolvedReference() {
        assertThatThrownBy(() -> AvroSchemaParser.parse("""
                        {"type":"record","name":"R","fields":[
                          {"name":"a","type":"NoSuchType"}]}
                        """))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("NoSuchType");
    }

    @Test
    void rejectsEnumDefaultThatIsNotASymbol() {
        assertThatThrownBy(() -> AvroSchemaParser.parse("""
                        {"type":"enum","name":"Suit","symbols":["SPADES","HEARTS"],"default":"CLUBS"}
                        """))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("CLUBS");
    }

    @Test
    void parsesRealManifestEntrySchema() throws Exception {
        Path manifest = TestCorpus.extractFile(
                "avro-reference/iceberg-manifest/manifest.avro", Files.createTempDirectory("avro-schema"));
        try (ByteRangeSource source = ByteRangeSource.ofFile(manifest)) {
            OcfHeader header = OcfHeader.read(new ByteRangeCursor(source));
            AvroSchema.Record entry = (AvroSchema.Record) header.schema();
            AvroSchema.Field dataFile = entry.field("data_file").orElseThrow();
            assertThat(dataFile.fieldId()).isPresent();
            AvroSchema.Record dataFileRecord = (AvroSchema.Record) unwrapNullable(dataFile.schema());
            assertThat(dataFileRecord.field("lower_bounds")).isPresent();
        }
    }

    private static AvroSchema resolve(AvroSchema schema) {
        return schema instanceof AvroSchema.Ref ref ? ref.target() : schema;
    }

    private static AvroSchema unwrapNullable(AvroSchema schema) {
        if (schema instanceof AvroSchema.Union union) {
            for (AvroSchema branch : union.branches()) {
                if (branch.type() != AvroSchema.Type.NULL) {
                    return branch;
                }
            }
        }
        return schema;
    }
}
