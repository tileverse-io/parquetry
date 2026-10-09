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
package io.tileverse.parquetry.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.io.ByteRangeSource;

class SchemaBuilderTest {

    private static final int DEEPEST_NESTING = 500;

    @Test
    void buildsFromEmptyIntColumn(@TempDir Path tmp) throws Exception {
        Path file = TestFixtures.emptyIntColumn(tmp);
        FileMetaData footer = readFooter(file);

        ParquetSchema schema = SchemaBuilder.build(footer.schema());

        assertThat(schema.root().children()).hasSize(1);
        SchemaNode idField = schema.root().children().get(0);
        assertThat(idField.name()).isEqualTo("id");
        assertThat(idField).isInstanceOf(SchemaNode.Primitive.class);
        assertThat(((SchemaNode.Primitive) idField).kind()).isEqualTo(PrimitiveKind.INT32);
    }

    @Test
    void buildsFromFlatIntSnappy(@TempDir Path tmp) throws Exception {
        Path file = TestFixtures.flatIntSnappy(tmp);
        FileMetaData footer = readFooter(file);

        ParquetSchema schema = SchemaBuilder.build(footer.schema());

        // 5 columns: int, long, float, double, boolean
        assertThat(schema.root().children()).hasSize(5);
        assertThat(schema.leafColumns()).hasSize(5);
    }

    @Test
    void buildsFromNestedList(@TempDir Path tmp) throws Exception {
        Path file = TestFixtures.nestedList(tmp);
        FileMetaData footer = readFooter(file);

        ParquetSchema schema = SchemaBuilder.build(footer.schema());

        // Avro array<int> -> Parquet list<int>: the total leaf count is exactly 1 (the int element).
        assertThat(schema.leafColumns()).hasSize(1);
    }

    @Test
    void buildsFromNestedMap(@TempDir Path tmp) throws Exception {
        Path file = TestFixtures.nestedMap(tmp);
        FileMetaData footer = readFooter(file);

        ParquetSchema schema = SchemaBuilder.build(footer.schema());

        // map<string,int> -> 2 leaves: key (string) and value (int)
        assertThat(schema.leafColumns()).hasSize(2);
    }

    @Test
    void buildsGroupsNestedToTheLimit() {
        ParquetSchema schema = SchemaBuilder.build(leafUnderNestedGroups(DEEPEST_NESTING));

        assertThat(schema.leafColumns()).hasSize(1);
        assertThat(schema.leafColumns().get(0).numParts()).isEqualTo(DEEPEST_NESTING + 1);
    }

    // Groups nested deeper than the limit of the reader are unsupported, not malformed.
    @ParameterizedTest(name = "{0} groups")
    @ValueSource(ints = {DEEPEST_NESTING + 1, 20_000})
    void rejectsNestingBeyondTheLimit(int groups) {
        List<SchemaElement> elements = leafUnderNestedGroups(groups);

        assertThatThrownBy(() -> SchemaBuilder.build(elements))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("nests more than " + DEEPEST_NESTING + " groups");
    }

    // A group declaring more children than the elements left in the schema is malformed.
    @ParameterizedTest(name = "{0} children")
    @ValueSource(ints = {2, 1_000_000, Integer.MAX_VALUE, -1})
    void rejectsChildrenBeyondTheSchema(int declaredChildren) {
        List<SchemaElement> elements = List.of(group("schema", 1), group("points", declaredChildren), leaf("x"));

        assertThatThrownBy(() -> SchemaBuilder.build(elements))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("points")
                .hasMessageContaining(String.valueOf(declaredChildren));
    }

    // The root declares two children and holds one: the last element is the child of the inner group.
    @Test
    void rejectsRootShortOfAChild() {
        List<SchemaElement> elements = List.of(group("schema", 2), group("g", 1), leaf("x"));

        assertThatThrownBy(() -> SchemaBuilder.build(elements))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("schema");
    }

    private static FileMetaData readFooter(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFormat.readFooter(source);
        }
    }

    /** A root group, then {@code groups} groups nested one inside the other, then one INT32 leaf. */
    private static List<SchemaElement> leafUnderNestedGroups(int groups) {
        List<SchemaElement> elements = new ArrayList<>(groups + 2);
        elements.add(group("schema", 1));
        for (int level = 0; level < groups; level++) {
            elements.add(group("g", 1));
        }
        elements.add(leaf("x"));
        return elements;
    }

    private static SchemaElement group(String name, int children) {
        return new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.OPTIONAL),
                name,
                OptionalInt.of(children),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }

    private static SchemaElement leaf(String name) {
        return new SchemaElement(
                Optional.of(PhysicalType.INT32),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.OPTIONAL),
                name,
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }
}
