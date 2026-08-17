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

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

/**
 * Resolves max repetition / definition levels for leaves reached through required, optional, and repeated ancestors,
 * and pins the failures reported for a path that names no reachable leaf.
 *
 * <p>Expected level pairs are derived by hand from the schemas built here: REQUIRED contributes nothing, OPTIONAL adds
 * one definition level, REPEATED adds one definition and one repetition level.
 */
class ParquetSchemaMaxLevelsTest {

    private final ParquetSchema schema = sampleSchema();

    @Test
    void topLevelRequiredLeafHasNoLevels() {
        assertThat(schema.maxLevels(ColumnPath.of("id"))).isEqualTo(new LevelMaxima(0, 0));
    }

    @Test
    void topLevelOptionalLeafIsOneDefinitionLevelDeep() {
        assertThat(schema.maxLevels(ColumnPath.of("comment"))).isEqualTo(new LevelMaxima(0, 1));
    }

    @Test
    void optionalAndRepeatedAncestorsBothRaiseTheDefinitionLevel() {
        ColumnPath locality = ColumnPath.of("addresses", "list", "element", "locality");

        assertThat(schema.maxLevels(locality)).isEqualTo(new LevelMaxima(1, 3));
    }

    @Test
    void everyRepeatedLevelAlongThePathRaisesTheRepetitionLevel() {
        ColumnPath tags = ColumnPath.of("addresses", "list", "element", "tags");

        assertThat(schema.maxLevels(tags)).isEqualTo(new LevelMaxima(2, 3));
    }

    @Test
    void unknownMiddleSegmentNamesTheSegmentAndTheWholePath() {
        ColumnPath unknown = ColumnPath.of("addresses", "nope", "element", "locality");

        assertThatThrownBy(() -> schema.maxLevels(unknown))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessage("ParquetSchema missing child 'nope' along path addresses.nope.element.locality");
    }

    @Test
    void unknownTopLevelSegmentNamesTheSegmentAndTheWholePath() {
        ColumnPath unknown = ColumnPath.of("nope", "locality");

        assertThatThrownBy(() -> schema.maxLevels(unknown))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessage("ParquetSchema missing child 'nope' along path nope.locality");
    }

    @Test
    void descendingPastALeafReportsTheNonGroup() {
        ColumnPath belowALeaf = ColumnPath.of("id", "deeper");

        assertThatThrownBy(() -> schema.maxLevels(belowALeaf))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessage("Path traversal hit a non-group at id.deeper");
    }

    @Test
    void segmentThatIsAPrefixOfAChildNameDoesNotMatch() {
        ParquetSchema prefixes = prefixSiblingSchema();

        assertThatThrownBy(() -> prefixes.maxLevels(ColumnPath.of("a", "leaf")))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessage("ParquetSchema missing child 'a' along path a.leaf");
    }

    @Test
    void segmentThatExtendsAChildNameDoesNotMatch() {
        ParquetSchema prefixes = prefixSiblingSchema();

        assertThatThrownBy(() -> prefixes.maxLevels(ColumnPath.of("abcd", "leaf")))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessage("ParquetSchema missing child 'abcd' along path abcd.leaf");
    }

    @Test
    void siblingsWhoseNamesSharePrefixesResolveToTheirOwnLevels() {
        ParquetSchema prefixes = prefixSiblingSchema();

        assertThat(prefixes.maxLevels(ColumnPath.of("ab", "leaf"))).isEqualTo(new LevelMaxima(0, 1));
        assertThat(prefixes.maxLevels(ColumnPath.of("abc", "leaf"))).isEqualTo(new LevelMaxima(1, 2));
    }

    private static ParquetSchema sampleSchema() {
        SchemaNode.Group element = group(
                "element",
                Repetition.REQUIRED,
                List.of(doubleLeaf("locality", Repetition.OPTIONAL), doubleLeaf("tags", Repetition.REPEATED)));
        SchemaNode.Group list = group("list", Repetition.REPEATED, List.of(element));
        SchemaNode.Group addresses = group("addresses", Repetition.OPTIONAL, List.of(list));
        SchemaNode.Group root = group(
                "root",
                Repetition.REQUIRED,
                List.of(intLeaf("id", Repetition.REQUIRED), doubleLeaf("comment", Repetition.OPTIONAL), addresses));
        return new ParquetSchema(root);
    }

    private static ParquetSchema prefixSiblingSchema() {
        SchemaNode.Group ab = group("ab", Repetition.OPTIONAL, List.of(doubleLeaf("leaf", Repetition.REQUIRED)));
        SchemaNode.Group abc = group("abc", Repetition.REPEATED, List.of(doubleLeaf("leaf", Repetition.OPTIONAL)));
        return new ParquetSchema(group("root", Repetition.REQUIRED, List.of(ab, abc)));
    }

    private static SchemaNode.Group group(String name, Repetition repetition, List<SchemaNode> children) {
        return new SchemaNode.Group(name, repetition, children, Optional.empty(), -1);
    }

    private static SchemaNode doubleLeaf(String name, Repetition repetition) {
        return new SchemaNode.Primitive(
                name, repetition, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode intLeaf(String name, Repetition repetition) {
        return new SchemaNode.Primitive(
                name, repetition, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }
}
