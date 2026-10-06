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
package io.tileverse.parquetry.cli.expr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * A quoted identifier of a filter names the column without its quotes. Quotes are how a filter reaches a column named
 * as a SQL keyword, or with a space.
 */
class QuotedColumnReferenceTest {

    private static final ColumnPath FROM = ColumnPath.of("from");
    private static final ColumnPath MY_COLUMN = ColumnPath.of("my column");
    private static final ColumnPath OUTER_C = ColumnPath.of("outer", "c");
    private static final ColumnPath TAG_SELECT = ColumnPath.of("tags", "list", "element", "select");
    private static final ColumnPath THE_GEOM = ColumnPath.of("the geom");
    private static final ParquetSchema SCHEMA = schema();

    @ParameterizedTest(name = "{0}")
    @MethodSource("filtersWithQuotedIdentifiers")
    void quotedIdentifierNamesTheColumnWithoutItsQuotes(String filter, Predicate expected) {
        Predicate parsed = parse(filter);

        assertThat(parsed).isEqualTo(expected);
    }

    static Stream<Arguments> filtersWithQuotedIdentifiers() {
        Predicate tagIsX = new Predicate.Eq(TAG_SELECT, new Value.StringVal("x"));
        return Stream.of(
                Arguments.of("\"from\" = 2", new Predicate.Eq(FROM, new Value.IntVal(2))),
                Arguments.of("`from` = 2", new Predicate.Eq(FROM, new Value.IntVal(2))),
                Arguments.of("\"from\" IS NULL", new Predicate.IsNull(FROM)),
                Arguments.of("\"my column\" > 1", new Predicate.Gt(MY_COLUMN, new Value.IntVal(1))),
                Arguments.of("\"outer\".c = 1", new Predicate.Eq(OUTER_C, new Value.IntVal(1))),
                Arguments.of("\"outer\".\"c\" = 1", new Predicate.Eq(OUTER_C, new Value.IntVal(1))),
                Arguments.of("tags.\"select\" = 'x'", new Predicate.Quantified(MatchAction.ANY, tagIsX)));
    }

    @Test
    void quotedGeometryColumnIsResolvedBySpatialFunctions() {
        String filter = "ST_Intersects(\"the geom\", ST_MakeEnvelope(0, 0, 1, 1))";

        Predicate parsed = FilterParser.parse(filter, SCHEMA, Set.of(THE_GEOM));

        assertThat(Predicate.columns(parsed)).containsExactly(THE_GEOM);
    }

    @Test
    void unknownQuotedColumnIsReportedWithoutItsQuotes() {
        assertThatThrownBy(() -> parse("\"nope\" = 1"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("no such column: nope");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("referencesWithABlankIdentifier")
    void blankIdentifierIsReportedAsAFilterError(String filter) {
        assertThatThrownBy(() -> parse(filter))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("blank name in column reference");
    }

    static Stream<String> referencesWithABlankIdentifier() {
        return Stream.of("\"\" = 1", "\"outer\".\"\" = 1", "\"outer\"..c = 1");
    }

    @Test
    void filterReferenceTellsHowToNameSuchAColumn() {
        assertThat(FilterSyntax.reference())
                .contains("A column named as a SQL keyword, or with a space, goes in double quotes")
                .contains("\"from\" > 1");
    }

    private static Predicate parse(String filter) {
        return FilterParser.parse(filter, SCHEMA, Set.of());
    }

    /**
     * Columns: {@code from} and {@code my column} integers, an {@code outer} struct with an integer {@code c}, a
     * {@code tags} list of structs with a string {@code select}, and a {@code the geom} binary column.
     */
    private static ParquetSchema schema() {
        SchemaNode from = primitive("from", PrimitiveKind.INT32, Optional.empty());
        SchemaNode myColumn = primitive("my column", PrimitiveKind.INT32, Optional.empty());
        SchemaNode c = primitive("c", PrimitiveKind.INT32, Optional.empty());
        SchemaNode outer = group("outer", Repetition.OPTIONAL, c, Optional.empty());
        SchemaNode select = primitive("select", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
        SchemaNode element = group("element", Repetition.OPTIONAL, select, Optional.empty());
        SchemaNode list = group("list", Repetition.REPEATED, element, Optional.empty());
        SchemaNode tags = group("tags", Repetition.OPTIONAL, list, Optional.of(new LogicalType.ListType()));
        SchemaNode theGeom = primitive("the geom", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        List<SchemaNode> columns = List.of(from, myColumn, outer, tags, theGeom);
        return new ParquetSchema(new SchemaNode.Group("schema", Repetition.REQUIRED, columns, Optional.empty(), -1));
    }

    private static SchemaNode.Primitive primitive(String name, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logicalType, -1);
    }

    private static SchemaNode.Group group(
            String name, Repetition repetition, SchemaNode child, Optional<LogicalType> logicalType) {
        return new SchemaNode.Group(name, repetition, List.of(child), logicalType, -1);
    }
}
