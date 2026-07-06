/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import org.junit.jupiter.api.Test;

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
 * Verifies that {@code --filter} accepts logical column paths: a comparison on a list/map element resolves to the
 * physical leaf and is wrapped in {@link Predicate.Quantified} with {@link MatchAction#ANY}, while struct and flat
 * leaves resolve to bare comparisons.
 */
class LogicalPathFilterTest {

    private final ParquetSchema schema = sampleSchema();

    private Predicate parse(String filter) {
        return FilterParser.parse(filter, schema, Set.of());
    }

    @Test
    void wrapsRepeatedLeafComparisonInAnyQuantifier() {
        Predicate predicate = parse("addresses.locality = 'Berlin'");

        assertThat(predicate).isInstanceOf(Predicate.Quantified.class);
        Predicate.Quantified quantified = (Predicate.Quantified) predicate;
        assertThat(quantified.match()).isEqualTo(MatchAction.ANY);

        Predicate.Eq expected = new Predicate.Eq(
                ColumnPath.of("addresses", "list", "element", "locality"), new Value.StringVal("Berlin"));
        assertThat(quantified.leaf()).isEqualTo(expected);
    }

    @Test
    void leavesStructLeafComparisonUnwrapped() {
        Predicate predicate = parse("brand.names.primary = 'X'");

        Predicate.Eq expected = new Predicate.Eq(ColumnPath.of("brand", "names", "primary"), new Value.StringVal("X"));
        assertThat(predicate).isEqualTo(expected);
    }

    @Test
    void leavesFlatLeafComparisonUnwrapped() {
        Predicate predicate = parse("pop > 1000");

        assertThat(predicate).isInstanceOf(Predicate.Gt.class);
        Predicate.Gt greater = (Predicate.Gt) predicate;
        assertThat(greater.col()).isEqualTo(ColumnPath.of("pop"));
        assertThat(greater.v()).isEqualTo(new Value.LongVal(1000));
    }

    @Test
    void rejectsUnknownLogicalPath() {
        assertThatThrownBy(() -> parse("addresses.nope = 'x'"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("no such column");
    }

    private static ParquetSchema sampleSchema() {
        SchemaNode pop = primitive("pop", PrimitiveKind.INT64, Optional.empty());
        SchemaNode brand = struct("brand", struct("names", string("primary")));
        SchemaNode addresses = list("addresses", struct("element", string("locality")));
        SchemaNode.Group root = group("root", Repetition.REQUIRED, List.of(pop, brand, addresses));
        return new ParquetSchema(root);
    }

    private static SchemaNode struct(String name, SchemaNode... children) {
        return group(name, Repetition.OPTIONAL, List.of(children));
    }

    private static SchemaNode list(String name, SchemaNode elementGroup) {
        SchemaNode.Group repeatedList = group("list", Repetition.REPEATED, List.of(elementGroup));
        return group(name, Repetition.OPTIONAL, List.of(repeatedList));
    }

    private static SchemaNode.Group group(String name, Repetition repetition, List<SchemaNode> children) {
        return new SchemaNode.Group(name, repetition, children, Optional.empty(), -1);
    }

    private static SchemaNode string(String name) {
        return primitive(name, PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
    }

    private static SchemaNode primitive(String name, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logicalType, -1);
    }
}
