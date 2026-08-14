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
package io.tileverse.parquetry.internal.filter;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class PredicateNormalizerTest {

    private static final ColumnPath LIST_ITEM = ColumnPath.of("tags", "list", "item");

    private static final ColumnPath REPEATED_TAG = ColumnPath.of("tag");

    @Test
    void notEqualBecomesNotEqAfterPushDown() {
        Predicate normalized =
                PredicateNormalizer.normalize(col("year").eq(2020).negate());
        assertThat(normalized).isEqualTo(new Predicate.NotEq(ColumnPath.of("year"), new Value.IntVal(2020)));
    }

    @Test
    void notLtBecomesGtEq() {
        Predicate normalized =
                PredicateNormalizer.normalize(col("year").lt(2020).negate());
        assertThat(normalized).isEqualTo(new Predicate.GtEq(ColumnPath.of("year"), new Value.IntVal(2020)));
    }

    @Test
    void doubleNotCancels() {
        Predicate input = col("year").eq(2020).negate().negate();
        assertThat(PredicateNormalizer.normalize(input))
                .isEqualTo(new Predicate.Eq(ColumnPath.of("year"), new Value.IntVal(2020)));
    }

    @Test
    void deMorganOnAnd() {
        Predicate input = col("year").eq(2020).and(col("country").eq("AR")).negate();
        Predicate expected = new Predicate.Or(List.of(
                new Predicate.NotEq(ColumnPath.of("year"), new Value.IntVal(2020)),
                new Predicate.NotEq(ColumnPath.of("country"), new Value.StringVal("AR"))));
        assertThat(PredicateNormalizer.normalize(input)).isEqualTo(expected);
    }

    @Test
    void deMorganOnOr() {
        Predicate input = col("year").eq(2020).or(col("country").eq("AR")).negate();
        Predicate expected = new Predicate.And(List.of(
                new Predicate.NotEq(ColumnPath.of("year"), new Value.IntVal(2020)),
                new Predicate.NotEq(ColumnPath.of("country"), new Value.StringVal("AR"))));
        assertThat(PredicateNormalizer.normalize(input)).isEqualTo(expected);
    }

    @Test
    void notIsNullBecomesIsNotNull() {
        assertThat(PredicateNormalizer.normalize(col("year").isNull().negate()))
                .isEqualTo(new Predicate.IsNotNull(ColumnPath.of("year")));
    }

    @Test
    void alwaysTrueAndCollapsesToOperand() {
        Predicate p =
                new Predicate.And(List.of(Predicate.ALWAYS_TRUE, col("year").eq(2020)));
        assertThat(PredicateNormalizer.normalize(p))
                .isEqualTo(new Predicate.Eq(ColumnPath.of("year"), new Value.IntVal(2020)));
    }

    @Test
    void alwaysFalseAndShortCircuits() {
        Predicate p =
                new Predicate.And(List.of(Predicate.ALWAYS_FALSE, col("year").eq(2020)));
        assertThat(PredicateNormalizer.normalize(p)).isEqualTo(Predicate.ALWAYS_FALSE);
    }

    @Test
    void alwaysTrueOrShortCircuits() {
        Predicate p =
                new Predicate.Or(List.of(Predicate.ALWAYS_TRUE, col("year").eq(2020)));
        assertThat(PredicateNormalizer.normalize(p)).isEqualTo(Predicate.ALWAYS_TRUE);
    }

    @Test
    void alwaysFalseOrCollapsesToOperand() {
        Predicate p =
                new Predicate.Or(List.of(Predicate.ALWAYS_FALSE, col("year").eq(2020)));
        assertThat(PredicateNormalizer.normalize(p))
                .isEqualTo(new Predicate.Eq(ColumnPath.of("year"), new Value.IntVal(2020)));
    }

    @Test
    void notAlwaysTrueIsAlwaysFalse() {
        assertThat(PredicateNormalizer.normalize(new Predicate.Not(Predicate.ALWAYS_TRUE)))
                .isEqualTo(Predicate.ALWAYS_FALSE);
    }

    @Test
    void nestedAndIsFlattened() {
        Predicate p =
                new Predicate.And(List.of(new Predicate.And(List.of(col("a").eq(1), col("b").eq(2))), col("c").eq(3)));
        Predicate normalized = PredicateNormalizer.normalize(p);
        assertThat(normalized).isInstanceOf(Predicate.And.class);
        assertThat(((Predicate.And) normalized).children()).hasSize(3);
    }

    @Test
    void nestedOrIsFlattened() {
        Predicate p =
                new Predicate.Or(List.of(new Predicate.Or(List.of(col("a").eq(1), col("b").eq(2))), col("c").eq(3)));
        Predicate normalized = PredicateNormalizer.normalize(p);
        assertThat(normalized).isInstanceOf(Predicate.Or.class);
        assertThat(((Predicate.Or) normalized).children()).hasSize(3);
    }

    @Test
    void singleChildAndUnwraps() {
        Predicate p = new Predicate.And(List.of(col("a").eq(1)));
        assertThat(PredicateNormalizer.normalize(p))
                .isEqualTo(new Predicate.Eq(ColumnPath.of("a"), new Value.IntVal(1)));
    }

    @Test
    void normalizeIsIdempotent() {
        Predicate input = col("year").eq(2020).and(col("country").eq("AR")).negate();
        Predicate once = PredicateNormalizer.normalize(input);
        Predicate twice = PredicateNormalizer.normalize(once);
        assertThat(twice).isEqualTo(once);
    }

    @Test
    void validateAcceptsCompatiblePredicate() {
        ParquetSchema schema = flatSchema();
        Predicate p = col("year").eq(2020).and(col("country").eq("AR"));
        PredicateNormalizer.validate(p, schema);
    }

    @Test
    void validateRejectsMissingColumn() {
        ParquetSchema schema = flatSchema();
        Predicate p = col("missing").eq(1);
        assertThatThrownBy(() -> PredicateNormalizer.validate(p, schema))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining("missing");
    }

    @Test
    void validateRejectsTypeMismatch() {
        ParquetSchema schema = flatSchema();
        Predicate p = col("year").eq("not-an-int");
        assertThatThrownBy(() -> PredicateNormalizer.validate(p, schema))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining("year");
    }

    @Test
    void validateRejectsGroupReference() {
        ParquetSchema schema = nestedSchema();
        Predicate p = col("addr").isNotNull();
        assertThatThrownBy(() -> PredicateNormalizer.validate(p, schema))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining("group");
    }

    @Test
    void validateAcceptsNestedColumnReference() {
        ParquetSchema schema = nestedSchema();
        PredicateNormalizer.validate(col("addr", "city").eq("Buenos Aires"), schema);
    }

    @Test
    void validateAcceptsDoubleBoundOnFloatColumn() {
        ParquetSchema schema = singleColumn("x", PrimitiveKind.FLOAT);
        PredicateNormalizer.validate(col("x").ltEq(1.5), schema);
    }

    @Test
    void validateAcceptsFloatBoundOnDoubleColumn() {
        ParquetSchema schema = singleColumn("y", PrimitiveKind.DOUBLE);
        PredicateNormalizer.validate(col("y").eq(1.5f), schema);
    }

    @Test
    void validateAcceptsLongBoundOnInt32Column() {
        ParquetSchema schema = singleColumn("n", PrimitiveKind.INT32);
        PredicateNormalizer.validate(col("n").gt(5_000L), schema);
    }

    @Test
    void validateAcceptsIntBoundOnInt64Column() {
        ParquetSchema schema = singleColumn("n", PrimitiveKind.INT64);
        PredicateNormalizer.validate(col("n").gt(5_000), schema);
    }

    @Test
    void validateRejectsBboxOnNonBinaryColumn() {
        ParquetSchema schema = flatSchema();
        Predicate p = col("year").intersects(Bbox.of2d(0, 0, 1, 1));
        assertThatThrownBy(() -> PredicateNormalizer.validate(p, schema))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining("binary");
    }

    @Test
    void validateRejectsInWithIncompatibleValue() {
        ParquetSchema schema = flatSchema();
        Predicate.In bad =
                new Predicate.In(ColumnPath.of("year"), List.of(new Value.IntVal(1), new Value.StringVal("oops")));
        assertThatThrownBy(() -> PredicateNormalizer.validate(bad, schema)).isInstanceOf(ParquetSchemaException.class);
    }

    @Test
    void rowIndexExcludedSurvivesNormalizationUnchangedInsideAnd() {
        Predicate deletes =
                new Predicate.RowIndexExcluded(ColumnPath.of("_pos"), SortedLongPositionSet.of(new long[] {1, 3}));
        Predicate input = col("year").eq(2020).and(deletes);

        Predicate normalized = PredicateNormalizer.normalize(input);

        assertThat(normalized)
                .isEqualTo(new Predicate.And(
                        List.of(new Predicate.Eq(ColumnPath.of("year"), new Value.IntVal(2020)), deletes)));
    }

    @Test
    void validateDoesNotCheckRowIndexExcludedSyntheticColumn() {
        ParquetSchema schema = flatSchema();
        Predicate deletes =
                new Predicate.RowIndexExcluded(ColumnPath.of("_pos"), SortedLongPositionSet.of(new long[] {0}));

        PredicateNormalizer.validate(col("year").eq(2020).and(deletes), schema);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("barePredicatesOverRepeatedLeaf")
    void validateRejectsBarePredicateOnListDescendantLeaf(String label, Predicate bare) {
        ParquetSchema schema = listSchema();
        assertThatThrownBy(() -> PredicateNormalizer.validate(bare, schema))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessage(multiValuedMessage(LIST_ITEM.dot()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("barePredicatesOverRepeatedPrimitive")
    void validateRejectsBarePredicateOnRepeatedPrimitiveLeaf(String label, Predicate bare) {
        ParquetSchema schema = repeatedPrimitiveSchema();
        assertThatThrownBy(() -> PredicateNormalizer.validate(bare, schema))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessage(multiValuedMessage(REPEATED_TAG.dot()));
    }

    @Test
    void validateRejectsBarePredicateOnRepeatedLeafNestedInsideConnective() {
        ParquetSchema schema = listSchema();
        Predicate bare =
                new Predicate.And(List.of(col("year").eq(2020), new Predicate.Eq(LIST_ITEM, new Value.StringVal("x"))));
        assertThatThrownBy(() -> PredicateNormalizer.validate(bare, schema))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessage(multiValuedMessage(LIST_ITEM.dot()));
    }

    @Test
    void validateAcceptsQuantifiedComparisonOverListDescendantLeaf() {
        ParquetSchema schema = listSchema();
        Predicate quantified =
                new Predicate.Quantified(MatchAction.ANY, new Predicate.Eq(LIST_ITEM, new Value.StringVal("x")));

        PredicateNormalizer.validate(quantified, schema);
    }

    @Test
    void validateAcceptsQuantifiedComparisonOverRepeatedPrimitiveLeaf() {
        ParquetSchema schema = repeatedPrimitiveSchema();
        Predicate quantified =
                new Predicate.Quantified(MatchAction.ALL, new Predicate.Gt(REPEATED_TAG, new Value.StringVal("a")));

        PredicateNormalizer.validate(quantified, schema);
    }

    @Test
    void validateStillRejectsTypeMismatchInsideQuantified() {
        ParquetSchema schema = repeatedPrimitiveSchema();
        Predicate quantified =
                new Predicate.Quantified(MatchAction.ANY, new Predicate.Eq(REPEATED_TAG, new Value.IntVal(3)));

        assertThatThrownBy(() -> PredicateNormalizer.validate(quantified, schema))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining("not compatible");
    }

    @Test
    void validateAcceptsBarePredicatesOnSingleValuedLeaves() {
        PredicateNormalizer.validate(col("year").eq(2020), listSchema());
        PredicateNormalizer.validate(col("addr", "city").eq("Buenos Aires"), nestedSchema());
        PredicateNormalizer.validate(col("addr", "city").isNotNull(), nestedSchema());
    }

    static Stream<Arguments> barePredicatesOverRepeatedLeaf() {
        return bareScalarPredicates(LIST_ITEM);
    }

    static Stream<Arguments> barePredicatesOverRepeatedPrimitive() {
        return bareScalarPredicates(REPEATED_TAG);
    }

    /** One instance of every scalar predicate the filter pipeline evaluates against a single physical leaf. */
    private static Stream<Arguments> bareScalarPredicates(ColumnPath leaf) {
        Value text = new Value.StringVal("x");
        return Stream.of(
                Arguments.of("Eq", new Predicate.Eq(leaf, text)),
                Arguments.of("NotEq", new Predicate.NotEq(leaf, text)),
                Arguments.of("Lt", new Predicate.Lt(leaf, text)),
                Arguments.of("LtEq", new Predicate.LtEq(leaf, text)),
                Arguments.of("Gt", new Predicate.Gt(leaf, text)),
                Arguments.of("GtEq", new Predicate.GtEq(leaf, text)),
                Arguments.of("In", new Predicate.In(leaf, List.of(text))),
                Arguments.of("IsNull", new Predicate.IsNull(leaf)),
                Arguments.of("IsNotNull", new Predicate.IsNotNull(leaf)),
                Arguments.of("BboxIntersects", new Predicate.Spatial.BboxIntersects(leaf, Bbox.of2d(0, 0, 1, 1))));
    }

    private static String multiValuedMessage(String dottedPath) {
        return "Column " + dottedPath + " is multi-valued (repeated, or nested under a repeated group); a bare"
                + " comparison has no single truth value over its elements. Wrap it in Predicate.Quantified with a"
                + " MatchAction (ANY, ALL, ONE).";
    }

    private static ParquetSchema flatSchema() {
        SchemaNode.Primitive year = primitive("year", PrimitiveKind.INT32);
        SchemaNode.Primitive country = primitive("country", PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(year, country), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema nestedSchema() {
        SchemaNode.Primitive city = primitive("city", PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Group addr = new SchemaNode.Group("addr", Repetition.OPTIONAL, List.of(city), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(addr), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /** A three-level LIST group ({@code tags.list.item}) beside a single-valued flat column. */
    private static ParquetSchema listSchema() {
        SchemaNode.Primitive item = primitive("item", PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Group list = new SchemaNode.Group("list", Repetition.REPEATED, List.of(item), Optional.empty(), -1);
        SchemaNode.Group tags = new SchemaNode.Group(
                "tags", Repetition.OPTIONAL, List.of(list), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Group root = new SchemaNode.Group(
                "root",
                Repetition.REQUIRED,
                List.of(tags, primitive("year", PrimitiveKind.INT32)),
                Optional.empty(),
                -1);
        return new ParquetSchema(root);
    }

    /** A repeated primitive leaf ({@code tag}) with no LIST annotation above it. */
    private static ParquetSchema repeatedPrimitiveSchema() {
        SchemaNode.Primitive tag = new SchemaNode.Primitive(
                "tag", Repetition.REPEATED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group(
                "root",
                Repetition.REQUIRED,
                List.of(tag, primitive("year", PrimitiveKind.INT32)),
                Optional.empty(),
                -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema singleColumn(String name, PrimitiveKind kind) {
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(primitive(name, kind)), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive primitive(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }
}
