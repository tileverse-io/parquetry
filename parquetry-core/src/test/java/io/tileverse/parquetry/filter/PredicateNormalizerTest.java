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
package io.tileverse.parquetry.filter;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class PredicateNormalizerTest {

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

    private static SchemaNode.Primitive primitive(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }
}
