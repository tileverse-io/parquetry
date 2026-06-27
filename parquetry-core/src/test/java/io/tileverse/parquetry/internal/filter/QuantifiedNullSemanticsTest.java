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
package io.tileverse.parquetry.internal.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Pins the null-element semantics of {@link Predicate.Quantified} at record level. A null element of a repeated leaf is
 * a present, non-matching member of the universe: it never makes ANY true, and it counts against ALL and ONE the same
 * way any non-matching element does. This mirrors GeoTools and SQL {@code = ALL} / {@code = ANY} behavior.
 */
class QuantifiedNullSemanticsTest {

    private static final ColumnPath LEAF = ColumnPath.of("tags", "list", "element");
    private static final ColumnPath ADDRESSES = ColumnPath.of("addresses");
    private static final ColumnPath STRUCT_LEAF = ColumnPath.of("addresses", "list", "element", "locality");

    @Test
    void anyMatchesWhenSomeElementMatches() {
        assertThat(evaluate(MatchAction.ANY, "a", "a", "b", "c")).isTrue();
    }

    @Test
    void anyFailsWhenNoElementMatches() {
        assertThat(evaluate(MatchAction.ANY, "z", "a", "b", "c")).isFalse();
    }

    @Test
    void allFailsWhenSomeElementDiffers() {
        assertThat(evaluate(MatchAction.ALL, "a", "a", "b", "c")).isFalse();
    }

    @Test
    void allMatchesWhenEveryElementMatches() {
        assertThat(evaluate(MatchAction.ALL, "a", "a", "a")).isTrue();
    }

    @Test
    void oneMatchesWhenExactlyOneElementMatches() {
        assertThat(evaluate(MatchAction.ONE, "a", "a", "b", "c")).isTrue();
    }

    @Test
    void oneFailsWhenTwoElementsMatch() {
        assertThat(evaluate(MatchAction.ONE, "b", "a", "b", "b")).isFalse();
    }

    @Test
    void allFailsWhenANullElementIsPresent() {
        assertThat(evaluate(MatchAction.ALL, "a", "a", null))
                .as("a null list element is a present, non-matching member of the universe")
                .isFalse();
    }

    @Test
    void anyIsUnaffectedByANullElement() {
        assertThat(evaluate(MatchAction.ANY, "a", "a", null)).isTrue();
    }

    @Test
    void oneCountsOnlyNonNullMatches() {
        assertThat(evaluate(MatchAction.ONE, "a", "a", null))
                .as("a null element does not match; exactly one non-null match satisfies ONE")
                .isTrue();
    }

    @Test
    void anyFailsOnEmptyUniverse() {
        assertThat(evaluate(MatchAction.ANY, "a")).isFalse();
    }

    @Test
    void allIsVacuouslyTrueOnEmptyUniverse() {
        assertThat(evaluate(MatchAction.ALL, "a")).isTrue();
    }

    @Test
    void oneFailsOnEmptyUniverse() {
        assertThat(evaluate(MatchAction.ONE, "a")).isFalse();
    }

    @Test
    void listOfStructWithNullLeafCountsTheSlotAsANonMatch() {
        ParquetRecord row = listOfStructRow("Berlin", null);

        assertThat(evaluateStruct(MatchAction.ANY, "Berlin", row)).isTrue();
        assertThat(evaluateStruct(MatchAction.ALL, "Berlin", row))
                .as("a present struct with a null leaf is a non-matching member; ALL is false")
                .isFalse();
        assertThat(evaluateStruct(MatchAction.ONE, "Berlin", row))
                .as("exactly one non-null leaf matches; ONE is true")
                .isTrue();
    }

    private static boolean evaluateStruct(MatchAction match, String wanted, ParquetRecord row) {
        Predicate leaf = new Predicate.Eq(STRUCT_LEAF, new Value.StringVal(wanted));
        Predicate quantified = new Predicate.Quantified(match, leaf);
        return RecordLevelEvaluator.test(quantified, RecordAccessors.of(row));
    }

    private static boolean evaluate(MatchAction match, String wanted, Object... universe) {
        Predicate leaf = new Predicate.Eq(LEAF, new Value.StringVal(wanted));
        Predicate quantified = new Predicate.Quantified(match, leaf);
        return RecordLevelEvaluator.test(quantified, universeRow(universe));
    }

    /**
     * A {@link RecordLevelEvaluator.RecordAccessor} whose repeated leaf holds {@code universe} (which may include null
     * elements). {@code multiValue} returns the null-free flattening per the public contract; {@code multiValueSize}
     * reports the full universe size including nulls.
     */
    private static RecordLevelEvaluator.RecordAccessor universeRow(Object... universe) {
        List<Object> all = Arrays.asList(universe);
        List<Object> nonNull = new ArrayList<>();
        for (Object element : all) {
            if (element != null) {
                nonNull.add(element);
            }
        }
        return new RecordLevelEvaluator.RecordAccessor() {
            @Override
            public Object value(ColumnPath path) {
                return null;
            }

            @Override
            public List<Object> multiValue(ColumnPath leafPath) {
                return List.copyOf(nonNull);
            }

            @Override
            public int multiValueSize(ColumnPath leafPath) {
                return all.size();
            }
        };
    }

    /**
     * A one-row {@code addresses LIST<STRUCT{locality}>} record. Every list element is a present struct; a {@code null}
     * locality is a present struct element whose leaf value is null.
     */
    private static ParquetRecord listOfStructRow(String... localities) {
        int count = localities.length;
        BinaryVector localityVector = nullableStringVector(localities);
        StructVector element =
                new StructVector(Map.of(ColumnPath.of("locality"), localityVector), Validity.allValid(count), count);
        int[] offsets = {0, count};
        ListVector addresses = new ListVector(offsets, element, Validity.allValid(1), 1);
        ParquetRecordBatch batch =
                new DefaultParquetRecordBatch(addressesSchema(), Map.of(ADDRESSES, addresses), 1, Arena.ofConfined());
        return batch.materialize(0);
    }

    private static BinaryVector nullableStringVector(String... values) {
        MemorySegment[] segments = new MemorySegment[values.length];
        BitSet present = new BitSet(values.length);
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            if (value == null) {
                segments[i] = MemorySegment.ofArray(new byte[0]);
            } else {
                segments[i] = MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8));
                present.set(i);
            }
        }
        return BinaryVector.materialized(segments, Validity.of(present, values.length));
    }

    /** Schema: root -> addresses LIST -> list (repeated) -> element struct -> locality STRING. */
    private static ParquetSchema addressesSchema() {
        SchemaNode.Primitive locality = new SchemaNode.Primitive(
                "locality", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group element =
                new SchemaNode.Group("element", Repetition.OPTIONAL, List.of(locality), Optional.empty(), -1);
        SchemaNode.Group list =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group addresses =
                new SchemaNode.Group("addresses", Repetition.OPTIONAL, List.of(list), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(addresses), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
