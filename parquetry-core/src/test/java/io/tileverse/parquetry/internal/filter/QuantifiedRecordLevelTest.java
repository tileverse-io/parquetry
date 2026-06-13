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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.Validity;
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
 * Existential / universal / exactly-one evaluation of a {@link Predicate.Quantified} over a repeated leaf's element
 * values. The fixture is a one-row batch with an {@code addresses LIST<STRUCT{locality}>} column; the quantified leaf
 * is the physical path {@code addresses.list.element.locality}.
 */
class QuantifiedRecordLevelTest {

    private static final ColumnPath ADDRESSES = ColumnPath.of("addresses");
    private static final ColumnPath LEAF = ColumnPath.of("addresses", "list", "element", "locality");

    @Test
    void anyMatchesWhenSomeElementSatisfies() {
        ParquetRecord row = rowWithLocalities("Berlin", "Bonn");

        assertThat(evaluate(MatchAction.ANY, "Bonn", row)).isTrue();
        assertThat(evaluate(MatchAction.ANY, "Cologne", row)).isFalse();
    }

    @Test
    void allMatchesOnlyWhenEveryElementSatisfies() {
        assertThat(evaluate(MatchAction.ALL, "Berlin", rowWithLocalities("Berlin", "Berlin")))
                .isTrue();
        assertThat(evaluate(MatchAction.ALL, "Berlin", rowWithLocalities("Berlin", "Bonn")))
                .isFalse();
    }

    @Test
    void oneMatchesOnlyWhenExactlyOneElementSatisfies() {
        assertThat(evaluate(MatchAction.ONE, "Berlin", rowWithLocalities("Berlin", "Bonn")))
                .isTrue();
        assertThat(evaluate(MatchAction.ONE, "Berlin", rowWithLocalities("Berlin", "Berlin")))
                .isFalse();
    }

    @Test
    void emptyListIsFalseForAnyAndVacuouslyTrueForAll() {
        ParquetRecord row = rowWithLocalities();

        assertThat(evaluate(MatchAction.ANY, "Berlin", row)).isFalse();
        assertThat(evaluate(MatchAction.ALL, "Berlin", row)).isTrue();
    }

    @Test
    void nullElementsAreExcludedFromTheQuantifiedUniverse() {
        // A present struct element with a null locality leaf contributes no value: multiValue excludes nulls.
        // The quantified universe is therefore the single non-null "Berlin". ALL and ONE must not count the null
        // element. This pins the intended "nulls excluded from the universe" behavior against silent drift.
        ParquetRecord row = rowWithNullableLocalities("Berlin", null);

        assertThat(evaluate(MatchAction.ANY, "Berlin", row)).isTrue();
        assertThat(evaluate(MatchAction.ALL, "Berlin", row)).isTrue();
        assertThat(evaluate(MatchAction.ONE, "Berlin", row)).isTrue();
    }

    @Test
    void multiValueReturnsTheElementLocalitiesInOrder() {
        ParquetRecord row = rowWithLocalities("Berlin", "Bonn", "Cologne");

        List<String> localities = row.multiValue(LEAF).stream()
                .map(QuantifiedRecordLevelTest::decode)
                .toList();
        assertThat(localities).containsExactly("Berlin", "Bonn", "Cologne");
    }

    private static String decode(Object value) {
        MemorySegment segment = (MemorySegment) value;
        return new String(segment.toArray(JAVA_BYTE), StandardCharsets.UTF_8);
    }

    private static boolean evaluate(MatchAction match, String locality, ParquetRecord row) {
        Predicate predicate = new Predicate.Quantified(match, new Predicate.Eq(LEAF, new Value.StringVal(locality)));
        return RecordLevelEvaluator.test(predicate, RecordAccessors.of(row));
    }

    private static ParquetRecord rowWithLocalities(String... localities) {
        ParquetRecordBatch batch = addressesBatch(localities);
        return batch.materialize(0);
    }

    /** A row whose {@code addresses} list holds one present struct per entry; a {@code null} entry is a null leaf. */
    private static ParquetRecord rowWithNullableLocalities(String... localities) {
        int count = localities.length;
        BinaryVector localityVec = nullableStringVector(localities);
        StructVector element =
                new StructVector(Map.of(ColumnPath.of("locality"), localityVec), Validity.allValid(count), count);
        int[] offsets = {0, count};
        ListVector addresses = new ListVector(offsets, element, Validity.allValid(1), 1);
        ParquetRecordBatch batch =
                new DefaultParquetRecordBatch(addressesSchema(), Map.of(ADDRESSES, addresses), 1, Arena.ofConfined());
        return batch.materialize(0);
    }

    /** A one-row batch whose {@code addresses} list holds one struct per locality. */
    private static ParquetRecordBatch addressesBatch(String... localities) {
        int count = localities.length;
        BinaryVector localityVec = stringVector(localities);
        StructVector element =
                new StructVector(Map.of(ColumnPath.of("locality"), localityVec), Validity.allValid(count), count);
        int[] offsets = {0, count};
        ListVector addresses = new ListVector(offsets, element, Validity.allValid(1), 1);
        return new DefaultParquetRecordBatch(addressesSchema(), Map.of(ADDRESSES, addresses), 1, Arena.ofConfined());
    }

    private static BinaryVector stringVector(String... values) {
        MemorySegment[] segments = new MemorySegment[values.length];
        for (int i = 0; i < values.length; i++) {
            segments[i] = MemorySegment.ofArray(values[i].getBytes(StandardCharsets.UTF_8));
        }
        return BinaryVector.materialized(segments, Validity.allValid(values.length));
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
