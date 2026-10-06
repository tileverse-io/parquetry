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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

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
 * Counts a {@link Predicate.Quantified} over a multi-row batch via per-row evaluation. The fixture is a
 * {@code addresses LIST<STRUCT{locality}>} column; the quantified leaf is the physical path
 * {@code addresses.list.element.locality}. The vectorized evaluator must agree with the record-level existential
 * semantics on which rows match.
 */
class VectorizedQuantifiedTest {

    private static final ColumnPath ADDRESSES = ColumnPath.of("addresses");
    private static final ColumnPath LEAF = ColumnPath.of("addresses", "list", "element", "locality");
    private static final ColumnPath STAMPS = ColumnPath.of("stamps");
    private static final ColumnPath STAMP = ColumnPath.of("stamps", "list", "element");
    private static final int INT96_BYTES = 12;
    private static final long JULIAN_DAY_OF_EPOCH = 2_440_588L;

    @Test
    void anySetsExactlyTheRowsWithAMatchingElement() {
        ParquetRecordBatch batch = batch(
                localities("Berlin", "Bonn"), // match
                localities("Cologne"), // no match
                localities(), // empty list, ANY is false
                localities("Bonn")); // match

        BitSet matches = VectorizedPredicateEvaluator.eval(quantified(MatchAction.ANY, "Bonn"), batch);

        assertThat(matches.get(0)).isTrue();
        assertThat(matches.get(1)).isFalse();
        assertThat(matches.get(2)).isFalse();
        assertThat(matches.get(3)).isTrue();
        assertThat(matches.cardinality()).isEqualTo(2);
    }

    @Test
    void allSetsTheRowsWhereEveryElementMatchesAndIsVacuouslyTrueForEmpty() {
        ParquetRecordBatch batch = batch(
                localities("Berlin", "Berlin"), // all match
                localities("Berlin", "Bonn"), // not all match
                localities()); // empty list, ALL is vacuously true

        BitSet matches = VectorizedPredicateEvaluator.eval(quantified(MatchAction.ALL, "Berlin"), batch);

        assertThat(matches.get(0)).isTrue();
        assertThat(matches.get(1)).isFalse();
        assertThat(matches.get(2)).isTrue();
        assertThat(matches.cardinality()).isEqualTo(2);
    }

    /**
     * INT96 cells are legacy timestamps of 12 bytes. They compare as such under a timestamp annotation too, which the
     * format reserves for INT64 cells.
     */
    @Test
    void int96ElementsCompareAsLegacyTimestampsUnderATimestampAnnotation() {
        LocalDateTime noon = LocalDateTime.of(2020, 1, 1, 12, 0);
        ParquetRecordBatch batch = stampsBatch(
                stamps(noon.minusHours(1), noon.plusHours(1)), // one element before noon
                stamps(noon.plusHours(2))); // none
        Predicate anyBeforeNoon =
                new Predicate.Quantified(MatchAction.ANY, new Predicate.Lt(STAMP, new Value.TimestampVal(noon, true)));

        BitSet matches = VectorizedPredicateEvaluator.eval(anyBeforeNoon, batch);

        assertThat(matches.get(0)).isTrue();
        assertThat(matches.get(1)).isFalse();
    }

    private static Predicate quantified(MatchAction match, String locality) {
        return new Predicate.Quantified(match, new Predicate.Eq(LEAF, new Value.StringVal(locality)));
    }

    private static String[] localities(String... values) {
        return values;
    }

    /** A batch whose {@code addresses} list holds one struct per locality in each row. */
    private static ParquetRecordBatch batch(String[]... rows) {
        int rowCount = rows.length;
        int totalElements = 0;
        for (String[] row : rows) {
            totalElements += row.length;
        }

        MemorySegment[] segments = new MemorySegment[totalElements];
        int[] offsets = new int[rowCount + 1];
        int cursor = 0;
        for (int row = 0; row < rowCount; row++) {
            offsets[row] = cursor;
            for (String locality : rows[row]) {
                segments[cursor] = MemorySegment.ofArray(locality.getBytes(StandardCharsets.UTF_8));
                cursor++;
            }
        }
        offsets[rowCount] = cursor;

        BinaryVector localityVec = BinaryVector.materialized(segments, Validity.allValid(totalElements));
        StructVector element = new StructVector(
                Map.of(ColumnPath.of("locality"), localityVec), Validity.allValid(totalElements), totalElements);
        ListVector addresses = new ListVector(offsets, element, Validity.allValid(rowCount), rowCount);
        return new DefaultParquetRecordBatch(
                addressesSchema(), Map.of(ADDRESSES, addresses), rowCount, Arena.ofConfined());
    }

    private static LocalDateTime[] stamps(LocalDateTime... values) {
        return values;
    }

    /** A batch whose {@code stamps} list holds one INT96 cell per timestamp in each row. */
    private static ParquetRecordBatch stampsBatch(LocalDateTime[]... rows) {
        int rowCount = rows.length;
        List<MemorySegment> cells = new ArrayList<>();
        int[] offsets = new int[rowCount + 1];
        for (int row = 0; row < rowCount; row++) {
            offsets[row] = cells.size();
            for (LocalDateTime stamp : rows[row]) {
                cells.add(int96Cell(stamp));
            }
        }
        offsets[rowCount] = cells.size();

        MemorySegment[] values = cells.toArray(MemorySegment[]::new);
        Int96Vector elements = Int96Vector.materialized(values, Validity.allValid(values.length));
        ListVector stamps = new ListVector(offsets, elements, Validity.allValid(rowCount), rowCount);
        return new DefaultParquetRecordBatch(stampsSchema(), Map.of(STAMPS, stamps), rowCount, Arena.ofConfined());
    }

    /** An INT96 cell: eight little-endian bytes of nanoseconds within the day, then four of the Julian day. */
    private static MemorySegment int96Cell(LocalDateTime stamp) {
        ByteBuffer cell = ByteBuffer.allocate(INT96_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        cell.putLong(stamp.toLocalTime().toNanoOfDay());
        cell.putInt((int) (stamp.toLocalDate().toEpochDay() + JULIAN_DAY_OF_EPOCH));
        return MemorySegment.ofArray(cell.array());
    }

    /** Schema: root -> stamps LIST -> list (repeated) -> element INT96 annotated as a nanosecond timestamp. */
    private static ParquetSchema stampsSchema() {
        LogicalType annotation = new LogicalType.Timestamp(true, LogicalType.TimeUnit.NANOS);
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT96, OptionalInt.empty(), Optional.of(annotation), -1);
        SchemaNode.Group list =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        Optional<LogicalType> listType = Optional.of(new LogicalType.ListType());
        SchemaNode.Group stamps = new SchemaNode.Group("stamps", Repetition.OPTIONAL, List.of(list), listType, -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(stamps), Optional.empty(), -1);
        return new ParquetSchema(root);
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
