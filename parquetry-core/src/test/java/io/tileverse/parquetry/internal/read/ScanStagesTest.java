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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.read.ScanStages.Stage;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/** Exercises the split of a predicate over a schema of fixed-width, variable-length and repeated leaves. */
class ScanStagesTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath HEIGHT = ColumnPath.of("height");
    private static final ColumnPath UID = ColumnPath.of("uid");
    private static final ColumnPath NAME = ColumnPath.of("name");
    private static final ColumnPath OTHER = ColumnPath.of("other");
    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final ColumnPath FOOTPRINT = ColumnPath.of("footprint");
    private static final ColumnPath TAG = ColumnPath.of("tags", "list", "element");
    private static final ParquetSchema SCHEMA = schema();

    private static final Bbox BOX = Bbox.of2d(0, 0, 1, 1);
    private static final Predicate TALL = Pred.col(HEIGHT).gt(1.0);
    private static final Predicate RECENT = Pred.col(ID).gt(2L);
    private static final Predicate NAMED = Pred.col(NAME).eq("a");
    private static final Predicate IN_BOX = Pred.col(GEOMETRY).bboxIntersects(BOX);
    private static final Predicate TAGGED =
            new Predicate.Quantified(MatchAction.ANY, Pred.col(TAG).eq("x"));
    private static final Predicate UNDELETED = undeleted("_pos");

    @Test
    void comparisonsThenSpatialAreTwo() {
        List<Stage> stages = stagesOf(Pred.and(TALL, IN_BOX));

        assertThat(ownLeavesOf(stages)).containsExactly(Set.of(HEIGHT), Set.of(GEOMETRY));
    }

    @Test
    void threeKindsKeepTheirOrder() {
        List<Stage> stages = stagesOf(Pred.and(IN_BOX, NAMED, TALL));

        assertThat(ownLeavesOf(stages)).containsExactly(Set.of(HEIGHT), Set.of(NAME), Set.of(GEOMETRY));
    }

    @Test
    void loneSpatialIsOneStage() {
        List<Stage> stages = stagesOf(IN_BOX);

        assertThat(stages).hasSize(1);
        assertThat(stages.getFirst().leaves()).containsExactly(GEOMETRY);
    }

    @Test
    void notAnAndIsOneStage() {
        Predicate either = Pred.or(NAMED, TALL);

        List<Stage> stages = stagesOf(either);

        assertThat(stages).hasSize(1);
        Stage only = stages.getFirst();
        assertThat(only.predicate()).isEqualTo(either);
        assertThat(only.leaves()).containsExactlyInAnyOrder(NAME, HEIGHT);
        assertThat(only.ownLeaves()).containsExactlyInAnyOrder(NAME, HEIGHT);
    }

    @Test
    void stringsWithoutANumberAreOne() {
        Predicate bothNames = Pred.and(NAMED, Pred.col(OTHER).eq("b"));

        List<Stage> stages = stagesOf(bothNames);

        assertThat(stages).hasSize(1);
        assertThat(stages.getFirst().predicate()).isEqualTo(bothNames);
    }

    @Test
    void stringThenSpatialAreTwo() {
        List<Stage> stages = stagesOf(Pred.and(NAMED, IN_BOX));

        assertThat(ownLeavesOf(stages)).containsExactly(Set.of(NAME), Set.of(GEOMETRY));
    }

    @Test
    void twoSpatialTestsAreOneStage() {
        Predicate inFootprint = Pred.col(FOOTPRINT).bboxIntersects(BOX);

        List<Stage> stages = stagesOf(Pred.and(IN_BOX, inFootprint));

        assertThat(stages).hasSize(1);
        assertThat(stages.getFirst().leaves()).containsExactlyInAnyOrder(GEOMETRY, FOOTPRINT);
    }

    // The element is a string, and only the repeated rule keeps its condition out of the second stage.
    @Test
    void repeatedLeafJoinsTheFirstStage() {
        Predicate tallAndTagged = Pred.and(TALL, TAGGED);

        List<Stage> stages = stagesOf(tallAndTagged);

        assertThat(stages).hasSize(1);
        Stage only = stages.getFirst();
        assertThat(only.predicate()).isEqualTo(tallAndTagged);
        assertThat(only.leaves()).containsExactlyInAnyOrder(HEIGHT, TAG);
    }

    @Test
    void repeatedLeafInAnOrStaysFirst() {
        List<Stage> stages = stagesOf(Pred.and(TALL, Pred.or(TAGGED, NAMED)));

        assertThat(stages).hasSize(1);
    }

    @Test
    void repeatedGeometryStaysFirst() {
        Predicate exactTest = Predicate.geometryFilter(geometryFilterOn(TAG));

        List<Stage> stages = stagesOf(Pred.and(TALL, exactTest));

        assertThat(stages).hasSize(1);
    }

    @Test
    void orWithAStringWaits() {
        List<Stage> stages = stagesOf(Pred.and(TALL, Pred.or(NAMED, RECENT)));

        assertThat(ownLeavesOf(stages)).containsExactly(Set.of(HEIGHT), Set.of(NAME, ID));
    }

    @Test
    void fixedLenBinaryIsFixedWidth() {
        Predicate uidEquals = Pred.col(UID).eq(new byte[16]);

        List<Stage> stages = stagesOf(Pred.and(NAMED, uidEquals));

        assertThat(ownLeavesOf(stages)).containsExactly(Set.of(UID), Set.of(NAME));
    }

    @Test
    void sharedLeafKeepsItsFirstStage() {
        Predicate sharesId = Pred.or(NAMED, Pred.col(ID).lt(0L));

        List<Stage> stages = stagesOf(Pred.and(RECENT, sharesId));

        assertThat(stages).hasSize(2);
        Stage second = stages.getLast();
        assertThat(second.leaves()).containsExactlyInAnyOrder(NAME, ID);
        assertThat(second.ownLeaves()).containsExactly(NAME);
    }

    @Test
    void laterStageMayOwnNothing() {
        Predicate hasGeometry = Pred.col(GEOMETRY).isNotNull();

        List<Stage> stages = stagesOf(Pred.and(hasGeometry, IN_BOX));

        assertThat(stages).hasSize(2);
        Stage second = stages.getLast();
        assertThat(second.leaves()).containsExactly(GEOMETRY);
        assertThat(second.ownLeaves()).isEmpty();
    }

    @Test
    void positionsAloneMergeForward() {
        Predicate undeletedInBox = Pred.and(UNDELETED, IN_BOX);

        List<Stage> stages = stagesOf(undeletedInBox);

        assertThat(stages).hasSize(1);
        Stage only = stages.getFirst();
        assertThat(only.predicate()).isEqualTo(undeletedInBox);
        assertThat(only.leaves()).containsExactly(GEOMETRY);
    }

    @Test
    void mergeTakesOnlyTheNextStage() {
        List<Stage> stages = stagesOf(Pred.and(UNDELETED, NAMED, IN_BOX));

        assertThat(ownLeavesOf(stages)).containsExactly(Set.of(NAME), Set.of(GEOMETRY));
        assertThat(stages.getFirst().predicate()).isEqualTo(Pred.and(UNDELETED, NAMED));
    }

    @Test
    void positionConditionStaysFirst() {
        List<Stage> stages = stagesOf(Pred.and(TALL, Pred.or(UNDELETED, NAMED)));

        assertThat(stages).hasSize(1);
        assertThat(stages.getFirst().leaves()).containsExactlyInAnyOrder(HEIGHT, NAME);
    }

    @Test
    void standInLeafStaysFirst() {
        Predicate synthesizedOnly = Pred.and(UNDELETED, undeleted("_other_pos"));

        List<Stage> stages = ScanStages.of(synthesizedOnly, Set.of(ID), SCHEMA).stages();

        assertThat(stages).hasSize(1);
        Stage only = stages.getFirst();
        assertThat(only.predicate()).isEqualTo(synthesizedOnly);
        assertThat(only.leaves()).containsExactly(ID);
        assertThat(only.ownLeaves()).containsExactly(ID);
        assertThat(only.schema()).isEqualTo(SCHEMA.project(Set.of(ID)));
    }

    @Test
    void negatedSpatialRunsLast() {
        List<Stage> stages = stagesOf(Pred.and(Pred.not(IN_BOX), NAMED));

        assertThat(ownLeavesOf(stages)).containsExactly(Set.of(NAME), Set.of(GEOMETRY));
    }

    @ParameterizedTest
    @MethodSource("exactTests")
    void exactTestsRunLast(Predicate exactTest) {
        List<Stage> stages = stagesOf(Pred.and(exactTest, NAMED));

        assertThat(ownLeavesOf(stages)).containsExactly(Set.of(NAME), Set.of(GEOMETRY));
    }

    private static Stream<Predicate> exactTests() {
        Predicate exactTest = Predicate.geometryFilter(geometryFilterOn(GEOMETRY));
        return Stream.of(exactTest, Pred.not(exactTest));
    }

    @Test
    void orHoldingASpatialTestIsSecond() {
        Predicate inBoxOrRecent = Pred.or(IN_BOX, RECENT);

        List<Stage> stages = stagesOf(Pred.and(TALL, inBoxOrRecent));

        assertThat(ownLeavesOf(stages)).containsExactly(Set.of(HEIGHT), Set.of(GEOMETRY, ID));
        assertThat(stages.getLast().predicate()).isEqualTo(inBoxOrRecent);
    }

    @Test
    void stageOfSeveralIsAnAnd() {
        List<Stage> stages = stagesOf(Pred.and(TALL, IN_BOX, RECENT));

        assertThat(stages.getFirst().predicate()).isEqualTo(Pred.and(TALL, RECENT));
        assertThat(stages.getLast().predicate()).isEqualTo(IN_BOX);
    }

    @Test
    void stageSchemaHoldsItsLeaves() {
        Predicate sharesHeight = Pred.or(NAMED, Pred.col(HEIGHT).lt(0.0));

        List<Stage> stages = stagesOf(Pred.and(TALL, sharesHeight));

        assertThat(stages.getFirst().schema()).isEqualTo(SCHEMA.project(Set.of(HEIGHT)));
        assertThat(stages.getLast().schema()).isEqualTo(SCHEMA.project(Set.of(HEIGHT, NAME)));
    }

    private static List<Stage> stagesOf(Predicate predicate) {
        return ScanStages.of(predicate, physicalLeaves(predicate), SCHEMA).stages();
    }

    /** The leaves read by {@code predicate}, without the synthesized columns. */
    private static Set<ColumnPath> physicalLeaves(Predicate predicate) {
        Set<ColumnPath> leaves = new HashSet<>(Predicate.columns(predicate));
        Predicate.rowPositionColumns(predicate).forEach(leaves::remove);
        return leaves;
    }

    private static List<Set<ColumnPath>> ownLeavesOf(List<Stage> stages) {
        return stages.stream().map(Stage::ownLeaves).toList();
    }

    private static Predicate undeleted(String positionColumn) {
        ColumnPath position = ColumnPath.of(positionColumn);
        return new Predicate.RowIndexExcluded(position, SortedLongPositionSet.of(new long[] {1L}));
    }

    private static GeometryFilter<MemorySegment> geometryFilterOn(ColumnPath column) {
        return new GeometryFilter<>() {
            @Override
            public ColumnPath column() {
                return column;
            }

            @Override
            public Optional<Predicate.Spatial> pruningPredicate() {
                return Optional.empty();
            }

            @Override
            public MemorySegment decode(MemorySegment wkb) {
                return wkb;
            }

            @Override
            public boolean matches(MemorySegment geometry) {
                return true;
            }
        };
    }

    private static ParquetSchema schema() {
        SchemaNode.Primitive element = WriteFixtures.optionalLeaf("element", PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Group list =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group tags = new SchemaNode.Group(
                "tags", Repetition.OPTIONAL, List.of(list), Optional.of(new LogicalType.ListType()), -1);
        return WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf("id", PrimitiveKind.INT64),
                WriteFixtures.requiredLeaf("height", PrimitiveKind.DOUBLE),
                WriteFixtures.requiredLeaf("uid", PrimitiveKind.FIXED_LEN_BYTE_ARRAY, OptionalInt.of(16), null),
                WriteFixtures.requiredLeaf("name", PrimitiveKind.BYTE_ARRAY),
                WriteFixtures.requiredLeaf("other", PrimitiveKind.BYTE_ARRAY),
                WriteFixtures.requiredLeaf("geometry", PrimitiveKind.BYTE_ARRAY),
                WriteFixtures.requiredLeaf("footprint", PrimitiveKind.BYTE_ARRAY),
                tags);
    }
}
