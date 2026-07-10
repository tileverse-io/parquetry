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
package io.tileverse.parquetry.filter.prune;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;

class FilePrunerTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath GEOM = ColumnPath.of("geom");
    private static final ColumnPath NAME = ColumnPath.of("name");

    private static BoundingBox box(double xmin, double ymin, double xmax, double ymax) {
        return new BoundingBox(
                xmin,
                xmax,
                ymin,
                ymax,
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty());
    }

    private static boolean eliminated(PruningDecision d) {
        return d instanceof PruningDecision.Eliminated;
    }

    @Test
    void numericPredicateOutsideRangeEliminates() {
        FileStats stats = FileStats.builder()
                .recordCount(100)
                .column(
                        ID,
                        new ColumnStatistics(
                                Optional.of(new Value.LongVal(0)),
                                Optional.of(new Value.LongVal(10)),
                                OptionalLong.of(0)))
                .build();
        Predicate p = new Predicate.Gt(ID, new Value.LongVal(50));
        assertThat(eliminated(FilePruner.evaluate(p, stats))).isTrue();
    }

    @Test
    void numericPredicateInsideRangeKeeps() {
        FileStats stats = FileStats.builder()
                .recordCount(100)
                .column(
                        ID,
                        new ColumnStatistics(
                                Optional.of(new Value.LongVal(0)),
                                Optional.of(new Value.LongVal(100)),
                                OptionalLong.of(0)))
                .build();
        Predicate p = new Predicate.Gt(ID, new Value.LongVal(50));
        assertThat(eliminated(FilePruner.evaluate(p, stats))).isFalse();
    }

    @Test
    void stringPredicateInsideRangeKeeps() {
        FileStats stats = stringNameStats();
        Predicate p = new Predicate.Lt(NAME, new Value.StringVal("k"));
        assertThat(eliminated(FilePruner.evaluate(p, stats))).isFalse();
    }

    @Test
    void stringPredicateBelowMinEliminates() {
        FileStats stats = stringNameStats();
        Predicate p = new Predicate.Lt(NAME, new Value.StringVal("a"));
        assertThat(eliminated(FilePruner.evaluate(p, stats))).isTrue();
    }

    @Test
    void stringPredicateAboveMaxEliminates() {
        FileStats stats = stringNameStats();
        Predicate p = new Predicate.Gt(NAME, new Value.StringVal("z"));
        assertThat(eliminated(FilePruner.evaluate(p, stats))).isTrue();
    }

    private static FileStats stringNameStats() {
        return FileStats.builder()
                .recordCount(100)
                .column(
                        NAME,
                        new ColumnStatistics(
                                Optional.of(new Value.StringVal("d")),
                                Optional.of(new Value.StringVal("m")),
                                OptionalLong.of(0)))
                .build();
    }

    @Test
    void disjointSpatialEliminates() {
        FileStats stats = FileStats.builder()
                .recordCount(100)
                .geometryBounds(GEOM, box(-10, -10, 0, 0))
                .build();
        Predicate p = new Predicate.Spatial.BboxIntersects(GEOM, Bbox.of2d(5, 5, 10, 10));
        assertThat(eliminated(FilePruner.evaluate(p, stats))).isTrue();
    }

    @Test
    void intersectingSpatialKeeps() {
        FileStats stats = FileStats.builder()
                .recordCount(100)
                .geometryBounds(GEOM, box(-10, -10, 5, 5))
                .build();
        Predicate p = new Predicate.Spatial.BboxIntersects(GEOM, Bbox.of2d(0, 0, 10, 10));
        assertThat(eliminated(FilePruner.evaluate(p, stats))).isFalse();
    }

    @Test
    void antimeridianWrapKeeps() {
        FileStats stats = FileStats.builder()
                .recordCount(100)
                .geometryBounds(GEOM, box(170, -10, -170, 10))
                .build();
        Predicate p = new Predicate.Spatial.BboxIntersects(GEOM, Bbox.of2d(0, 0, 10, 10));
        assertThat(eliminated(FilePruner.evaluate(p, stats))).isFalse();
    }

    @Test
    void alwaysTrueKeeps() {
        FileStats stats = FileStats.builder().recordCount(100).build();
        assertThat(eliminated(FilePruner.evaluate(Predicate.ALWAYS_TRUE, stats)))
                .isFalse();
    }
}
