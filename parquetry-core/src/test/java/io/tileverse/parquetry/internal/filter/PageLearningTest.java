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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

class PageLearningTest {

    private static final long ROW_COUNT = 1_000;

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath X = ColumnPath.of("x");
    private static final ColumnPath XMIN = ColumnPath.of("bbox", "xmin");
    private static final ColumnPath XMAX = ColumnPath.of("bbox", "xmax");
    private static final ColumnPath YMIN = ColumnPath.of("bbox", "ymin");
    private static final ColumnPath YMAX = ColumnPath.of("bbox", "ymax");

    private static final Predicate IN_THE_WORLD = col("geometry").bboxIntersects(Bbox.of2d(-180, -90, 180, 90));

    // A spatial leaf has no page statistics of its own.
    @Test
    void noPrunableLeafMeansNoPart() {
        assertThat(PageLearning.prunablePart(IN_THE_WORLD)).isEmpty();
    }

    // A read matching each row drops no page.
    @Test
    void alwaysTrueHasNoPart() {
        assertThat(PageLearning.prunablePart(Predicate.ALWAYS_TRUE)).isEmpty();
    }

    @Test
    void andKeepsItsPrunableChildren() {
        Predicate idAbove = col(ID).gt(950L);
        Predicate predicate = new Predicate.And(List.of(IN_THE_WORLD, idAbove));

        assertThat(PageLearning.prunablePart(predicate)).contains(new Predicate.And(List.of(idAbove)));
    }

    // A world render over the covering columns of a GeoParquet 1.1 file. Their statistics record no NaN count.
    @Test
    void allRowsPassOnAWorldBox() {
        Predicate worldRender = new Predicate.And(List.of(
                col(XMIN).ltEq(180.0),
                col(XMAX).gtEq(-180.0),
                col(YMIN).ltEq(90.0),
                col(YMAX).gtEq(-90.0),
                IN_THE_WORLD));
        StatsEvaluator.TypedColumns covering = lookup(Map.of(
                XMIN, floatSummary(-10f, 5f),
                XMAX, floatSummary(-5f, 10f),
                YMIN, floatSummary(40f, 45f),
                YMAX, floatSummary(42f, 50f)));
        Predicate part = PageLearning.prunablePart(worldRender).orElseThrow();

        assertThat(PageLearning.allRowsPass(part, covering, ROW_COUNT)).isTrue();
    }

    // Without a NaN count the STATS tier cannot show each row passing, and the page tier drops no page for NaN cells.
    @Test
    void nanDoesNotBlockAllRowsPass() {
        Predicate above = col(X).gt(-1.0);
        StatsEvaluator.TypedColumns summaries = lookup(Map.of(X, doubleSummary(0.0, 10.0)));

        PruningDecision stats = StatsEvaluator.evaluate(above, summaries, ROW_COUNT);

        assertThat(stats).isNotInstanceOf(PruningDecision.PassedAll.class);
        assertThat(PageLearning.allRowsPass(above, summaries, ROW_COUNT)).isTrue();
    }

    // IS NOT NULL drops the pages holding only nulls, and a row group counting nulls may hold such pages.
    @Test
    void isNotNullWithNullsDoesNotPass() {
        Predicate notNull = col(ID).isNotNull();
        StatsEvaluator.TypedColumns summaries = lookup(Map.of(ID, longSummary(0, 999, 3)));

        assertThat(PageLearning.allRowsPass(notNull, summaries, ROW_COUNT)).isFalse();
    }

    private static StatsEvaluator.TypedColumns lookup(Map<ColumnPath, ColumnSummary> summaries) {
        return path -> Optional.ofNullable(summaries.get(path));
    }

    /** The statistics of a FLOAT column without nulls, with no NaN count. */
    private static ColumnSummary floatSummary(float min, float max) {
        Optional<Value> lower = Optional.of(new Value.FloatVal(min));
        Optional<Value> upper = Optional.of(new Value.FloatVal(max));
        return summary(PrimitiveKind.FLOAT, lower, upper, 0);
    }

    /** The statistics of a DOUBLE column without nulls, with no NaN count. */
    private static ColumnSummary doubleSummary(double min, double max) {
        Optional<Value> lower = Optional.of(new Value.DoubleVal(min));
        Optional<Value> upper = Optional.of(new Value.DoubleVal(max));
        return summary(PrimitiveKind.DOUBLE, lower, upper, 0);
    }

    private static ColumnSummary longSummary(long min, long max, long nullCount) {
        Optional<Value> lower = Optional.of(new Value.LongVal(min));
        Optional<Value> upper = Optional.of(new Value.LongVal(max));
        return summary(PrimitiveKind.INT64, lower, upper, nullCount);
    }

    private static ColumnSummary summary(PrimitiveKind kind, Optional<Value> min, Optional<Value> max, long nullCount) {
        OptionalLong nulls = OptionalLong.of(nullCount);
        OptionalLong values = OptionalLong.of(ROW_COUNT);
        return ColumnSummary.of(kind, min, max, nulls, values, NaNCells.POSSIBLE);
    }
}
