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

import java.util.Optional;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.explain.PruningDecision;

/**
 * The rules deciding which reads learn page indexes from page headers. A read learns the pages of a row group only when
 * its predicate can drop some of them. The predicate needs a part evaluable by the page tier, and the statistics of the
 * row group must not already show each row passing that part.
 *
 * <p>A read keeps the entries of a row group only if that part drops a page with them. The learner of the read applies
 * this last rule, because it needs the entries.
 */
public final class PageLearning {

    private PageLearning() {}

    /**
     * The part of {@code normalized} evaluable by the page tier, or empty when a read with it can never drop a page.
     * See {@link PagePrunable#part}. A constant predicate has no such part. A read matching each row drops no page, and
     * a read matching no row fetches none.
     */
    public static Optional<Predicate> prunablePart(Predicate normalized) {
        Optional<Predicate> part = PagePrunable.part(normalized);
        return part.filter(prunable -> !(prunable instanceof Predicate.Always));
    }

    /**
     * Whether the statistics of a row group show each of its rows passing {@code prunablePart}. Such a row group learns
     * nothing, because the page tier would drop none of its pages. The NaN cells of a FLOAT or DOUBLE column are read
     * as absent. A learned page index has no NaN count, and without one the page tier drops no page for its NaN cells.
     *
     * @param prunablePart the part of a normalized predicate evaluable by the page tier
     * @param summaries the statistics of the columns of the row group
     * @param rowCount the number of rows of the row group
     */
    public static boolean allRowsPass(Predicate prunablePart, StatsEvaluator.TypedColumns summaries, long rowCount) {
        StatsEvaluator.TypedColumns freeOfNaN = path -> summaries.get(path).map(PageLearning::withoutNaNCells);
        PruningDecision decision = StatsEvaluator.evaluate(prunablePart, freeOfNaN, rowCount);
        return decision instanceof PruningDecision.PassedAll;
    }

    private static ColumnSummary withoutNaNCells(ColumnSummary summary) {
        return new ColumnSummary(
                summary.kind(),
                summary.min(),
                summary.max(),
                summary.nullCount(),
                summary.valueCount(),
                NaNCells.ABSENT);
    }
}
