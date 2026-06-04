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
package io.tileverse.parquetry.filter.explain;

import io.tileverse.parquetry.filter.RowRanges;

/**
 * The outcome of applying a single filter {@link Tier} to a row group.
 *
 * <p>{@link Eliminated} short-circuits the pipeline for the row group. {@link PassedAll} keeps the row group fully
 * intact for downstream tiers. {@link NarrowedTo} carries forward a refined set of surviving rows. {@link NotApplied}
 * means the tier had no opinion (e.g., missing statistics) and the row group continues unchanged.
 */
public sealed interface PruningDecision {

    /** The tier that produced this decision. */
    Tier tier();

    /** Human-readable reason, surfaced in ExplainPlan output. */
    String reason();

    /** The tier proved no row in this row group can match the predicate. */
    record Eliminated(Tier tier, String reason) implements PruningDecision {}

    /** The tier proved every row in this row group matches. Subsequent tiers may still narrow further. */
    record PassedAll(Tier tier, String reason) implements PruningDecision {}

    /** The tier narrowed the surviving row set to {@code ranges}. */
    record NarrowedTo(Tier tier, RowRanges ranges, String reason) implements PruningDecision {}

    /** The tier could not make a decision (e.g., statistics missing). The row group continues unchanged. */
    record NotApplied(Tier tier, String reason) implements PruningDecision {}
}
