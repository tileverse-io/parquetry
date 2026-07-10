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
package io.tileverse.parquetry.dataset.explain;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.explain.ExplainPlan;

/**
 * One file's place in a {@link DatasetExplainPlan}: where it is, whether the query kept or skipped it, why, its row
 * count when cheaply known, and - for a kept file - the core single-file plan ({@code rowGroups}, present only for
 * {@link Outcome#KEEP}; holds execution stats after {@code explainAnalyze}).
 */
public record FileExplain(
        String location, Outcome outcome, String reason, OptionalLong recordCount, Optional<ExplainPlan> rowGroups) {

    public FileExplain {
        Objects.requireNonNull(location, "location");
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(recordCount, "recordCount");
        Objects.requireNonNull(rowGroups, "rowGroups");
    }
}
