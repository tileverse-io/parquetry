/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.util.List;
import java.util.OptionalLong;

/**
 * File-dimension totals for a {@link DatasetExplainPlan}. File counts are always known; row totals are present only
 * when every relevant file reports its record count, otherwise empty (a backend that cannot count cheaply leaves it
 * unknown rather than paying to find out).
 */
public record Totals(
        int filesTotal, int filesKept, int filesSkipped, OptionalLong rowsTotal, OptionalLong rowsSkipped) {

    public static Totals from(List<FileExplain> files) {
        int kept = 0;
        int skipped = 0;
        for (FileExplain file : files) {
            if (file.outcome() == Outcome.KEEP) {
                kept++;
            } else {
                skipped++;
            }
        }
        OptionalLong rowsTotal = sumRecordCounts(files, false);
        OptionalLong rowsSkipped = sumRecordCounts(files, true);
        return new Totals(files.size(), kept, skipped, rowsTotal, rowsSkipped);
    }

    private static OptionalLong sumRecordCounts(List<FileExplain> files, boolean skippedOnly) {
        long sum = 0L;
        for (FileExplain file : files) {
            if (skippedOnly && file.outcome() != Outcome.SKIP) {
                continue;
            }
            if (file.recordCount().isEmpty()) {
                return OptionalLong.empty();
            }
            sum += file.recordCount().getAsLong();
        }
        return OptionalLong.of(sum);
    }
}
