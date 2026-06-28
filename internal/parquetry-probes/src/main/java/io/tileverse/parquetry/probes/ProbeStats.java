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
package io.tileverse.parquetry.probes;

import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.observe.SpillStats;

/** Renders the aggregated {@link QueryStats} of an {@code --analyze} probe run, with the spill tally up front. */
final class ProbeStats {

    private ProbeStats() {}

    static String format(QueryStats stats) {
        SpillStats spill = stats.spillStats();
        StringBuilder out = new StringBuilder();
        out.append("parquetry analyze (summed over every read):\n");
        out.append("  rows decoded:    ").append(stats.rowsDecoded()).append('\n');
        out.append("  rows matched:    ").append(stats.rowsMatched()).append('\n');
        out.append("  row groups read: ")
                .append(stats.rowGroupsRead())
                .append('/')
                .append(stats.rowGroupsTotal())
                .append('\n');
        out.append("  pages decoded:   ").append(stats.pagesDecoded()).append('\n');
        out.append("  pages pruned:    ").append(stats.pagesPruned()).append('\n');
        out.append("  spill:           ").append(formatSpill(spill));
        return out.toString();
    }

    private static String formatSpill(SpillStats spill) {
        if (!spill.hasActivity()) {
            return "none";
        }
        StringBuilder out = new StringBuilder();
        out.append(spill.batchesSpilled())
                .append(" batches / ")
                .append(spill.bytesSpilled())
                .append(" bytes spilled, ")
                .append(spill.batchesRestored())
                .append(" restored in ")
                .append(spill.restoreNanos() / 1_000_000L)
                .append("ms");
        if (spill.spillsRejectedDiskFull() > 0) {
            out.append(", ").append(spill.spillsRejectedDiskFull()).append(" parked (disk full)");
        }
        return out.toString();
    }
}
