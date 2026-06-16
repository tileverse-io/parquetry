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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.observe.RowGroupRead;

/** Renders an {@link ExplainPlan} as a compact fixed-width ASCII table. */
final class AsciiTableRenderer {

    private static final List<Tier> TIER_ORDER =
            List.of(Tier.STATS, Tier.DICTIONARY, Tier.COLUMN_INDEX, Tier.BLOOM_FILTER, Tier.RECORD_LEVEL);

    private static final Map<Tier, String> TIER_HEADER = headerMap();

    private final ExplainPlan plan;

    AsciiTableRenderer(ExplainPlan plan) {
        this.plan = plan;
    }

    String render() {
        StringBuilder out = new StringBuilder();
        out.append(tierConfigurationLine()).append('\n');
        appendTable(out);
        plan.execution().ifPresent(stats -> out.append(totalsLine(stats)).append('\n'));
        return out.toString();
    }

    private String tierConfigurationLine() {
        TierConfiguration config = TierConfiguration.from(plan);
        List<String> labels = new ArrayList<>();
        for (Tier tier : TierConfiguration.ORDER) {
            labels.add(config.isOff(tier) ? tier.name() + "(off)" : tier.name());
        }
        return "Tiers: " + String.join(", ", labels);
    }

    private void appendTable(StringBuilder out) {
        boolean withExecution = plan.execution().isPresent();
        boolean withTime = withExecution && hasAnyTimings();

        List<List<String>> rows = new ArrayList<>();
        rows.add(headerCells(withExecution, withTime));
        for (RowGroupPlan rg : plan.rowGroups()) {
            rows.add(rowCells(rg, withExecution, withTime));
        }

        int[] widths = columnWidths(rows);
        for (List<String> row : rows) {
            appendRow(out, row, widths);
        }
    }

    private static List<String> headerCells(boolean withExecution, boolean withTime) {
        List<String> headers = new ArrayList<>();
        headers.add("RG");
        headers.add("Rows");
        for (Tier t : TIER_ORDER) {
            headers.add(TIER_HEADER.get(t));
        }
        headers.add("Outcome");
        if (withExecution) {
            headers.add("actual");
            headers.add("bytes");
            if (withTime) {
                headers.add("time");
            }
        }
        return headers;
    }

    private static List<String> rowCells(RowGroupPlan rg, boolean withExecution, boolean withTime) {
        Map<Tier, PruningDecision> byTier = new EnumMap<>(Tier.class);
        for (PruningDecision d : rg.tiers()) {
            byTier.put(d.tier(), d);
        }
        List<String> cells = new ArrayList<>();
        cells.add(Integer.toString(rg.index()));
        cells.add(Long.toString(rg.rowCount()));
        for (Tier t : TIER_ORDER) {
            cells.add(cell(byTier.get(t), rg.rowCount()));
        }
        cells.add(outcomeCell(rg));
        if (withExecution) {
            appendExecutionCells(cells, rg, withTime);
        }
        return cells;
    }

    private static void appendExecutionCells(List<String> cells, RowGroupPlan rg, boolean withTime) {
        if (rg.execution().isEmpty()) {
            cells.add("-");
            cells.add("-");
            if (withTime) {
                cells.add("-");
            }
            return;
        }
        RowGroupRead read = rg.execution().orElseThrow();
        cells.add(Long.toString(read.rowsMatched()));
        cells.add(Long.toString(read.fetch().totalBytes()));
        if (withTime) {
            cells.add(read.timings().map(t -> formatDuration(t.decodeNanos())).orElse("-"));
        }
    }

    private static String outcomeCell(RowGroupPlan rg) {
        return switch (rg.outcome()) {
            case ELIMINATED -> "skip";
            case PARTIAL -> rg.survivingRows().map(r -> r.totalRows() + " rows").orElse("partial");
            case FULL -> "all";
            case MATCHED -> "matched";
        };
    }

    private String totalsLine(QueryStats stats) {
        return "Total: "
                + stats.rowsMatched()
                + " rows, "
                + stats.totalFetch().totalBytes()
                + " bytes, "
                + formatDuration(stats.wallClockNanos());
    }

    private boolean hasAnyTimings() {
        if (plan.execution().map(stats -> stats.cpuTimings().isPresent()).orElse(false)) {
            return true;
        }
        return plan.rowGroups().stream()
                .anyMatch(rg ->
                        rg.execution().map(read -> read.timings().isPresent()).orElse(false));
    }

    private static String cell(PruningDecision d, long rowCount) {
        if (d == null) {
            return "-";
        }
        return switch (d) {
            case PruningDecision.Eliminated _ -> "ELIM";
            case PruningDecision.PassedAll _ -> "passed";
            case PruningDecision.NarrowedTo n -> "NARROW " + n.ranges().totalRows() + "/" + rowCount;
            case PruningDecision.Inconclusive _ -> "kept";
            case PruningDecision.NotApplied _ -> "n/a";
        };
    }

    /**
     * Formats a nanosecond duration as a short ASCII string in microseconds or milliseconds, picking the unit that
     * reads naturally for the magnitude. Sub-millisecond values render in microseconds.
     */
    private static String formatDuration(long nanos) {
        long micros = nanos / 1_000L;
        if (micros < 1_000L) {
            return micros + "us";
        }
        return (nanos / 1_000_000L) + "ms";
    }

    private static int[] columnWidths(List<List<String>> rows) {
        int columnCount = rows.get(0).size();
        int[] widths = new int[columnCount];
        for (List<String> row : rows) {
            for (int c = 0; c < columnCount; c++) {
                widths[c] = Math.max(widths[c], row.get(c).length());
            }
        }
        return widths;
    }

    private static void appendRow(StringBuilder out, List<String> row, int[] widths) {
        for (int c = 0; c < row.size(); c++) {
            if (c > 0) {
                out.append("  ");
            }
            out.append(pad(row.get(c), widths[c]));
        }
        out.append('\n');
    }

    private static String pad(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        StringBuilder sb = new StringBuilder(width);
        sb.append(s);
        while (sb.length() < width) {
            sb.append(' ');
        }
        return sb.toString();
    }

    private static Map<Tier, String> headerMap() {
        Map<Tier, String> m = new EnumMap<>(Tier.class);
        m.put(Tier.STATS, "Stats");
        m.put(Tier.DICTIONARY, "Dict");
        m.put(Tier.COLUMN_INDEX, "ColIdx");
        m.put(Tier.BLOOM_FILTER, "Bloom");
        m.put(Tier.RECORD_LEVEL, "Record");
        return m;
    }
}
