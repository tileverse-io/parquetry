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
package io.tileverse.parquetry.filter;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

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
        List<String> headers = new ArrayList<>();
        headers.add("RG");
        headers.add("Rows");
        for (Tier t : TIER_ORDER) {
            headers.add(TIER_HEADER.get(t));
        }
        headers.add("Outcome");

        List<List<String>> rows = new ArrayList<>();
        rows.add(headers);
        for (RowGroupPlan rg : plan.rowGroups()) {
            rows.add(rowCells(rg));
        }
        int[] widths = columnWidths(rows);

        StringBuilder out = new StringBuilder();
        appendRow(out, rows.get(0), widths);
        for (int i = 1; i < rows.size(); i++) {
            appendRow(out, rows.get(i), widths);
        }
        return out.toString();
    }

    private static List<String> rowCells(RowGroupPlan rg) {
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
        cells.add(
                switch (rg.outcome()) {
                    case ELIMINATED -> "skip";
                    case PARTIAL ->
                        rg.survivingRows().map(r -> r.totalRows() + " rows").orElse("partial");
                    case FULL -> "all";
                });
        return cells;
    }

    private static String cell(PruningDecision d, long rowCount) {
        if (d == null) {
            return "-";
        }
        return switch (d) {
            case PruningDecision.Eliminated _ -> "ELIM";
            case PruningDecision.PassedAll _ -> "passed";
            case PruningDecision.NarrowedTo n -> "NARROW " + n.ranges().totalRows() + "/" + rowCount;
            case PruningDecision.NotApplied _ -> "n/a";
        };
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
