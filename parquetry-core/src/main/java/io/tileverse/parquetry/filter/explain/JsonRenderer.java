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

/** Minimal hand-rolled JSON emitter for {@link ExplainPlan}. Avoids pulling Jackson into core's runtime classpath. */
final class JsonRenderer {

    private JsonRenderer() {}

    static String render(ExplainPlan plan) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("{\"originalPredicate\":");
        appendString(sb, plan.originalPredicate().toString());
        sb.append(",\"normalizedPredicate\":");
        appendString(sb, plan.normalizedPredicate().toString());
        sb.append(",\"estimatedRowsScanned\":").append(plan.estimatedRowsScanned());
        sb.append(",\"estimatedBytesRead\":").append(plan.estimatedBytesRead());
        sb.append(",\"rowGroups\":[");
        for (int i = 0; i < plan.rowGroups().size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            appendRowGroup(sb, plan.rowGroups().get(i));
        }
        sb.append("]}");
        return sb.toString();
    }

    private static void appendRowGroup(StringBuilder sb, RowGroupPlan rg) {
        sb.append("{\"index\":").append(rg.index());
        sb.append(",\"rowCount\":").append(rg.rowCount());
        sb.append(",\"outcome\":\"").append(rg.outcome().name()).append('"');
        sb.append(",\"tiers\":[");
        for (int i = 0; i < rg.tiers().size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            appendDecision(sb, rg.tiers().get(i));
        }
        sb.append("]");
        rg.survivingRows().ifPresent(r -> appendRowRanges(sb, r));
        sb.append('}');
    }

    private static void appendDecision(StringBuilder sb, PruningDecision d) {
        sb.append("{\"tier\":\"").append(d.tier().name()).append('"');
        sb.append(",\"kind\":\"").append(d.getClass().getSimpleName()).append('"');
        sb.append(",\"reason\":");
        appendString(sb, d.reason());
        if (d instanceof PruningDecision.NarrowedTo n) {
            sb.append(",\"survivingRows\":").append(n.ranges().totalRows());
        }
        sb.append('}');
    }

    private static void appendRowRanges(StringBuilder sb, RowRanges r) {
        sb.append(",\"survivingRanges\":[");
        for (int i = 0; i < r.ranges().size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            RowRanges.Range range = r.ranges().get(i);
            sb.append('[')
                    .append(range.first())
                    .append(',')
                    .append(range.last())
                    .append(']');
        }
        sb.append(']');
    }

    private static void appendString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }
}
