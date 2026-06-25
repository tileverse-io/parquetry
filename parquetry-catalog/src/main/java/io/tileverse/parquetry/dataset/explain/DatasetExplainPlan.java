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
package io.tileverse.parquetry.dataset.explain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.explain.ExplainPlan;

/**
 * A dataset-level explain plan: the file dimension a query prunes (the kept and skipped files), then each kept file's
 * core single-file plan. Returned by {@code ParquetDataset.explain}/{@code explainAnalyze}.
 */
public record DatasetExplainPlan(Predicate predicate, List<FileExplain> files, Totals totals) {

    public DatasetExplainPlan {
        Objects.requireNonNull(predicate, "predicate");
        files = List.copyOf(files);
        Objects.requireNonNull(totals, "totals");
    }

    /** A compact ASCII report: a header, the file table, then each kept file's core {@link ExplainPlan} table. */
    public String toAsciiTable() {
        StringBuilder out = new StringBuilder();
        out.append("predicate: ").append(predicate).append('\n');
        out.append(totals.filesKept())
                .append(" kept, ")
                .append(totals.filesSkipped())
                .append(" skipped");
        totals.rowsSkipped()
                .ifPresent(rows -> out.append("  (").append(rows).append(" rows pruned without opening a file)"));
        out.append("\n\n");
        out.append(String.format("%-40s %-7s %12s   %s%n", "location", "outcome", "rows", "reason"));
        for (FileExplain file : files) {
            OptionalLong recordCount = file.recordCount();
            String rows = recordCount.isPresent() ? Long.toString(recordCount.getAsLong()) : "?";
            out.append(String.format("%-40s %-7s %12s   %s%n", file.location(), file.outcome(), rows, file.reason()));
        }
        for (FileExplain file : files) {
            Optional<ExplainPlan> rowGroups = file.rowGroups();
            if (rowGroups.isPresent()) {
                out.append('\n').append(file.location()).append(":\n");
                out.append(rowGroups.get().toAsciiTable());
            }
        }
        return out.toString();
    }

    /** A single JSON object mirroring the ASCII report; embeds each kept file's core {@link ExplainPlan} JSON. */
    public String toJson() {
        StringBuilder out = new StringBuilder();
        out.append("{\"predicate\":\"").append(escape(predicate.toString())).append("\",");
        out.append("\"totals\":{")
                .append("\"filesTotal\":")
                .append(totals.filesTotal())
                .append(",\"filesKept\":")
                .append(totals.filesKept())
                .append(",\"filesSkipped\":")
                .append(totals.filesSkipped())
                .append(",\"rowsTotal\":")
                .append(jsonNumber(totals.rowsTotal()))
                .append(",\"rowsSkipped\":")
                .append(jsonNumber(totals.rowsSkipped()))
                .append("},");
        out.append("\"files\":[");
        for (int i = 0; i < files.size(); i++) {
            FileExplain file = files.get(i);
            if (i > 0) {
                out.append(',');
            }
            out.append("{\"location\":\"").append(escape(file.location())).append('"');
            out.append(",\"outcome\":\"").append(file.outcome()).append('"');
            out.append(",\"reason\":\"").append(escape(file.reason())).append('"');
            out.append(",\"recordCount\":").append(jsonNumber(file.recordCount()));
            Optional<ExplainPlan> rowGroups = file.rowGroups();
            if (rowGroups.isPresent()) {
                out.append(",\"rowGroups\":").append(rowGroups.get().toJson());
            }
            out.append('}');
        }
        out.append("]}");
        return out.toString();
    }

    private static String jsonNumber(OptionalLong value) {
        return value.isPresent() ? Long.toString(value.getAsLong()) : "null";
    }

    private static String escape(String text) {
        return text.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }
}
