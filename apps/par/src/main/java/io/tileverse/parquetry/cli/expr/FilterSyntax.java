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
package io.tileverse.parquetry.cli.expr;

import java.util.List;

/**
 * Renders the human-readable reference of everything {@code --filter} accepts. The spatial function lists come from
 * {@link SpatialFilterTranslator}, which keeps them in step with the relations the translator actually recognizes. The
 * scalar, logical, and set operators are matched by parser token type rather than an enumerable set; this class lists
 * them directly.
 */
public final class FilterSyntax {

    private static final String COMPARISONS = "= != <> < <= > >=";
    private static final String LOGICAL = "AND, OR, NOT";
    private static final String SETS_AND_RANGES = "IN (...), NOT IN (...), BETWEEN x AND y, IS NULL, IS NOT NULL";

    private FilterSyntax() {}

    /** A multi-line listing of the supported {@code --filter} predicates, suitable for printing to a terminal. */
    public static String reference() {
        String spatial = String.join(", ", SpatialFilterTranslator.relationFunctionNames());
        String constructors = String.join(", ", SpatialFilterTranslator.queryConstructorNames());
        List<String> lines = List.of(
                "Supported --filter predicates (a SQL WHERE expression):",
                "  Comparisons : " + COMPARISONS,
                "  Logical     : " + LOGICAL,
                "  Sets/ranges : " + SETS_AND_RANGES,
                "  Spatial     : " + spatial,
                "  Query geom  : " + constructors,
                "",
                "Notes:",
                "  - The left side of a comparison is a column; the right side is a literal"
                        + " (number, 'string', or true/false).",
                "  - Contains/Within and Covers/CoveredBy honor argument order; ST_DWithin takes a trailing distance.",
                "  - Spatial tests run in the file's native CRS; the query geometry is assumed to be in that CRS"
                        + " (no reprojection).",
                "",
                "Example:",
                "  par cat data.parquet --filter \"pop > 1000000"
                        + " AND ST_Intersects(geom, ST_MakeEnvelope(-60, -35, -58, -33))\"");
        return String.join(System.lineSeparator(), lines);
    }
}
