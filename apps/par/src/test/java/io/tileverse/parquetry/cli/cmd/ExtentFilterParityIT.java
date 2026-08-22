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
package io.tileverse.parquetry.cli.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.cli.support.CliRunner;
import io.tileverse.parquetry.cli.support.Fixtures;

/**
 * Proves the interop claim behind the extent form: the same {@code --filter} text, handed to {@code par} and to DuckDB
 * over the same GeoParquet file, selects the rows the relation calls for. DuckDB reads the WKB geometry column through
 * {@code read_parquet} as a GEOMETRY with the spatial extension loaded, and accepts the extent grammar as written.
 *
 * <p>Each case names the rows it must select, which is what keeps the comparison from passing on two engines that
 * happen to agree on nothing. Every selection is non-empty, and no two relations select the same rows for the same
 * reason.
 *
 * <p>The negated relation is here for the row with no geometry, where the two engines could most easily part ways: a
 * SQL WHERE clause drops a NULL result, and both engines leave that row out of ST_Disjoint as well as ST_Intersects.
 *
 * <p>The fixture's vertical line sits at a longitude float32 cannot hold, hence the file's FLOAT bbox covering reaches
 * past it on both sides. Two cases turn on that strip: an equality against the line's real bounding box, which the
 * wider covering does not match, and an intersection with a box that ends inside the strip, which the wider covering
 * does match. A reader answering from the covering alone gets both wrong, in opposite directions.
 */
class ExtentFilterParityIT {

    private static final Pattern ID = Pattern.compile("\"id\"\\s*:\\s*(\\d+)");
    private static final String QUERY_BOX = "ST_MakeEnvelope(0, 0, 10, 10)";

    /** The vertical line's longitude as SQL text; both engines parse it to the same double. */
    private static final String LINE_X = String.valueOf(Fixtures.EXTENT_LINE_X);

    /**
     * A longitude between the covering's rounded-down xmin ({@code 2.3815999031066895}) and the line itself: right of
     * everything the covering rules out, left of the geometry.
     */
    private static final String INSIDE_ROUNDING_STRIP = "2.38159995";

    static Stream<Arguments> extentCases() {
        return Stream.of(
                Arguments.of("ST_Intersects(ST_Extent(geometry), " + QUERY_BOX + ")", List.of(1, 2, 3, 4)),
                Arguments.of("ST_Intersects(ST_Envelope(geometry), " + QUERY_BOX + ")", List.of(1, 2, 3, 4)),
                Arguments.of("ST_Covers(ST_Extent(geometry), ST_Extent(ST_MakeEnvelope(10, 10, 11, 11)))", List.of(3)),
                Arguments.of("ST_CoveredBy(ST_Extent(geometry), " + QUERY_BOX + ")", List.of(1, 2, 4)),
                Arguments.of(
                        "ST_Equals(ST_Extent(geometry), ST_Extent(ST_MakeEnvelope(" + LINE_X + ", 2, " + LINE_X
                                + ", 8)))",
                        List.of(4)),
                Arguments.of(
                        "ST_Intersects(ST_Extent(geometry), ST_MakeEnvelope(0, 4, " + INSIDE_ROUNDING_STRIP + ", 6))",
                        List.of(2)),
                Arguments.of("ST_Disjoint(ST_Extent(geometry), " + QUERY_BOX + ")", List.of(5)),
                Arguments.of("ST_Intersects(geometry, " + QUERY_BOX + ")", List.of(1, 2, 4)),
                Arguments.of(
                        "ST_Intersects(geometry, ST_Extent(ST_GeomFromText('POLYGON((0 0, 20 0, 0 20, 0 0))')))",
                        List.of(1, 2, 3, 4)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("extentCases")
    void parAndDuckDbSelectTheExpectedRows(String filter, List<Integer> expected, @TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("extent-cases.parquet");
        Fixtures.writeExtentCases(file);

        assertThat(parIds(file, filter)).as("rows par selected").isEqualTo(expected);
        assertThat(duckDbIds(file, filter)).as("rows DuckDB selected").isEqualTo(expected);
    }

    private List<Integer> parIds(Path file, String filter) {
        CliRunner.Result result = CliRunner.run("cat", file.toString(), "--columns", "id", "--filter", filter);
        assertThat(result.exitCode()).as("par stderr: %s", result.stderr()).isZero();
        List<Integer> ids = new ArrayList<>();
        Matcher matcher = ID.matcher(result.stdout());
        while (matcher.find()) {
            ids.add(Integer.valueOf(matcher.group(1)));
        }
        return ids.stream().sorted().toList();
    }

    private List<Integer> duckDbIds(Path file, String filter) throws Exception {
        String sql = "SELECT id FROM read_parquet('" + file.toAbsolutePath() + "') WHERE " + filter + " ORDER BY id";
        List<Integer> ids = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement()) {
            statement.execute("INSTALL spatial");
            statement.execute("LOAD spatial");
            try (ResultSet rows = statement.executeQuery(sql)) {
                while (rows.next()) {
                    ids.add(rows.getInt("id"));
                }
            }
        }
        return ids;
    }
}
