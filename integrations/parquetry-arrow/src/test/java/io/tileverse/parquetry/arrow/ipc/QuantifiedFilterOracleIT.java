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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ResolvedColumn;

/**
 * Cross-checks parquetry's quantified (ANY/ALL) filtering over a {@code LIST<STRUCT>} column against DuckDB's own
 * {@code list_filter} semantics on the same file.
 *
 * <p>The parquetry writer is flat-only and cannot emit a nested column. DuckDB therefore writes the fixture: a four-row
 * {@code addresses LIST<STRUCT(...)>} covering the edge cases that distinguish ANY from ALL - a multi-element list, a
 * single-element list, a list whose elements all match, and an empty list. parquetry then resolves the logical path
 * {@code addresses.locality} to the physical leaf DuckDB stored ({@code addresses.list.element.locality}) and evaluates
 * the quantified predicate; DuckDB evaluates the equivalent {@code list_filter} query as the oracle.
 *
 * <p>DuckDB annotates the list group with the deprecated {@code LIST} converted type rather than the modern logical
 * type. Two element shapes are covered: a single-field element struct that the reader could collapse to a bare scalar,
 * and a two-field element struct whose second field must survive assembly while the filtered field is still read. Both
 * paths only resolve once the reader honors the legacy {@code LIST} annotation; without it the element wrapper
 * collapses and the quantified universe reads empty.
 *
 * <p>parquetry treats ALL over an empty list as vacuously true; the DuckDB oracle for ALL is written to agree on that
 * edge (an empty list has no element that fails the predicate).
 */
class QuantifiedFilterOracleIT {

    private static final ColumnPath LOGICAL_LOCALITY = ColumnPath.of("addresses", "locality");
    private static final ColumnPath LOGICAL_SCORES = ColumnPath.of("scores");
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final Value BERLIN = new Value.StringVal("Berlin");
    private static final long SHARED_SCORE = 7L;

    @Test
    void anyAndAllMatchDuckDbWithSingleFieldElement(@TempDir Path tempDir) throws Exception {
        assertOracleAgreementOn(tempDir, "STRUCT(locality VARCHAR)", QuantifiedFilterOracleIT::singleFieldRows);
    }

    @Test
    void anyAndAllMatchDuckDbWithMultiFieldElement(@TempDir Path tempDir) throws Exception {
        assertOracleAgreementOn(
                tempDir, "STRUCT(locality VARCHAR, postcode VARCHAR)", QuantifiedFilterOracleIT::multiFieldRows);
    }

    /**
     * Pins the row-group-elimination bound for an existential {@code ANY} over an integer element list. Every non-empty
     * row holds the same {@code BIGINT} value and there is an empty-list row, which makes the physical chunk a single
     * distinct value with no nulls: a scalar {@code Eq} would prove every value matches and let the STATS tier mark the
     * row group MATCHED, reading it whole with no record-level check. The empty-list row has zero elements and so fails
     * {@code ANY}; DuckDB's {@code list_filter} oracle excludes it, and parquetry must agree.
     */
    @Test
    void anyOverSingleDistinctIntListExcludesEmptyListRow(@TempDir Path tempDir) throws Exception {
        Path scoresFile = tempDir.resolve("scores.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            writeIntListFixture(conn, scoresFile);

            Set<Long> parquetryMatches = matchedScoreIds(scoresFile);
            Set<Long> oracleMatches = queryIds(conn, intListAnyOracleSql(scoresFile));
            assertThat(parquetryMatches)
                    .as("parquetry ANY over an integer list excludes the empty-list row, matching DuckDB")
                    .isEqualTo(oracleMatches)
                    .doesNotContain(4L);
        }
    }

    /**
     * Writes a {@code LIST<STRUCT>} fixture of {@code elementType} from {@code rowsLiteral}, then asserts parquetry's
     * ANY and ALL quantified matches over {@code addresses.locality} equal DuckDB's {@code list_filter} oracle on the
     * same file. A two-field element type exercises the element-struct reconstruction (the second field must survive
     * assembly and the filtered field must still be read), which a single-field element collapses past.
     */
    private void assertOracleAgreementOn(Path tempDir, String elementType, Function<String, String> rowsLiteral)
            throws Exception {
        Path nestedFile = tempDir.resolve("addresses.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            writeNestedFixture(conn, nestedFile, elementType, rowsLiteral.apply(elementType));

            assertQuantifiedMatchesOracle(nestedFile, conn, MatchAction.ANY, anyOracleSql(nestedFile));
            assertQuantifiedMatchesOracle(nestedFile, conn, MatchAction.ALL, allOracleSql(nestedFile));
        }
    }

    private static String singleFieldRows(String elementType) {
        return "(1, [{'locality':'Berlin'}, {'locality':'Bonn'}]), "
                + "(2, [{'locality':'Cologne'}]), "
                + "(3, [{'locality':'Berlin'}, {'locality':'Berlin'}]), "
                + "(4, CAST([] AS " + elementType + "[]))";
    }

    private static String multiFieldRows(String elementType) {
        return "(1, [{'locality':'Berlin','postcode':'10115'}, {'locality':'Bonn','postcode':'53111'}]), "
                + "(2, [{'locality':'Cologne','postcode':'50667'}]), "
                + "(3, [{'locality':'Berlin','postcode':'10115'}, {'locality':'Berlin','postcode':'10117'}]), "
                + "(4, CAST([] AS " + elementType + "[]))";
    }

    /**
     * Writes a {@code scores LIST<BIGINT>} fixture where every non-empty row holds only {@link #SHARED_SCORE} and row 4
     * has an empty list. The physical element chunk is therefore a single distinct value with no nulls.
     */
    private void writeIntListFixture(Connection conn, Path scoresFile) throws SQLException {
        String rows = "(1, [7, 7]), (2, [7]), (3, [7, 7, 7]), (4, CAST([] AS BIGINT[]))";
        String copy = "COPY ("
                + "SELECT CAST(id AS BIGINT) AS id, CAST(scores AS BIGINT[]) AS scores FROM (VALUES "
                + rows
                + ") t(id, scores)"
                + ") TO '" + sqlPath(scoresFile) + "' (FORMAT PARQUET)";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(copy);
        }
    }

    private Set<Long> matchedScoreIds(Path scoresFile) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(scoresFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ColumnPath physicalLeaf = onlyLeafUnder(reader.schema(), LOGICAL_SCORES);
            Predicate quantified = new Predicate.Quantified(
                    MatchAction.ANY, new Predicate.Eq(physicalLeaf, new Value.LongVal(SHARED_SCORE)));
            return collectIds(reader, quantified);
        }
    }

    /**
     * Returns the single physical leaf nested under the {@code group} logical column. A {@code LIST<BIGINT>} has no
     * trailing logical segment for {@link ParquetSchema#resolve} to descend; the element leaf is found directly among
     * the schema's physical leaf columns ({@code scores.list.element}).
     */
    private ColumnPath onlyLeafUnder(ParquetSchema schema, ColumnPath group) {
        List<ColumnPath> leaves = schema.leafColumns().stream()
                .filter(path ->
                        path.numParts() > group.numParts() && path.part(0).equals(group.part(0)))
                .toList();
        assertThat(leaves).as("exactly one physical leaf under %s", group.dot()).hasSize(1);
        return leaves.get(0);
    }

    private String intListAnyOracleSql(Path scoresFile) {
        return "SELECT id FROM read_parquet('" + sqlPath(scoresFile) + "') " + "WHERE len(list_filter(scores, x -> x = "
                + SHARED_SCORE + ")) > 0";
    }

    private void writeNestedFixture(Connection conn, Path nestedFile, String elementType, String rowsLiteral)
            throws SQLException {
        String copy = "COPY ("
                + "SELECT CAST(id AS BIGINT) AS id, CAST(addresses AS " + elementType + "[]) AS addresses FROM (VALUES "
                + rowsLiteral
                + ") t(id, addresses)"
                + ") TO '" + sqlPath(nestedFile) + "' (FORMAT PARQUET)";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(copy);
        }
    }

    private void assertQuantifiedMatchesOracle(Path nestedFile, Connection conn, MatchAction match, String oracleSql)
            throws Exception {
        Set<Long> parquetryMatches = matchedIds(nestedFile, match);
        Set<Long> oracleMatches = queryIds(conn, oracleSql);
        assertThat(parquetryMatches)
                .as("parquetry %s matched ids equal DuckDB %s matched ids", match, match)
                .isEqualTo(oracleMatches);
    }

    private Set<Long> matchedIds(Path nestedFile, MatchAction match) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(nestedFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ColumnPath physicalLeaf = resolveRepeatedLeaf(reader.schema(), LOGICAL_LOCALITY);
            Predicate quantified = new Predicate.Quantified(match, new Predicate.Eq(physicalLeaf, BERLIN));
            return collectIds(reader, quantified);
        }
    }

    private ColumnPath resolveRepeatedLeaf(ParquetSchema schema, ColumnPath logical) {
        ResolvedColumn resolved = schema.resolve(logical).orElseThrow();
        assertThat(resolved.isRepeated())
                .as("%s is a repeated (list element) leaf", logical.dot())
                .isTrue();
        return resolved.physical();
    }

    private Set<Long> collectIds(ParquetFileReader reader, Predicate predicate) {
        Set<Long> ids = new TreeSet<>();
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            rows.forEach(rec -> ids.add(rec.getLong(ID)));
        }
        return ids;
    }

    private Set<Long> queryIds(Connection conn, String sql) throws SQLException {
        Set<Long> ids = new TreeSet<>();
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ids.add(rs.getLong("id"));
            }
        }
        return ids;
    }

    private String anyOracleSql(Path nestedFile) {
        return "SELECT id FROM read_parquet('" + sqlPath(nestedFile) + "') "
                + "WHERE len(list_filter(addresses, x -> x.locality = 'Berlin')) > 0";
    }

    private String allOracleSql(Path nestedFile) {
        return "SELECT id FROM read_parquet('" + sqlPath(nestedFile) + "') "
                + "WHERE len(list_filter(addresses, x -> x.locality = 'Berlin')) = len(addresses)";
    }

    private static String sqlPath(Path file) {
        return file.toAbsolutePath().toString().replace("'", "''");
    }
}
