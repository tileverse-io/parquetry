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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.filter.RecordAccessors;
import io.tileverse.parquetry.internal.filter.RecordLevelEvaluator;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Regression for a filtered read over a repeated column whose rows outrun their data pages. The fixture writes five
 * elements per row at three values per page, which leaves most rows starting in one page and ending in the next, and it
 * is read without the page index so that the scan walks the whole row group rather than a row-aligned selection. A row
 * the predicate keeps must reach the caller with its element list whole, at one row per batch and unbounded alike.
 */
class MaskedNestedPageSpanRegressionTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath VALUES = ColumnPath.of("values");
    private static final int ROWS = 20;
    private static final int ELEMENTS_PER_ROW = 5;
    private static final Predicate SELECTIVE =
            Pred.and(Pred.col("id").gtEq(12L), Pred.col("id").lt(16L));

    @TempDir
    Path tempDir;

    @Test
    void aSpanningRowSurvivesTheFilterWhole() throws Exception {
        assertFilteredReadMatchesReference(unbounded());
    }

    @Test
    void aSpanningRowSurvivesTheFilterWholeOneRowAtATime() throws Exception {
        assertFilteredReadMatchesReference(oneRowPerBatch());
    }

    private void assertFilteredReadMatchesReference(ReadOptions options) throws Exception {
        Path file = writeSpanningRowFixture();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            List<Row> filtered = filteredRows(reader, options);
            List<Row> reference = referenceRows(reader);

            assertThat(reference).as("the fixture selects a handful of rows").hasSize(4);
            assertThat(filtered)
                    .as("the filtered read returns the reference rows, element lists whole")
                    .isEqualTo(reference);
        }
    }

    /** The page index is off, which leaves the scan no row mask and hence the page-spanning row to follow by hand. */
    private static ReadOptions unbounded() {
        return ReadOptions.builder().useColumnIndexFilter(false).build();
    }

    private static ReadOptions oneRowPerBatch() {
        return ReadOptions.builder().useColumnIndexFilter(false).batchSize(1).build();
    }

    private static List<Row> filteredRows(ParquetFileReader reader, ReadOptions options) {
        try (Stream<ParquetRecord> rows = reader.read(SELECTIVE, Projection.ALL, options)) {
            return rows.map(MaskedNestedPageSpanRegressionTest::rowOf).toList();
        }
    }

    /** The reference: decode every column unfiltered, then test each materialized row in Java. */
    private static List<Row> referenceRows(ParquetFileReader reader) {
        List<Row> rows = new ArrayList<>();
        try (Stream<ParquetRecordBatch> batches =
                reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> collectMatchingRows(batch, rows));
        }
        return rows;
    }

    private static void collectMatchingRows(ParquetRecordBatch batch, List<Row> into) {
        try (batch) {
            for (int row = 0; row < batch.rowCount(); row++) {
                ParquetRecord materialized = batch.materialize(row);
                if (RecordLevelEvaluator.test(SELECTIVE, RecordAccessors.of(materialized))) {
                    into.add(rowOf(materialized));
                }
            }
        }
    }

    private static Row rowOf(ParquetRecord materialized) {
        List<?> elements = (List<?>) materialized.get(VALUES);
        return new Row(materialized.getLong(ID), List.copyOf(elements));
    }

    /**
     * Twenty rows of five elements each, three values per page: every column cuts a page mid-row, which leaves rows
     * whose elements start in one page and end in the next.
     */
    private Path writeSpanningRowFixture() throws Exception {
        ParquetSchema schema = spanningRowSchema();
        WriteOptions options =
                WriteOptions.builder().tempDir(tempDir).pageValueLimit(3).build();
        Path file = tempDir.resolve("spanning-rows.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(ROWS);
            for (int row = 0; row < ROWS; row++) {
                appendRow(appender, row);
            }
            appender.flush();
        }
        return file;
    }

    private static void appendRow(ParquetRecordBatchBuilder appender, int row) {
        appender.setLong(0, row);
        appender.beginList(VALUES);
        for (int element = 0; element < ELEMENTS_PER_ROW; element++) {
            appender.addLong((long) row * 100 + element);
        }
        appender.endList();
        appender.endRow();
    }

    private static ParquetSchema spanningRowSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group values = new SchemaNode.Group(
                "values", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, values), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private record Row(long id, List<?> elements) {}
}
