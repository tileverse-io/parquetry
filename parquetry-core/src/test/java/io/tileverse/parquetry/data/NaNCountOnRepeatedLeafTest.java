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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.ReadFixtures;

/**
 * A read prunes a list of FLOAT elements by the NaN count of its leaf chunk. The chunk counts one value per element and
 * one per empty or null list, and its NaN and null counts add up to them only when no element is a number.
 *
 * <p>The file has two row groups of five rows: lists of NaN elements, then lists holding numbers. Each row group has an
 * empty list, a null list and a null element.
 */
class NaNCountOnRepeatedLeafTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath VALUES = ColumnPath.of("values");
    private static final ColumnPath ELEMENT = ColumnPath.of("values", "list", "element");
    private static final int ROWS_PER_GROUP = 5;
    private static final int ONLY_NAN_ROW_GROUP = 0;
    /** The outcomes leaving the rows of a row group to the scan. */
    private static final Set<RowGroupOutcome> SCANNED = Set.of(RowGroupOutcome.FULL, RowGroupOutcome.PARTIAL);

    @TempDir
    Path tempDir;

    private Path file;

    @BeforeEach
    void writeFile() throws IOException {
        file = tempDir.resolve("lists.parquet");
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .build();
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema(), options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            appendListsOfNaN(appender);
            appendListsHoldingNumbers(appender);
        }
    }

    @Test
    void comparisonWithANumberSkipsARowGroupOfOnlyNaNElements() {
        Predicate anyIsOne = anyElement(new Predicate.Eq(ELEMENT, new Value.FloatVal(1.0f)));

        RowGroupOutcome outcome = explain(anyIsOne).get(ONLY_NAN_ROW_GROUP).outcome();

        assertThat(outcome).isEqualTo(RowGroupOutcome.ELIMINATED);
        assertIdsRead(anyIsOne, 5, 9);
    }

    @Test
    void nanLiteralLeavesARowGroupOfOnlyNaNElementsToTheScan() {
        Predicate anyIsNaN = anyElement(new Predicate.Eq(ELEMENT, new Value.FloatVal(Float.NaN)));

        RowGroupOutcome outcome = explain(anyIsNaN).get(ONLY_NAN_ROW_GROUP).outcome();

        assertThat(outcome).as("rows with an empty or null list match nothing").isIn(SCANNED);
        assertIdsRead(anyIsNaN, 0, 3, 8);
    }

    private static Predicate anyElement(Predicate comparison) {
        return new Predicate.Quantified(MatchAction.ANY, comparison);
    }

    private List<RowGroupPlan> explain(Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ExplainPlan plan = ParquetFileReader.open(source).explain(predicate, Projection.ALL, ReadOptions.DEFAULTS);
            return plan.rowGroups();
        }
    }

    private void assertIdsRead(Predicate predicate, Integer... expected) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(idsRead(reader, predicate, ReadOptions.DEFAULTS))
                    .as("rows matching %s", predicate)
                    .isEqualTo(idsRead(reader, predicate, ReadFixtures.METADATA_PRUNING_OFF))
                    .containsExactly(expected);
        }
    }

    private static List<Integer> idsRead(ParquetFileReader reader, Predicate predicate, ReadOptions options) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, options)) {
            return rows.map(row -> row.getInt(ID)).toList();
        }
    }

    private static void appendListsOfNaN(ParquetRecordBatchBuilder appender) {
        appendList(appender, 0, Float.NaN, Float.NaN);
        appendList(appender, 1);
        appendNullList(appender, 2);
        appendList(appender, 3, Float.NaN, null);
        appendList(appender, 4, (Float) null);
    }

    private static void appendListsHoldingNumbers(ParquetRecordBatchBuilder appender) {
        appendList(appender, 5, 1.0f, 2.0f);
        appendList(appender, 6);
        appendNullList(appender, 7);
        appendList(appender, 8, 3.0f, Float.NaN);
        appendList(appender, 9, 1.0f, null);
    }

    private static void appendList(ParquetRecordBatchBuilder appender, int id, Float... elements) {
        appender.setInt(ID, id);
        appender.beginList(VALUES);
        for (Float element : elements) {
            if (element == null) {
                appender.addNull();
            } else {
                appender.addFloat(element);
            }
        }
        appender.endList();
        appender.endRow();
    }

    private static void appendNullList(ParquetRecordBatchBuilder appender, int id) {
        appender.setInt(ID, id);
        appender.setNull(VALUES);
        appender.endRow();
    }

    /** {@code required int32 id; optional group values (LIST) { repeated group list { optional float element; } }}. */
    private static ParquetSchema schema() {
        SchemaNode.Primitive element = WriteFixtures.optionalLeaf("element", PrimitiveKind.FLOAT);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group values = new SchemaNode.Group(
                "values", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        return WriteFixtures.schemaOf(WriteFixtures.requiredLeaf("id", PrimitiveKind.INT32), values);
    }
}
