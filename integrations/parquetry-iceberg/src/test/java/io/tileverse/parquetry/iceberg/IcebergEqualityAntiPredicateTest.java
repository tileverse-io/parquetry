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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.ConstantFolding;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Pins the null-safe semantics of the Iceberg equality-delete anti-predicate. The equality fields are {@code (category,
 * flag)} and the delete tuples are {@code (category="a", flag="x")} and {@code (category=NULL, flag="y")}, mirroring
 * the vendored {@code iceberg-deletes/equality} fixture. Evaluating the constructed predicate against representative
 * rows - including rows with null cells - must keep exactly the rows Iceberg's IS NOT DISTINCT FROM matching keeps: the
 * two matching rows are deleted, and the null-edge rows survive.
 */
class IcebergEqualityAntiPredicateTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath CATEGORY = ColumnPath.of("category");
    private static final ColumnPath FLAG = ColumnPath.of("flag");

    @TempDir
    Path tempDir;

    @Test
    void deletesMatchingRowsAndKeepsNullEdgeSurvivors() throws Exception {
        Predicate antiPredicate = fixtureAntiPredicate();

        List<Long> kept = readKeptIds(antiPredicate);

        assertThat(kept).containsExactly(3L, 4L, 5L);
    }

    @Test
    void buildsNoPredicateWhenThereAreNoTuples() {
        Optional<Predicate> none = IcebergEqualityAntiPredicate.build(List.of(CATEGORY, FLAG), List.of());

        assertThat(none).isEmpty();
    }

    @Test
    void foldsAnAntiPredicateOverAnEqualityFieldThatReconcilesToAConstant() {
        Predicate antiPredicate = fixtureAntiPredicate();
        Map<ColumnPath, Value> constants = Map.of(CATEGORY, new Value.StringVal("b"));

        Predicate folded = ConstantFolding.fold(antiPredicate, constants, Set.of());

        assertThat(folded).isNotNull().isNotEqualTo(Predicate.ALWAYS_FALSE);
    }

    @Test
    void foldsAnAntiPredicateOverAnEqualityFieldThatReconcilesToANull() {
        Predicate antiPredicate = fixtureAntiPredicate();

        Predicate folded = ConstantFolding.fold(antiPredicate, Map.of(), Set.of(CATEGORY));

        assertThat(folded).isNotNull();
    }

    private static Predicate fixtureAntiPredicate() {
        List<List<Value>> tuples = new ArrayList<>();
        tuples.add(Arrays.asList(new Value.StringVal("a"), new Value.StringVal("x")));
        tuples.add(Arrays.asList(null, new Value.StringVal("y")));
        return IcebergEqualityAntiPredicate.build(List.of(CATEGORY, FLAG), tuples)
                .orElseThrow();
    }

    /**
     * Five rows that exercise each null-safe edge:
     *
     * <ul>
     *   <li>id=1 (a,x): matches the non-null tuple - DELETED
     *   <li>id=2 (NULL,y): matches the null-component tuple - DELETED
     *   <li>id=3 (NULL,z): category null but flag differs - SURVIVES
     *   <li>id=4 (b,y): flag matches the null tuple but category is non-null - SURVIVES
     *   <li>id=5 (a,NULL): category matches the non-null tuple but flag is null - SURVIVES
     * </ul>
     */
    private List<Long> readKeptIds(Predicate antiPredicate) throws Exception {
        Path file = tempDir.resolve("equality-rows.parquet");
        writeRepresentativeRows(file);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(antiPredicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                return rows.map(row -> row.getLong(ID)).sorted().toList();
            }
        }
    }

    private void writeRepresentativeRows(Path file) throws Exception {
        ParquetSchema schema = rowsSchema();
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options);
                ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema)) {
            appendRow(builder, 1L, "a", "x");
            appendRow(builder, 2L, null, "y");
            appendRow(builder, 3L, null, "z");
            appendRow(builder, 4L, "b", "y");
            appendRow(builder, 5L, "a", null);
            try (ParquetRecordBatch batch = builder.build()) {
                writer.writeBatch(batch);
            }
        }
    }

    private static void appendRow(ParquetRecordBatchBuilder builder, long id, String category, String flag) {
        builder.setLong(ID, id);
        setStringOrNull(builder, CATEGORY, category);
        setStringOrNull(builder, FLAG, flag);
        builder.endRow();
    }

    private static void setStringOrNull(ParquetRecordBatchBuilder builder, ColumnPath column, String value) {
        if (value == null) {
            builder.setNull(column);
        } else {
            builder.setString(column, value);
        }
    }

    private static ParquetSchema rowsSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive category = string("category");
        SchemaNode.Primitive flag = string("flag");
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, category, flag), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive string(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
    }
}
