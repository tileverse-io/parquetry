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
package io.tileverse.parquetry.internal.write.conformance;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * parquet-java skipping pages by the page index reads the lists of a file written with small pages. That reader finds
 * the rows of a page through the page index, correct only for pages starting at the first value of a row.
 *
 * <p>The file has 400 rows, with pages limited to 100 values: a row number, and a list of integers of 250, 10, 5, 300
 * and 3 elements in turn. Element {@code k} of the list of row {@code i} is {@code 1000 * i + k}.
 */
@Tag("conformance")
class RowsWithinPagesConformanceIT {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath NUMBERS = ColumnPath.of("numbers");
    private static final int ROWS = 400;
    private static final int PAGE_VALUE_LIMIT = 100;
    private static final int[] LIST_SIZES = {250, 10, 5, 300, 3};

    @TempDir
    Path tempDir;

    @Test
    void parquetJavaSkippingPagesReadsEachListWhole() throws IOException {
        Path file = writeLists(tempDir.resolve("lists.parquet"));
        FilterPredicate tenRows = and(gtEq(longColumn("id"), 250L), ltEq(longColumn("id"), 259L));

        List<GenericRecord> rows = readWithAvro(file, tenRows);

        assertThat(rows).hasSize(10);
        for (GenericRecord row : rows) {
            int id = Math.toIntExact((Long) row.get("id"));
            assertThat(longElements(row.get("numbers")))
                    .as("list of row %d", id)
                    .isEqualTo(numbersOf(id));
        }
    }

    private static List<GenericRecord> readWithAvro(Path file, FilterPredicate filter) throws IOException {
        List<GenericRecord> rows = new ArrayList<>();
        ParquetReader.Builder<GenericData.Record> builder =
                AvroParquetReader.<GenericData.Record>builder(new LocalInputFile(file));
        builder.withFilter(FilterCompat.get(filter));
        builder.useColumnIndexFilter(true);
        try (ParquetReader<GenericData.Record> reader = builder.build()) {
            GenericData.Record row = reader.read();
            while (row != null) {
                rows.add(row);
                row = reader.read();
            }
        }
        return rows;
    }

    private static List<Long> longElements(Object listValue) {
        List<Long> elements = new ArrayList<>();
        for (Object item : (List<?>) listValue) {
            elements.add((Long) ((GenericRecord) item).get("element"));
        }
        return elements;
    }

    private static List<Long> numbersOf(int row) {
        int size = LIST_SIZES[row % LIST_SIZES.length];
        List<Long> numbers = new ArrayList<>(size);
        for (int k = 0; k < size; k++) {
            numbers.add(1000L * row + k);
        }
        return numbers;
    }

    private Path writeLists(Path file) throws IOException {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        Optional<LogicalType> listType = Optional.of(new LogicalType.ListType());
        SchemaNode.Group numbers =
                new SchemaNode.Group("numbers", Repetition.OPTIONAL, List.of(repeated), listType, -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, numbers), Optional.empty(), -1);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(PAGE_VALUE_LIMIT)
                .build();
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, new ParquetSchema(root), options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (int row = 0; row < ROWS; row++) {
                appender.setLong(ID, row);
                appender.beginList(NUMBERS);
                for (long number : numbersOf(row)) {
                    appender.addLong(number);
                }
                appender.endList();
                appender.endRow();
            }
        }
        return file;
    }
}
