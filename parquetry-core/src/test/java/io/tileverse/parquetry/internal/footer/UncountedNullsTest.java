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
package io.tileverse.parquetry.internal.footer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.SchemaBuilder;

/**
 * Covers the leaf columns with a null count not kept: the leaves under a struct of a list, in a file written by DuckDB
 * 1.5. A list nested in a list keeps its null count.
 */
class UncountedNullsTest {

    private static final String DUCKDB_1_5 = "DuckDB version v1.5.6 (build 069cc9f9b5)";
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath CODE = ColumnPath.of("v", "list", "element", "code");
    private static final ColumnPath TAG = ColumnPath.of("tags", "list", "element");
    private static final ColumnPath CELL = ColumnPath.of("grid", "list", "element", "list", "element");

    /** {@code schema { id, v: list<struct<code>>, tags: list<int>, grid: list<list<int>> }}. */
    private static final List<SchemaElement> SCHEMA = List.of(
            group("schema", FieldRepetitionType.REQUIRED, 4),
            leaf("id"),
            group("v", FieldRepetitionType.OPTIONAL, 1),
            group("list", FieldRepetitionType.REPEATED, 1),
            group("element", FieldRepetitionType.OPTIONAL, 1),
            leaf("code"),
            group("tags", FieldRepetitionType.OPTIONAL, 1),
            group("list", FieldRepetitionType.REPEATED, 1),
            leaf("element"),
            group("grid", FieldRepetitionType.OPTIONAL, 1),
            group("list", FieldRepetitionType.REPEATED, 1),
            group("element", FieldRepetitionType.OPTIONAL, 1),
            group("list", FieldRepetitionType.REPEATED, 1),
            leaf("element"));

    @Test
    void marksLeavesOfStructsInLists() {
        LeafIndex leaves = LeafIndex.of(SchemaBuilder.build(SCHEMA));

        BitSet uncounted = UncountedNulls.leavesOf(footer(Optional.of(DUCKDB_1_5)));

        assertThat(uncounted.stream().boxed().toList()).containsExactly(leaves.ordinalOf(CODE));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "DuckDB version v2.0.0 (build d54cbbe3df)",
                "DuckDB version v1.4.0 (build b8a06e4a22)",
                "parquet-cpp-arrow version 24.0.0",
                ""
            })
    void otherWritersKeepNullCounts(String createdBy) {
        assertThat(UncountedNulls.leavesOf(footer(Optional.of(createdBy))).isEmpty())
                .isTrue();
    }

    @Test
    void encodedChunkDropsNullCount() {
        LeafIndex leaves = LeafIndex.of(SchemaBuilder.build(SCHEMA));

        CompactFooter compact = CompactFooter.encode(footer(Optional.of(DUCKDB_1_5)), leaves);

        assertThat(compact.chunk(0, leaves.ordinalOf(CODE)).nullCount()).isEmpty();
        assertThat(compact.chunk(0, leaves.ordinalOf(TAG)).nullCount()).hasValue(1L);
        assertThat(compact.chunk(0, leaves.ordinalOf(ID)).nullCount()).hasValue(1L);
        assertThat(compact.chunk(0, leaves.ordinalOf(CELL)).nullCount()).hasValue(1L);
    }

    /** A one-row-group footer whose chunks hold 2 values each, one of them null. */
    private static FileMetaData footer(Optional<String> createdBy) {
        List<ColumnChunk> chunks = new ArrayList<>();
        chunks.add(chunk(List.of("id")));
        chunks.add(chunk(List.of("v", "list", "element", "code")));
        chunks.add(chunk(List.of("tags", "list", "element")));
        chunks.add(chunk(List.of("grid", "list", "element", "list", "element")));
        RowGroup rowGroup = RowGroup.builder().columns(chunks).numRows(2L).build();
        return FileMetaData.builder()
                .version(2)
                .schema(SCHEMA)
                .numRows(2L)
                .rowGroups(List.of(rowGroup))
                .createdBy(createdBy)
                .build();
    }

    private static ColumnChunk chunk(List<String> pathInSchema) {
        Statistics statistics =
                Statistics.builder().nullCount(OptionalLong.of(1L)).build();
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .codec(CompressionCodec.UNCOMPRESSED)
                .pathInSchema(pathInSchema)
                .numValues(2L)
                .totalCompressedSize(1L)
                .dataPageOffset(4L)
                .statistics(Optional.of(statistics))
                .build();
        return ColumnChunk.builder().metaData(Optional.of(meta)).build();
    }

    private static SchemaElement group(String name, FieldRepetitionType repetition, int children) {
        return new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                Optional.of(repetition),
                name,
                OptionalInt.of(children),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }

    private static SchemaElement leaf(String name) {
        return new SchemaElement(
                Optional.of(PhysicalType.INT32),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.OPTIONAL),
                name,
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }
}
