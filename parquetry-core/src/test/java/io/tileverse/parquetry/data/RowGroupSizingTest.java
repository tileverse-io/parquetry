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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class RowGroupSizingTest {

    private static final long MB = 1024L * 1024L;

    @TempDir
    Path tempDir;

    @Test
    void adaptiveBelowSmallThresholdHasNoByteBound() {
        WriteOptions options =
                WriteOptions.builder().tempDir(tempDir).expectedRowCount(40_000).build();
        assertThat(RowGroupSizingPolicy.targetBytes(options)).isEqualTo(Long.MAX_VALUE);
        assertThat(RowGroupSizingPolicy.targetRows(options)).isEmpty();
    }

    @Test
    void adaptiveMediumPicks128Mb() {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .expectedRowCount(500_000)
                .build();
        assertThat(RowGroupSizingPolicy.targetBytes(options)).isEqualTo(128L * MB);
    }

    @Test
    void adaptiveLargePicks512Mb() {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .expectedRowCount(5_000_000)
                .build();
        assertThat(RowGroupSizingPolicy.targetBytes(options)).isEqualTo(512L * MB);
    }

    @Test
    void adaptiveVeryLargePicks1Gb() {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .expectedRowCount(20_000_000)
                .build();
        assertThat(RowGroupSizingPolicy.targetBytes(options)).isEqualTo(1024L * MB);
    }

    @Test
    void adaptiveWithoutHintFallsBackTo128Mb() {
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        assertThat(RowGroupSizingPolicy.targetBytes(options)).isEqualTo(128L * MB);
    }

    @Test
    void explicitBytesPinsThreshold() {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.bytes(1024L))
                .build();
        assertThat(RowGroupSizingPolicy.targetBytes(options)).isEqualTo(1024L);
        assertThat(RowGroupSizingPolicy.targetRows(options)).isEmpty();
    }

    @Test
    void explicitRowsPinsThresholdAndDisablesByteBound() {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(100L))
                .build();
        assertThat(RowGroupSizingPolicy.targetRows(options)).hasValue(100L);
        assertThat(RowGroupSizingPolicy.targetBytes(options)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void writerRoutesThroughTheRowsPolicy() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(4L))
                .build();
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        try (ParquetWriter writer = ParquetWriter.create(sink, schema, options)) {
            for (int i = 0; i < 9; i++) {
                writer.write(row(Map.of(ColumnPath.of("id"), i)));
            }
            assertThat(writer.rowGroupsWritten()).isEqualTo(2L);
        }
    }

    // --- helpers ---

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static WriteRow row(Map<ColumnPath, Object> values) {
        Map<ColumnPath, Object> copy = new HashMap<>(values);
        return copy::get;
    }
}
