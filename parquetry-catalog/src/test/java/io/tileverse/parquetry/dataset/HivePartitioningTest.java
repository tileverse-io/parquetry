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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.prune.ColumnStatistics;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class HivePartitioningTest {

    private static ParquetSchema schemaWithYearAndCity() {
        SchemaNode.Primitive year = new SchemaNode.Primitive(
                "year", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 1);
        SchemaNode.Primitive city = new SchemaNode.Primitive(
                "city", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), 2);
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(year, city), Optional.empty(), 0);
        return new ParquetSchema(root);
    }

    @Test
    void bindsPhysicalKeysAndBuildsExactStats() {
        HivePartitioning binding = HivePartitioning.bind(List.of("year", "city"), schemaWithYearAndCity());
        FileStats stats = binding.fileStats(Map.of("year", "2024", "city", "rome"), 100L);
        assertThat(stats.columns().get(ColumnPath.of("year")))
                .isEqualTo(new ColumnStatistics(
                        Optional.of(new Value.IntVal(2024)), Optional.of(new Value.IntVal(2024)), OptionalLong.of(0)));
        assertThat(stats.columns().get(ColumnPath.of("city")))
                .isEqualTo(new ColumnStatistics(
                        Optional.of(new Value.StringVal("rome")),
                        Optional.of(new Value.StringVal("rome")),
                        OptionalLong.of(0)));
        assertThat(stats.recordCount()).isEqualTo(100L);
    }

    @Test
    void pathOnlyKeyThrows() {
        List<String> partitionKeys = List.of("year", "region");
        ParquetSchema schema = schemaWithYearAndCity();
        assertThatThrownBy(() -> HivePartitioning.bind(partitionKeys, schema))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("region")
                .hasMessageContaining("path-only");
    }

    @Test
    void unboundPartitionValueIsSkipped() {
        HivePartitioning binding = HivePartitioning.bind(List.of("year"), schemaWithYearAndCity());
        FileStats stats = binding.fileStats(Map.of("year", "2024", "extra", "x"), 10L);
        assertThat(stats.columns()).containsKey(ColumnPath.of("year"));
        assertThat(stats.columns()).doesNotContainKey(ColumnPath.of("extra"));
    }

    @Test
    void malformedNumericPartitionValueIsSkipped() {
        HivePartitioning binding = HivePartitioning.bind(List.of("year"), schemaWithYearAndCity());
        Map<String, String> malformed = Map.of("year", "notanumber");
        assertThatCode(() -> binding.fileStats(malformed, 1L)).doesNotThrowAnyException();
        FileStats stats = binding.fileStats(malformed, 1L);
        assertThat(stats.columns()).doesNotContainKey(ColumnPath.of("year"));
    }

    @Test
    void dateLogicalTypeOnInt32ParsesIsoDate() {
        HivePartitioning binding = HivePartitioning.bind(List.of("dt"), schemaWithDateColumn());
        FileStats stats = binding.fileStats(Map.of("dt", "2024-01-15"), 7L);
        Value.DateVal expected = new Value.DateVal(LocalDate.parse("2024-01-15"));
        assertThat(stats.columns().get(ColumnPath.of("dt")))
                .isEqualTo(new ColumnStatistics(Optional.of(expected), Optional.of(expected), OptionalLong.of(0)));
    }

    @Test
    void booleanEncodingsParse() {
        HivePartitioning binding = HivePartitioning.bind(List.of("flag"), schemaWithBooleanColumn());
        assertThat(boolStat(binding, "1")).isEqualTo(new Value.BoolVal(true));
        assertThat(boolStat(binding, "0")).isEqualTo(new Value.BoolVal(false));
        assertThat(boolStat(binding, "true")).isEqualTo(new Value.BoolVal(true));
        assertThat(boolStat(binding, "TRUE")).isEqualTo(new Value.BoolVal(true));
    }

    @Test
    void unrecognizedBooleanValueIsSkipped() {
        HivePartitioning binding = HivePartitioning.bind(List.of("flag"), schemaWithBooleanColumn());
        FileStats stats = binding.fileStats(Map.of("flag", "yes"), 1L);
        assertThat(stats.columns()).doesNotContainKey(ColumnPath.of("flag"));
    }

    @Test
    void hiveNullSentinelIsSkipped() {
        HivePartitioning binding = HivePartitioning.bind(List.of("year"), schemaWithYearAndCity());
        FileStats stats = binding.fileStats(Map.of("year", "__HIVE_DEFAULT_PARTITION__"), 1L);
        assertThat(stats.columns()).doesNotContainKey(ColumnPath.of("year"));
    }

    @Test
    void int96ColumnIsSkipped() {
        HivePartitioning binding = HivePartitioning.bind(List.of("ts"), schemaWithInt96Column());
        FileStats stats = binding.fileStats(Map.of("ts", "12345"), 1L);
        assertThat(stats.columns()).doesNotContainKey(ColumnPath.of("ts"));
    }

    private static Value boolStat(HivePartitioning binding, String raw) {
        FileStats stats = binding.fileStats(Map.of("flag", raw), 1L);
        return stats.columns().get(ColumnPath.of("flag")).min().orElseThrow();
    }

    private static ParquetSchema schemaWithDateColumn() {
        SchemaNode.Primitive dt = new SchemaNode.Primitive(
                "dt",
                Repetition.OPTIONAL,
                PrimitiveKind.INT32,
                OptionalInt.empty(),
                Optional.of(new LogicalType.DateType()),
                1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(dt), Optional.empty(), 0);
        return new ParquetSchema(root);
    }

    private static ParquetSchema schemaWithBooleanColumn() {
        SchemaNode.Primitive flag = new SchemaNode.Primitive(
                "flag", Repetition.OPTIONAL, PrimitiveKind.BOOLEAN, OptionalInt.empty(), Optional.empty(), 1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(flag), Optional.empty(), 0);
        return new ParquetSchema(root);
    }

    private static ParquetSchema schemaWithInt96Column() {
        SchemaNode.Primitive ts = new SchemaNode.Primitive(
                "ts", Repetition.OPTIONAL, PrimitiveKind.INT96, OptionalInt.empty(), Optional.empty(), 1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(ts), Optional.empty(), 0);
        return new ParquetSchema(root);
    }
}
