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

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.PhaseTimings;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.observe.RowGroupRead;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies the opt-in per-phase CPU timing: an observer whose {@code wantsTimings()} returns {@code true} receives
 * {@link PhaseTimings} on each {@link RowGroupRead} and an aggregated {@code cpuTimings} on the final
 * {@link QueryStats}, while the default (timings off) leaves both empty.
 *
 * <p>The fixture writes ids {@code 0..8} four rows per row group, hence three row groups holding {@code [0,1,2,3]},
 * {@code [4,5,6,7]}, and {@code [8]}. The predicate {@code id >= 5} eliminates the first row group from statistics and
 * runs the record-level filter over the surviving two.
 */
class QueryTimingsIT {

    private static final Predicate PREDICATE = col("id").gtEq(5);

    @Test
    void timingObserverReceivesPerPhaseTimings(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader reader = ParquetReader.open(source);
            TimingObserver observer = new TimingObserver();
            ReadOptions options = ReadOptions.builder().queryObserver(observer).build();

            try (Stream<?> rows = reader.read(PREDICATE, Projection.ALL, options)) {
                rows.forEach(row -> {});
            }

            assertThat(observer.events)
                    .as("two non-eliminated row groups decode")
                    .hasSize(2);
            assertThat(observer.events)
                    .as("every decoded row group reports its phase timings")
                    .allSatisfy(event -> assertThat(event.timings()).isPresent());
            assertThat(observer.events)
                    .as("each group's fetch and decode were measured")
                    .allSatisfy(event -> {
                        PhaseTimings timings = event.timings().orElseThrow();
                        assertThat(timings.fetchNanos()).isPositive();
                        assertThat(timings.decodeNanos()).isPositive();
                    });

            QueryStats stats = observer.lastStats;
            assertThat(stats.cpuTimings())
                    .as("the final stats aggregate the per-group timings")
                    .isPresent();
            PhaseTimings cpu = stats.cpuTimings().orElseThrow();
            assertThat(cpu.pipelineNanos())
                    .as("the filter pipeline run was measured at query level")
                    .isPositive();
            assertThat(cpu.decodeNanos()).isPositive();
            assertThat(cpu.fetchNanos()).isPositive();
            assertThat(cpu.recordFilterNanos())
                    .as("record-level processing over the surviving rows was measured")
                    .isPositive();
        }
    }

    @Test
    void countObserverReceivesQueryLevelTimings(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader reader = ParquetReader.open(source);
            TimingObserver observer = new TimingObserver();
            ReadOptions options = ReadOptions.builder().queryObserver(observer).build();

            reader.count(PREDICATE, options);

            QueryStats stats = observer.lastStats;
            assertThat(stats.cpuTimings()).isPresent();
            PhaseTimings cpu = stats.cpuTimings().orElseThrow();
            assertThat(cpu.pipelineNanos()).isPositive();
            assertThat(cpu.decodeNanos())
                    .as("the residual row group decodes its predicate column")
                    .isPositive();
            assertThat(cpu.recordFilterNanos())
                    .as("count evaluates batches columnar-style, never through the record filter")
                    .isZero();
        }
    }

    @Test
    void defaultObserverReceivesNoTimings(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader reader = ParquetReader.open(source);
            TimingObserver observer = new TimingObserver(false);
            ReadOptions options = ReadOptions.builder().queryObserver(observer).build();

            try (Stream<?> rows = reader.read(PREDICATE, Projection.ALL, options)) {
                rows.forEach(row -> {});
            }

            assertThat(observer.events)
                    .as("timings stay empty when the observer does not opt in")
                    .allSatisfy(event -> assertThat(event.timings()).isEmpty());
            assertThat(observer.lastStats.cpuTimings()).isEmpty();
        }
    }

    private static Path writeThreeRowGroups(Path tmp) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tmp)
                .rowGroupSize(RowGroupSize.rows(4L))
                .build();
        Path file = tmp.resolve("query-timings.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetWriter writer = ParquetWriter.create(out, schema, options)) {
            for (int i = 0; i < 9; i++) {
                writer.write(row(Map.of(ColumnPath.of("id"), i)));
            }
        }
        return file;
    }

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

    private static final class TimingObserver implements QueryObserver {

        private final boolean wantsTimings;
        private final List<RowGroupRead> events = new ArrayList<>();
        private QueryStats lastStats;

        TimingObserver() {
            this(true);
        }

        TimingObserver(boolean wantsTimings) {
            this.wantsTimings = wantsTimings;
        }

        @Override
        public boolean wantsTimings() {
            return wantsTimings;
        }

        @Override
        public synchronized void onRowGroupRead(RowGroupRead event) {
            events.add(event);
        }

        @Override
        public synchronized void onQueryFinished(QueryStats stats) {
            this.lastStats = stats;
        }
    }
}
