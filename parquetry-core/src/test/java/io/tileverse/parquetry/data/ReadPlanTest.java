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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.RowPositionColumn;
import io.tileverse.parquetry.internal.read.RowPositionSynthesis;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Exercises the survivor routing and the row-position bases of a {@link ReadPlan} on a flat file of several row groups.
 * The {@code value} column of the file grows with the row.
 */
class ReadPlanTest {

    private static final int ROWS = 4_000;

    @TempDir
    Path tempDir;

    private Path file;

    @BeforeEach
    void writeFixture() throws IOException {
        file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tempDir, ROWS);
    }

    @Test
    void residualLeavesOutMatched() {
        // Every row matches, hence statistics mark each row group MATCHED.
        ReadPlan plan = plan(file, Pred.col("year").gtEq(2020));
        assertThat(plan.residual()).isEmpty();
        assertThat(plan.survivors())
                .hasSize(plan.chunks().size())
                .allSatisfy(
                        survivor -> assertThat(survivor.recordEvalRequired()).isFalse());
    }

    @Test
    void positionBasesCountPrunedRows() {
        // The value grows with the row, hence the leading row groups hold only smaller values and are eliminated.
        ReadPlan plan = plan(file, Pred.col("value").gt(5_000.0));
        List<RowGroupSurvivor> survivors = plan.survivors();
        List<RowPositionSynthesis> syntheses =
                plan.rowPositionSyntheses(survivors, List.of(RowPositionColumn.position(ColumnPath.of("_pos"), 0L)));
        assertThat(plan.explainPlan().rowGroups().getFirst().outcome()).isEqualTo(RowGroupOutcome.ELIMINATED);
        assertThat(syntheses).hasSameSizeAs(survivors);
        assertThat(syntheses.getFirst().base())
                .isEqualTo(rowsBefore(plan.chunks(), survivors.getFirst().index()));
        assertThat(plan.rowPositionSyntheses(survivors, List.of())).isEmpty();
    }

    @Test
    void eliminatedHasNoSurvivor() {
        ReadPlan plan = plan(file, Pred.col("value").gt(5_000.0));
        RowGroupPlan eliminated = plan.explainPlan().rowGroups().getFirst();
        assertThatThrownBy(() -> plan.survivorFor(eliminated)).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Plans {@code predicate} over the whole file. The source may close before the assertions run, because routing the
     * row groups and summing their row counts read nothing from the file.
     */
    private static ReadPlan plan(Path file, Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ReadPlanner planner = planner(source, reader.footer());
            return planner.plan(predicate, Projection.ALL, QueryObservation.observe(ReadOptions.DEFAULTS));
        }
    }

    /** A planner with its own collaborators, built as the reader builds them. */
    private static ReadPlanner planner(ByteRangeSource source, FooterMetadata footer) {
        ReadResources readResources = new ReadResources(source, footer.fileSchema(), ParquetRuntime.defaultRuntime());
        SpatialReadGates spatialReadGates = new SpatialReadGates(
                footer.primaryGeometry(), footer.spatialBounds(), footer.fileSchema(), footer.geoMetadata());
        return new ReadPlanner(source, footer, readResources, spatialReadGates);
    }

    /** The rows of the row groups placed before row group {@code index} in file order. */
    private static long rowsBefore(List<RowGroupChunks> chunks, int index) {
        long rows = 0L;
        for (int j = 0; j < index; j++) {
            rows += chunks.get(j).numRows();
        }
        return rows;
    }
}
