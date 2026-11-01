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

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.filter.LearnedPages;
import io.tileverse.parquetry.internal.read.LearnedPageIndexes;
import io.tileverse.parquetry.internal.read.PageIndexLearner;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.RowPositionColumn;
import io.tileverse.parquetry.internal.read.RowPositionSynthesis;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Exercises the survivor routing, the row-position bases and the learner of a {@link ReadPlan} on a flat file of
 * several row groups. The {@code value} column of the file grows with the row, and the file has a page index.
 *
 * <p>To check the learner, the tests plan queries over another file without a page index, written once for the class.
 * It holds {@value #ROWS} rows in row groups of {@value #ROWS_PER_GROUP} rows, and row {@code i} holds id {@code i} and
 * a list of one tag.
 */
class ReadPlanTest {

    private static final int ROWS = 4_000;
    private static final int ROWS_PER_GROUP = 1_000;
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath TAGS = ColumnPath.of("tags");
    private static final Projection IDS = Projection.ofPhysical(List.of(ID));

    /** Learned pages holding no entry yet, as the reader gives them to the planner of a file allowed to learn. */
    private static final LearnedPageIndexes NOTHING_LEARNED_YET = new LearnedPageIndexes() {

        @Override
        public Optional<LearnedPages> get(int rowGroup, int column) {
            return Optional.empty();
        }

        @Override
        public void putIfAbsent(int rowGroup, int column, LearnedPages pages) {
            // these checks plan only, and no read stores an entry
        }
    };

    @TempDir
    static Path sharedDir;

    private static Path withoutIndex;

    @TempDir
    Path tempDir;

    private Path file;

    @BeforeAll
    static void writeFileWithoutIndex() throws IOException {
        withoutIndex = writeIdsAndTags(sharedDir);
    }

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

    // Row group 0 is eliminated, row group 1 holds some ids below 1,500, and the later row groups match as a whole.
    @Test
    void learnsOnlyTestedRowGroups() {
        ReadPlan plan = plan(withoutIndex, col(ID).gtEq(1_500L), IDS, ReadOptions.DEFAULTS);

        assertThat(learnableRowGroups(plan)).containsExactly(1);
    }

    @Test
    void constantHasNoLearner() {
        ReadPlan plan = plan(withoutIndex, Predicate.ALWAYS_TRUE, IDS, ReadOptions.DEFAULTS);

        assertThat(plan.learner()).isSameAs(PageIndexLearner.NONE);
    }

    @Test
    void eachRowPassingHasNoLearner() {
        ReadPlan plan = plan(withoutIndex, col(ID).gtEq(0L), IDS, ReadOptions.DEFAULTS);

        assertThat(plan.learner()).isSameAs(PageIndexLearner.NONE);
    }

    // A positional delete keeps the row groups tested, and their statistics show each id passing.
    @Test
    void passingStatisticsHaveNoLearner() {
        Predicate delete =
                new Predicate.RowIndexExcluded(ColumnPath.of("_pos"), SortedLongPositionSet.of(new long[] {1L}));
        ReadPlan plan = plan(withoutIndex, Pred.and(col(ID).gtEq(0L), delete), IDS, ReadOptions.DEFAULTS);

        assertThat(plan.residual()).as("the row groups tested by the read").isNotEmpty();
        assertThat(plan.learner()).isSameAs(PageIndexLearner.NONE);
    }

    @Test
    void toggleOffHasNoLearner() {
        ReadOptions withoutPageIndexes =
                ReadOptions.builder().useColumnIndexFilter(false).build();

        ReadPlan plan = plan(withoutIndex, col(ID).gtEq(1_500L), IDS, withoutPageIndexes);

        assertThat(plan.learner()).isSameAs(PageIndexLearner.NONE);
    }

    // A scan of a repeated leaf never gets a row mask.
    @Test
    void repeatedScanHasNoLearner() {
        ReadPlan plan = plan(withoutIndex, col(ID).gtEq(1_500L), Projection.ALL, ReadOptions.DEFAULTS);

        assertThat(plan.learner()).isSameAs(PageIndexLearner.NONE);
    }

    // In a partly indexed file, a row group with an offset index on a scanned column learns nothing.
    @Test
    void declaredIndexHasNoLearner() {
        ReadPlan plan = plan(file, Pred.col("value").gt(5_000.0), NOTHING_LEARNED_YET);

        assertThat(plan.explainPlan().rowGroups())
                .as("row groups tested by the read")
                .anyMatch(rowGroup ->
                        rowGroup.outcome() == RowGroupOutcome.PARTIAL || rowGroup.outcome() == RowGroupOutcome.FULL);
        assertThat(plan.learner()).isSameAs(PageIndexLearner.NONE);
    }

    /**
     * Plans {@code predicate} over the whole file. The source may close before the assertions run, because routing the
     * row groups and summing their row counts read nothing from the file.
     */
    private static ReadPlan plan(Path file, Predicate predicate) {
        return plan(file, predicate, Projection.ALL, ReadOptions.DEFAULTS);
    }

    private static ReadPlan plan(Path file, Predicate predicate, Projection projection, ReadOptions options) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ReadPlanner planner = planner(source, reader.footer());
            return planner.plan(predicate, projection, QueryObservation.observe(options));
        }
    }

    /** Plans {@code predicate} over the whole file, with {@code learned} in place of what the reader would give. */
    private static ReadPlan plan(Path file, Predicate predicate, LearnedPageIndexes learned) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ReadPlanner planner = planner(source, reader.footer(), learned);
            return planner.plan(predicate, Projection.ALL, QueryObservation.observe(ReadOptions.DEFAULTS));
        }
    }

    /** The row groups allowed to the learner of {@code plan}. */
    private static List<Integer> learnableRowGroups(ReadPlan plan) {
        PageIndexLearner learner = plan.learner();
        List<Integer> learnable = new ArrayList<>();
        for (int rowGroup = 0; rowGroup < plan.chunks().size(); rowGroup++) {
            if (learner.learns(rowGroup)) {
                learnable.add(rowGroup);
            }
        }
        return learnable;
    }

    /** A planner with its own collaborators, built as the reader builds them. */
    private static ReadPlanner planner(ByteRangeSource source, FooterMetadata footer) {
        return planner(source, footer, CachedPageIndexes.of(source, footer, false));
    }

    private static ReadPlanner planner(ByteRangeSource source, FooterMetadata footer, LearnedPageIndexes learned) {
        ReadResources readResources = new ReadResources(source, footer.fileSchema(), ParquetRuntime.defaultRuntime());
        SpatialReadGates spatialReadGates = new SpatialReadGates(
                footer.primaryGeometry(), footer.spatialBounds(), footer.fileSchema(), footer.geoMetadata());
        return new ReadPlanner(source, footer, readResources, spatialReadGates, learned);
    }

    /** The rows of the row groups placed before row group {@code index} in file order. */
    private static long rowsBefore(List<RowGroupChunks> chunks, int index) {
        long rows = 0L;
        for (int j = 0; j < index; j++) {
            rows += chunks.get(j).numRows();
        }
        return rows;
    }

    /** Writes the ids and the tags with Parquet 1 page headers. Such a file declares no page index. */
    private static Path writeIdsAndTags(Path dir) throws IOException {
        WriteOptions options = WriteOptions.builder()
                .tempDir(dir)
                .parquetVersion(ParquetVersion.V1_1)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .pageValueLimit(100)
                .build();
        Path target = dir.resolve("ids-and-tags.parquet");
        try (OutputStream out = Files.newOutputStream(target);
                ParquetFileWriter writer = ParquetFileWriter.create(out, idsAndTags(), options)) {
            // The writer seals a row group between two batches.
            ParquetRecordBatchBuilder appender = writer.appender(ROWS_PER_GROUP);
            for (long id = 0; id < ROWS; id++) {
                appender.setLong(ID, id);
                appender.beginList(TAGS);
                appender.addLong(id % 7);
                appender.endList();
                appender.endRow();
            }
        }
        return target;
    }

    private static ParquetSchema idsAndTags() {
        SchemaNode.Primitive id = WriteFixtures.requiredLeaf("id", PrimitiveKind.INT64);
        SchemaNode.Primitive tag = WriteFixtures.optionalLeaf("element", PrimitiveKind.INT64);
        SchemaNode.Group list = new SchemaNode.Group("list", Repetition.REPEATED, List.of(tag), Optional.empty(), -1);
        SchemaNode.Group tags = new SchemaNode.Group(
                "tags", Repetition.OPTIONAL, List.of(list), Optional.of(new LogicalType.ListType()), -1);
        return WriteFixtures.schemaOf(id, tags);
    }
}
