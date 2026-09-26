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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;

/**
 * Asks {@link RowGroupPlans} the questions asked by the prefetcher and the decode coordinator, over the survivors of a
 * multi-row-group file: which mask a fetch and a decode take, whether a row group is dropped, how many rows its masked
 * scan walks, and that the planner is consulted at most once per row group.
 */
class RowGroupPlansTest {

    private static final RowRanges FIRST_FOUR_ROWS = new RowRanges(List.of(new Range(0, 3)));

    @TempDir
    Path tempDir;

    private ByteRangeSource source;
    private ParquetSchema schema;
    private List<RowGroupSurvivor> survivors;
    private List<Optional<RowMask>> pipelineMasks;

    @BeforeEach
    void openFixture() throws IOException {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tempDir, 4_000);
        source = ByteRangeSource.ofFile(file);
        FileMetaData footer = ParquetFormat.readFooter(source);
        schema = SchemaBuilder.build(footer.schema());
        survivors = TestRowGroupChunks.allOf(footer, schema, indexLoader(source)).stream()
                .map(RowGroupSurvivor::full)
                .toList();
        pipelineMasks = pipelineMasks();
        assertThat(survivors).as("the fixture spans several row groups").hasSizeGreaterThanOrEqualTo(3);
    }

    /** One present mask per survivor, each covering every row, standing in for the column-index tier's output. */
    private List<Optional<RowMask>> pipelineMasks() {
        return survivors.stream()
                .map(survivor ->
                        RowMasks.maskFor(survivor.chunks(), RowRanges.all(survivor.numRows()), schema.leafColumns()))
                .toList();
    }

    @AfterEach
    void closeFixture() {
        source.close();
    }

    @Test
    void unplannedPlansReturnThePipelineMasksAndDropNothing() {
        RowGroupPlans plans = RowGroupPlans.unplanned(survivors, pipelineMasks, false);

        assertThat(plans.size()).isEqualTo(survivors.size());
        for (int position = 0; position < survivors.size(); position++) {
            assertThat(plans.dropped(position)).isFalse();
            assertThat(plans.decodeMask(position)).isEqualTo(pipelineMasks.get(position));
            assertThat(plans.fetchMask(position))
                    .as("page-narrowed fetch is off, hence the fetch takes the whole chunks")
                    .isEmpty();
            assertThat(plans.rowsToScan(position))
                    .isEqualTo(survivors.get(position).numRows());
        }
    }

    @Test
    void thePlannerRunsOnceForAPositionWhicheverQuestionComesFirst() {
        AtomicInteger calls = new AtomicInteger();
        RowGroupPlanner planner = _ -> {
            calls.incrementAndGet();
            return RowGroupNarrowing.whole();
        };
        RowGroupPlans plans = RowGroupPlans.planned(survivors, pipelineMasks, true, planner, neverMasks());

        plans.decodeMask(1);
        plans.dropped(1);
        plans.fetchMask(1);
        plans.rowsToScan(1);
        assertThat(calls).hasValue(1);

        plans.dropped(0);
        assertThat(calls).as("each position is planned on its own").hasValue(2);
    }

    @Test
    void aDroppedPlanIsDroppedAndHasNoMask() {
        RowGroupPlans plans =
                RowGroupPlans.planned(survivors, pipelineMasks, true, dropsPositionZero(), narrowingMaskFactory());

        assertThat(plans.dropped(0)).isTrue();
        assertThat(plans.dropped(1)).isFalse();
    }

    @Test
    void aNarrowingPlanBuildsTheNarrowedMaskThroughTheFactory() {
        RowGroupPlans plans =
                RowGroupPlans.planned(survivors, pipelineMasks, true, narrowsToFirstFourRows(), narrowingMaskFactory());

        Optional<RowMask> decodeMask = plans.decodeMask(0);

        assertThat(decodeMask).isPresent();
        assertThat(decodeMask.orElseThrow().survivingRows()).isEqualTo(FIRST_FOUR_ROWS);
        assertThat(plans.fetchMask(0)).isEqualTo(decodeMask);
        assertThat(plans.rowsToScan(0)).isEqualTo(4L);
    }

    @Test
    void aNarrowingPlanWithoutAMaskFallsBackToThePipelineMask() {
        RowGroupPlans plans =
                RowGroupPlans.planned(survivors, pipelineMasks, true, narrowsToFirstFourRows(), neverMasks());

        assertThat(plans.decodeMask(0)).isEqualTo(pipelineMasks.get(0));
        assertThat(plans.dropped(0)).isFalse();
    }

    @Test
    void fetchMaskIsEmptyWhenPageNarrowedFetchIsOff() {
        RowGroupPlans plans = RowGroupPlans.planned(
                survivors, pipelineMasks, false, narrowsToFirstFourRows(), narrowingMaskFactory());

        assertThat(plans.decodeMask(0)).isPresent();
        assertThat(plans.fetchMask(0)).isEmpty();
    }

    @Test
    void theMaskCountIsValidatedAgainstTheSurvivors() {
        List<Optional<RowMask>> tooFew = Collections.nCopies(survivors.size() - 1, Optional.empty());

        assertThatThrownBy(() -> RowGroupPlans.unplanned(survivors, tooFew, true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private RowGroupPlanner narrowsToFirstFourRows() {
        return _ -> RowGroupNarrowing.narrowedTo(FIRST_FOUR_ROWS);
    }

    private RowGroupPlanner dropsPositionZero() {
        return position -> position == 0 ? RowGroupNarrowing.dropped() : RowGroupNarrowing.whole();
    }

    private RowGroupPlans.NarrowedMaskFactory narrowingMaskFactory() {
        return (survivor, rows) -> RowMasks.maskFor(survivor.chunks(), rows, schema.leafColumns());
    }

    /** A factory standing in for a scan whose leaves cannot be masked. */
    private static RowGroupPlans.NarrowedMaskFactory neverMasks() {
        return (_, _) -> Optional.empty();
    }

    private static IndexSectionLoader indexLoader(ByteRangeSource source) {
        return new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                return ParquetFormat.readOffsetIndex(source, offset, length);
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                return ParquetFormat.readColumnIndex(source, offset, length);
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                throw new UnsupportedOperationException("bloom filters are not used in this test");
            }
        };
    }
}
