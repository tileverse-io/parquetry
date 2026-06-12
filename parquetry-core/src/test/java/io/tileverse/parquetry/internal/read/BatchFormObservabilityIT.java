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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.LevelListVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Pins what each {@link BatchForm} produces for a row-aligned LIST group, the wiring the row read path chooses between:
 * {@link BatchForm#ASSEMBLED} yields the eager {@link ListVector} an unfiltered read uses, {@link BatchForm#LEVELS}
 * yields the lazy {@link LevelListVector} a filtered read uses. The two forms are observed at the live batch through
 * the {@link BatchPipeline#rows} batch observer, the same batch the row iterator reads.
 *
 * <p>The decision the row read makes between the two forms is unit-tested separately against the
 * {@code ParquetReader.rowsForm} helper; here we confirm the forms the helper selects actually differ in vector shape.
 */
class BatchFormObservabilityIT {

    private static final String LIST_FIXTURE = "list_columns.parquet";
    private static final ColumnPath LIST_COLUMN = ColumnPath.of("int64_list");

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void assembledFormHoldsAnEagerListVectorAtTheListColumn() {
        List<ColumnVector> listVectors = listColumnVectorsFor(BatchForm.ASSEMBLED);
        assertThat(listVectors)
                .as("an assembled batch wraps the LIST group in the eager Arrow-shape vector")
                .isNotEmpty()
                .allMatch(ListVector.class::isInstance);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void levelsFormHoldsALazyLevelListVectorAtTheListColumn() {
        List<ColumnVector> listVectors = listColumnVectorsFor(BatchForm.LEVELS);
        assertThat(listVectors)
                .as("a levels batch wraps the LIST group in the lazy level vector")
                .isNotEmpty()
                .allMatch(LevelListVector.class::isInstance);
    }

    /**
     * Drives the row pipeline over the fixture with {@code form} and collects the LIST column's vector per live batch.
     */
    private static List<ColumnVector> listColumnVectorsFor(BatchForm form) {
        Path file = CorpusFixtures.parquetTestingData().resolve(LIST_FIXTURE);
        List<ColumnVector> listVectors = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            ParallelDecodeCoordinator coordinator = fixture.serialCoordinator(source, form);
            try (Stream<Integer> rows = BatchPipeline.rows(
                    coordinator,
                    rowCountMaterializer(),
                    fixture.schema(),
                    /*recordFilter*/ null,
                    batch -> listVectors.add(batch.columns().get(LIST_COLUMN)))) {
                rows.forEach(ignored -> {});
            }
        }
        return listVectors;
    }

    /** A no-op materializer; the test reads vector shapes off the live batch, not row values. */
    private static Materializer<Integer> rowCountMaterializer() {
        return (ParquetSchema schema, ParquetRecord row) -> 1;
    }

    private static ExecutorService newFetchExecutor() {
        return Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("test-fetch-", 0).factory());
    }

    /** The fixture's footer plus the machinery to build a serial coordinator over it in a chosen batch form. */
    private record Fixture(ParquetSchema schema, List<RowGroup> rowGroups) {

        static Fixture open(ByteRangeSource source) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            ParquetSchema schema = SchemaBuilder.build(footer.schema());
            return new Fixture(schema, footer.rowGroups());
        }

        ParallelDecodeCoordinator serialCoordinator(ByteRangeSource source, BatchForm form) {
            List<RowGroupSurvivor> survivors = survivors(source);
            RowGroupPrefetcher prefetcher = prefetcher(source, survivors);
            List<Optional<RowMask>> masks = Collections.nCopies(survivors.size(), Optional.empty());
            List<Boolean> recordEvalRequired =
                    survivors.stream().map(RowGroupSurvivor::recordEvalRequired).toList();
            return new ParallelDecodeCoordinator(
                    prefetcher,
                    DecodeExecutor.shared(),
                    DecodeBudget.defaultBudget(),
                    TestDecodeBuffers.ample(),
                    DiskBudget.defaultBudget(),
                    Path.of(System.getProperty("java.io.tmpdir")),
                    true,
                    /*decodeAhead*/ 0,
                    schema,
                    schema,
                    OptionalInt.empty(),
                    masks,
                    recordEvalRequired,
                    Optional.empty(),
                    form);
        }

        private List<RowGroupSurvivor> survivors(ByteRangeSource source) {
            IndexSectionLoader loader = indexLoader(source);
            return rowGroups.stream()
                    .map(rg -> RowGroupSurvivor.full(RowGroupChunks.of(rg, schema, loader)))
                    .toList();
        }

        private RowGroupPrefetcher prefetcher(ByteRangeSource source, List<RowGroupSurvivor> survivors) {
            RowGroupFetcher fetcher = TestFetchers.over(source, schema, schema, SegmentPool.create());
            return new RowGroupPrefetcher(
                    survivors,
                    fetcher,
                    FetchBudget.defaultBudget(),
                    newFetchExecutor(),
                    /*prefetchDepth*/ 2,
                    /*maxConcurrent*/ 2);
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
                    throw new UnsupportedOperationException("bloom filters not used in this test");
                }
            };
        }
    }
}
