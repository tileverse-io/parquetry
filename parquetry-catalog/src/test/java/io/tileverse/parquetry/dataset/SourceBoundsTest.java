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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.spatial.BoundsAccumulator;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testsupport.PointParquet;

/**
 * Exercises {@link DefaultParquetSource#bounds} over a multi-file GeoParquet dataset. Every fixture file records exact
 * native geometry statistics, which makes a brute-force scan of the same predicate an exact oracle for the fan-out's
 * answer. The dataset fans its files out across virtual threads and folds their per-file bounds through one shared
 * accumulator; the assertions pin that the folded answer is exact and stable regardless of fan-out completion order.
 */
class SourceBoundsTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    @TempDir
    Path tempDir;

    @Test
    void unfilteredBoundsEqualTheScanOracle() throws Exception {
        withMultiFileSource(source -> {
            Optional<BoundingBox> bounds = source.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
            Optional<BoundingBox> oracle = scanBounds(source, Predicate.ALWAYS_TRUE);

            assertThat(oracle).as("the dataset holds geometry rows to bound").isPresent();
            assertThat(bounds).isPresent();
            assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
        });
    }

    @Test
    void spatialWindowBoundsEqualTheScanOracle() throws Exception {
        withMultiFileSource(source -> {
            Predicate window = Pred.col("geometry").bboxIntersects(Bbox.of2d(0.0, 0.0, 15.0, 15.0));

            Optional<BoundingBox> bounds = source.bounds(window, ReadOptions.DEFAULTS);
            Optional<BoundingBox> oracle = scanBounds(source, window);
            Optional<BoundingBox> unfiltered = source.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);

            assertThat(oracle)
                    .as("the window must clip a non-empty subset for a meaningful test")
                    .isPresent();
            assertThat(bounds).isPresent();
            assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
            assertThat(bounds.orElseThrow().xmax())
                    .as("a fan-out ignoring the spatial predicate would return the full dataset extent")
                    .isLessThan(unfiltered.orElseThrow().xmax());
        });
    }

    @Test
    void repeatedCallsReturnIdenticalBounds() throws Exception {
        withMultiFileSource(source -> {
            Optional<BoundingBox> first = source.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
            assertThat(first).isPresent();

            for (int call = 0; call < 10; call++) {
                Optional<BoundingBox> repeat = source.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
                assertThat(repeat).as("call %d", call).isPresent();
                assertSameBox2d(repeat.orElseThrow(), first.orElseThrow());
            }
        });
    }

    @Test
    void readerWithoutOverrideReturnsEmpty() {
        ParquetReader withoutBounds = readerThrowingOnEveryRead();

        assertThat(withoutBounds.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                .as("the shared read contract defaults bounds to empty")
                .isEmpty();
    }

    // --- source construction ---

    private void withMultiFileSource(Consumer<DefaultParquetSource> body) throws Exception {
        List<ByteRangeSource> sources = writeFixtureFiles();
        try {
            body.accept(sourceOver(sources));
        } finally {
            closeAll(sources);
        }
    }

    /**
     * Four single-row-group point files with distinct extents. File 3 spans them all and files 1 and 2 nest inside file
     * 0's box, which exercises the containment skip whenever a wider file folds before a narrower one. The union over
     * every row is {@code [-5, -5, 30, 25]}.
     */
    private List<ByteRangeSource> writeFixtureFiles() throws Exception {
        double[][][] pointsPerFile = {
            {{0.0, 0.0}, {10.0, 10.0}},
            {{2.0, 2.0}, {8.0, 8.0}},
            {{10.0, 5.0}, {20.0, 10.0}},
            {{-5.0, -5.0}, {30.0, 25.0}},
        };
        List<ByteRangeSource> sources = new ArrayList<>(pointsPerFile.length);
        for (int file = 0; file < pointsPerFile.length; file++) {
            Path path = tempDir.resolve("points-" + file + ".parquet");
            PointParquet.writePoints(path, "geometry", GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0, pointsPerFile[file]);
            sources.add(ByteRangeSource.ofFile(path));
        }
        return sources;
    }

    private static DefaultParquetSource sourceOver(List<ByteRangeSource> sources) {
        ParquetRuntime runtime = ParquetRuntime.defaultRuntime();
        List<ParquetFileReader> readers = new ArrayList<>(sources.size());
        for (ByteRangeSource source : sources) {
            readers.add(ParquetFileReader.open(source, runtime, Optional.empty()));
        }
        return new DefaultParquetSource(readers, runtime.maxConcurrentFiles());
    }

    private static void closeAll(List<ByteRangeSource> sources) {
        for (ByteRangeSource source : sources) {
            source.close();
        }
    }

    // --- oracle ---

    /** The exact bounding box of every {@code predicate}-matching row across the dataset, scanned from the WKB. */
    private static Optional<BoundingBox> scanBounds(DefaultParquetSource source, Predicate predicate) {
        BoundsAccumulator oracle = new BoundsAccumulator();
        try (Stream<ParquetRecordBatch> batches =
                source.readBatches(predicate, Projection.ofPhysical(List.of(GEOMETRY)), ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {
                foldBatchEnvelopes(batch, oracle);
                batch.close();
            });
        }
        return oracle.snapshot();
    }

    private static void foldBatchEnvelopes(ParquetRecordBatch batch, BoundsAccumulator oracle) {
        BinaryVector cells = (BinaryVector) batch.columns().get(GEOMETRY);
        for (int row = 0; row < batch.rowCount(); row++) {
            MemorySegment wkb = cells.get(row);
            if (wkb != null) {
                Bbox envelope = WkbEnvelope.compute(wkb);
                oracle.unionXy(envelope.minX(), envelope.minY(), envelope.maxX(), envelope.maxY());
            }
        }
    }

    private static void assertSameBox2d(BoundingBox actual, BoundingBox expected) {
        assertThat(actual.xmin()).as("xmin").isEqualTo(expected.xmin());
        assertThat(actual.ymin()).as("ymin").isEqualTo(expected.ymin());
        assertThat(actual.xmax()).as("xmax").isEqualTo(expected.xmax());
        assertThat(actual.ymax()).as("ymax").isEqualTo(expected.ymax());
    }

    // --- a reader that implements only the abstract read contract ---

    /**
     * A {@link ParquetReader} whose every abstract read method throws and which adds no {@code bounds} override,
     * isolating the interface default for the empty-by-default assertion.
     */
    private static ParquetReader readerThrowingOnEveryRead() {
        return new ParquetReader() {
            @Override
            public ParquetSchema schema() {
                throw new UnsupportedOperationException();
            }

            @Override
            @MustBeClosed
            public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
                throw new UnsupportedOperationException();
            }

            @Override
            @MustBeClosed
            public <T> Stream<T> read(
                    Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
                throw new UnsupportedOperationException();
            }

            @Override
            @MustBeClosed
            public Stream<ParquetRecordBatch> readBatches(
                    Predicate predicate, Projection projection, ReadOptions options) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long count(Predicate predicate, ReadOptions options) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
