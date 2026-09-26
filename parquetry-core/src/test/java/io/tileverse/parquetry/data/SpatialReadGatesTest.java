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

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.read.IndexSectionLoader;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowGroupGate;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Drives the row-group gate of a decimated read over a two-point GeoParquet file through a scripted probe. The gate
 * offers the row group's bounding box to the read-only region consultation: it drops the group reported as already
 * covered, reads the group left undecided by the probe, and refuses a substitute, which would stand for rows never
 * proven to satisfy the query.
 */
class SpatialReadGatesTest {

    private static final String GEOMETRY = "geometry";

    /** The gate decides from a row group's bounding box alone, hence it reads no index section at all. */
    private static final IndexSectionLoader NO_INDEX_SECTIONS = new IndexSectionLoader() {

        @Override
        public OffsetIndex readOffsetIndex(long offset, int length) {
            throw new UnsupportedOperationException("the row-group gate reads no offset index");
        }

        @Override
        public ColumnIndex readColumnIndex(long offset, int length) {
            throw new UnsupportedOperationException("the row-group gate reads no column index");
        }

        @Override
        public SplitBlockBloomFilter readBloom(long offset, int length) {
            throw new UnsupportedOperationException("the row-group gate reads no bloom filter");
        }
    };

    @TempDir
    Path tempDir;

    private ByteRangeSource source;

    @AfterEach
    void closeSource() {
        if (source != null) {
            source.close();
        }
    }

    @Test
    void aCoveredRowGroupIsSkipped() throws Exception {
        RowGroupGate gate = gateOver(regionProbe(Decision.skip()));

        assertThat(gate.skip(0)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("undecidedAnswers")
    void aRowGroupNotReportedCoveredIsRead(Decision answer) throws Exception {
        RowGroupGate gate = gateOver(regionProbe(answer));

        assertThat(gate.skip(0)).isFalse();
    }

    @Test
    void aSubstituteAnsweredToTheReadOnlyConsultationIsRejected() throws Exception {
        RowGroupGate gate = gateOver(regionProbe(Decision.substitute()));

        assertThatThrownBy(() -> gate.skip(0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Substitute");
    }

    /** The two answers that leave the row group to be read: the probe cannot decide it, or wants it whole. */
    private static Stream<Decision> undecidedAnswers() {
        return Stream.of(Decision.descend(), Decision.keep());
    }

    /** The row-group gate of a read over a freshly written single-row-group file, consulting {@code probe}. */
    private RowGroupGate gateOver(SpatialReadProbe probe) throws Exception {
        Path file = FileStatsFixtures.writePoints(tempDir, GEOMETRY, new double[][] {{1, 1}, {2, 2}});
        source = ByteRangeSource.ofFile(file);
        ParquetFileReader reader = ParquetFileReader.open(source);
        SpatialReadGates gates = new SpatialReadGates(reader.spatialBounds(), reader.schema(), geoMetadataOf(reader));
        ReadOptions options = ReadOptions.builder().spatialReadProbe(probe).build();
        return gates.rowGroupGate(survivorsOf(reader), options).orElseThrow();
    }

    /** The file's one row group, as the filter pipeline hands it over once it has cleared it in full. */
    private static List<RowGroupSurvivor> survivorsOf(ParquetFileReader reader) {
        RowGroupChunks chunks =
                RowGroupChunks.of(reader.compactFooter(), 0, reader.leafIndex(), reader.schema(), NO_INDEX_SECTIONS);
        return List.of(RowGroupSurvivor.full(chunks));
    }

    private static Optional<GeoParquetMetadata> geoMetadataOf(ParquetFileReader reader) {
        String geo = reader.keyValueMetadata().get("geo");
        return Optional.of(GeoParquetMetadata.parse(geo));
    }

    /** A probe that answers every read-only coarse consultation with {@code answer} and keeps every row. */
    private static SpatialReadProbe regionProbe(Decision answer) {
        return new SpatialReadProbe() {

            @Override
            public Decision probe(double minX, double minY, double maxX, double maxY) {
                return Decision.keep();
            }

            @Override
            public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
                return answer;
            }
        };
    }
}
