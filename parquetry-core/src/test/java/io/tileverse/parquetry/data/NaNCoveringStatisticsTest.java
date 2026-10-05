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

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.FooterRewrite;

/**
 * A GeoParquet 1.1 file with a NaN maximum in the bbox covering statistics of one row group. The NaN bounds nothing: a
 * containment query keeps the matching row of that row group with pruning on and off, as it does on the same file with
 * intact statistics.
 */
class NaNCoveringStatisticsTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final ColumnPath COVERING_XMAX = ColumnPath.of("bbox", "xmax");
    private static final ReadOptions METADATA_PRUNING_OFF = ReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useColumnIndexFilter(false)
            .useBloomFilter(false)
            .build();

    @TempDir
    Path tempDir;

    @Test
    void aContainmentQueryKeepsTheRowsOfARowGroupWithANaNCoveringMaximum() throws IOException {
        Path intact = writeCoveredPoints(tempDir.resolve("intact.parquet"));
        Path nanMaximum = FooterRewrite.rewrite(
                intact,
                tempDir.resolve("nan-maximum.parquet"),
                FooterRewrite.statisticsMax(COVERING_XMAX, 0, littleEndianFloat(Float.NaN)));
        Predicate containsTheSecondPoint = new Predicate.Spatial.BboxContains(GEOMETRY, Bbox.of2d(5, 5, 5, 5));

        assertThat(count(nanMaximum, containsTheSecondPoint, ReadOptions.DEFAULTS))
                .isEqualTo(count(nanMaximum, containsTheSecondPoint, METADATA_PRUNING_OFF))
                .isEqualTo(count(intact, containsTheSecondPoint, ReadOptions.DEFAULTS))
                .isEqualTo(1);
    }

    private static long count(Path file, Predicate predicate, ReadOptions options) {
        Projection geometryOnly = Projection.ofPhysical(List.of(GEOMETRY));
        try (ByteRangeSource source = ByteRangeSource.ofFile(file);
                Stream<ParquetRecord> rows = ParquetFileReader.open(source).read(predicate, geometryOnly, options)) {
            return rows.count();
        }
    }

    /** Two row groups of two points, written with a GeoParquet 1.1 bbox covering and no native statistics. */
    private Path writeCoveredPoints(Path file) throws IOException {
        ParquetSchema schema = geometrySchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(2))
                .geoParquetMetadata(GeoParquetMetadataMode.V1_1_ONLY)
                .crsEpsg("geometry", 4326)
                .build();
        double[][] points = {{0, 0}, {5, 5}, {100, 0}, {105, 5}};
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (double[] point : points) {
                MemorySegment wkb = MemorySegment.ofArray(wkbPoint(point[0], point[1]));
                WriteFixtures.appendRow(appender, schema, Map.of(GEOMETRY, wkb));
            }
        }
        return file;
    }

    private static ParquetSchema geometrySchema() {
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                "geometry", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(1);
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
    }

    private static MemorySegment littleEndianFloat(float value) {
        ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putFloat(value);
        return MemorySegment.ofArray(buffer.array());
    }
}
