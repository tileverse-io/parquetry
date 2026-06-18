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
package io.tileverse.parquetry.iceberg;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.prune.ColumnStatistics;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;

class IcebergFileStatsTest {

    private static final int GEOM_FIELD_ID = 2;
    private static final int VAL_FIELD_ID = 1;

    private static final long VAL_LOWER = 100L;
    private static final long VAL_UPPER = 900L;

    private static final double XMIN = -10.5;
    private static final double YMIN = 4.25;
    private static final double XMAX = 12.75;
    private static final double YMAX = 33.5;

    @Test
    void buildsFileStatsFromManifestBounds() {
        IcebergManifests.DataFileRef ref = new IcebergManifests.DataFileRef(
                "s3://bucket/data/file-0.parquet",
                42L,
                Map.of(VAL_FIELD_ID, leLong(VAL_LOWER), GEOM_FIELD_ID, packedXy(XMIN, YMIN)),
                Map.of(VAL_FIELD_ID, leLong(VAL_UPPER), GEOM_FIELD_ID, packedXy(XMAX, YMAX)),
                Map.of());

        List<IcebergField> fields = List.of(
                new IcebergField(GEOM_FIELD_ID, "geom", "geometry"), new IcebergField(VAL_FIELD_ID, "val", "long"));

        FileStats stats = IcebergFileStats.from(ref, fields);

        assertThat(stats.recordCount()).isEqualTo(42L);
        assertGeometryBounds(stats);
        assertColumnBounds(stats);
    }

    @Test
    void skipsGeometryBoundWithUnexpectedLengthWithoutFailing() {
        MemorySegment malformedPoint = MemorySegment.ofArray(new byte[10]);
        IcebergManifests.DataFileRef ref = new IcebergManifests.DataFileRef(
                "s3://bucket/data/file-0.parquet",
                42L,
                Map.of(VAL_FIELD_ID, leLong(VAL_LOWER), GEOM_FIELD_ID, malformedPoint),
                Map.of(VAL_FIELD_ID, leLong(VAL_UPPER), GEOM_FIELD_ID, malformedPoint),
                Map.of());

        List<IcebergField> fields = List.of(
                new IcebergField(GEOM_FIELD_ID, "geom", "geometry"), new IcebergField(VAL_FIELD_ID, "val", "long"));

        FileStats stats = IcebergFileStats.from(ref, fields);

        assertThat(stats.geometryBounds()).doesNotContainKey(ColumnPath.of("geom"));
        assertColumnBounds(stats);
    }

    @Test
    void skipsColumnBoundWithUnsupportedTypeWithoutFailing() {
        IcebergManifests.DataFileRef ref = new IcebergManifests.DataFileRef(
                "s3://bucket/data/file-0.parquet",
                42L,
                Map.of(VAL_FIELD_ID, leLong(VAL_LOWER)),
                Map.of(VAL_FIELD_ID, leLong(VAL_UPPER)),
                Map.of());

        List<IcebergField> fields = List.of(new IcebergField(VAL_FIELD_ID, "amount", "decimal"));

        FileStats stats = IcebergFileStats.from(ref, fields);

        assertThat(stats.columns()).doesNotContainKey(ColumnPath.of("amount"));
    }

    private void assertGeometryBounds(FileStats stats) {
        BoundingBox expected = new BoundingBox(
                XMIN,
                XMAX,
                YMIN,
                YMAX,
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty());
        assertThat(stats.geometryBounds()).containsEntry(ColumnPath.of("geom"), expected);
    }

    private void assertColumnBounds(FileStats stats) {
        ColumnStatistics column = stats.columns().get(ColumnPath.of("val"));
        assertThat(column).isNotNull();
        assertThat(column.min()).contains(new Value.LongVal(VAL_LOWER));
        assertThat(column.max()).contains(new Value.LongVal(VAL_UPPER));
    }

    private static MemorySegment leLong(long value) {
        byte[] bytes =
                ByteBuffer.allocate(8).order(LITTLE_ENDIAN).putLong(value).array();
        return MemorySegment.ofArray(bytes);
    }

    private static MemorySegment packedXy(double x, double y) {
        byte[] bytes = ByteBuffer.allocate(16)
                .order(LITTLE_ENDIAN)
                .putDouble(x)
                .putDouble(y)
                .array();
        return MemorySegment.ofArray(bytes);
    }
}
