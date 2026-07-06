/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.read.RowGroupGate;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.SpatialDecimationGate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.geoparquet.GeometryColumns;

/**
 * Builds the spatial decimation gates for one file. A read whose {@link ReadOptions} supplies a
 * {@link SpatialReadProbe} consults the gates to drop already-covered units by bounding box: the row-group gate skips a
 * covered row group before any fetch, and the leaf gate narrows a decoded batch's surviving rows. When the file exposes
 * no geometry column, or a read supplies no probe, the gates are empty and the read is unchanged. Constructed once per
 * file.
 *
 * <p>The two gates of one read share the read's single probe instance, hence one accumulating coverage state spans the
 * row-group and leaf levels. The coarse row-group level consults the read-only {@link SpatialReadProbe#probeRegion},
 * which records no coverage; only the leaf level paints.
 */
final class SpatialReadGates {

    private final Optional<ColumnPath> primaryGeometry;
    private final SpatialBoundsSource boundsSource;
    private final Map<RowGroup, Integer> rowGroupIndices;

    SpatialReadGates(FileMetaData footer, ParquetSchema fileSchema, Optional<GeoParquetMetadata> geoMetadata) {
        this.primaryGeometry = GeometryColumns.primary(fileSchema, geoMetadata);
        this.boundsSource = SpatialBoundsSource.of(footer, fileSchema, geoMetadata);
        this.rowGroupIndices = indexByRowGroup(footer);
    }

    /**
     * The leaf (per-row) gate for this read, present only when {@code options} supplies a probe and the file has a
     * primary geometry column.
     */
    Optional<SpatialDecimationGate> leafGate(ReadOptions options) {
        return probeFor(options).map(probe -> new SpatialDecimationGate(primaryGeometry.orElseThrow(), probe));
    }

    /**
     * The row-group gate for this read, present only when {@code options} supplies a probe and the file has a primary
     * geometry column. The gate drops a survivor whose geometry bounds the probe reports as already covered through the
     * read-only {@link SpatialReadProbe#probeRegion}; a survivor with no recorded bounds is never dropped.
     */
    Optional<RowGroupGate> rowGroupGate(List<RowGroupSurvivor> survivors, ReadOptions options) {
        return probeFor(options).map(probe -> {
            List<Optional<Bbox>> perSurvivorBounds = perSurvivorBounds(survivors);
            return position -> probeSkips(probe, perSurvivorBounds.get(position));
        });
    }

    /** The read's probe, present only when the read supplies one and the file has a primary geometry column. */
    private Optional<SpatialReadProbe> probeFor(ReadOptions options) {
        if (primaryGeometry.isEmpty()) {
            return Optional.empty();
        }
        return options.spatialReadProbe();
    }

    private List<Optional<Bbox>> perSurvivorBounds(List<RowGroupSurvivor> survivors) {
        ColumnPath geometry = primaryGeometry.orElseThrow();
        List<Optional<Bbox>> bounds = new ArrayList<>(survivors.size());
        for (RowGroupSurvivor survivor : survivors) {
            int absoluteIndex = rowGroupIndices.get(survivor.rowGroup());
            Optional<BoundingBox> box = boundsSource.rowGroupBounds(geometry, absoluteIndex);
            bounds.add(box.map(SpatialReadGates::toBbox));
        }
        return bounds;
    }

    private static boolean probeSkips(SpatialReadProbe probe, Optional<Bbox> bounds) {
        if (bounds.isEmpty()) {
            return false;
        }
        Bbox box = bounds.orElseThrow();
        SpatialReadProbe.Decision decision = probe.probeRegion(box.minX(), box.minY(), box.maxX(), box.maxY());
        return decision instanceof SpatialReadProbe.Decision.Skip;
    }

    /** Maps each footer {@link RowGroup} instance to its absolute file-order index. */
    private static Map<RowGroup, Integer> indexByRowGroup(FileMetaData footer) {
        List<RowGroup> rowGroups = footer.rowGroups();
        Map<RowGroup, Integer> indices = new IdentityHashMap<>(rowGroups.size());
        for (int index = 0; index < rowGroups.size(); index++) {
            indices.put(rowGroups.get(index), index);
        }
        return indices;
    }

    private static Bbox toBbox(BoundingBox box) {
        return Bbox.of2d(box.xmin(), box.ymin(), box.xmax(), box.ymax());
    }
}
