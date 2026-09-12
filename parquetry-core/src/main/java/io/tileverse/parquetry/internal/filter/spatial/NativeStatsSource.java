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
package io.tileverse.parquetry.internal.filter.spatial;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.footer.CompactFooter;
import io.tileverse.parquetry.internal.footer.LeafIndex;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * {@link SpatialBoundsSource} backed by the GeoParquet 2.0 native {@code GeospatialStatistics.bbox} recorded for each
 * column chunk. Most precise of the three tiers: per-row-group bounds are exact, and the file bounds are the union of
 * those.
 *
 * <p>Construction precomputes both the per-row-group bboxes and the file-level union. Lookups are then a single map
 * read. Callers that need to refresh after a footer change must build a new instance.
 */
final class NativeStatsSource implements SpatialBoundsSource {

    private final Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup;
    private final Map<ColumnPath, BoundingBox> fileLevel;

    private NativeStatsSource(
            Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup, Map<ColumnPath, BoundingBox> fileLevel) {
        this.perRowGroup = perRowGroup;
        this.fileLevel = fileLevel;
    }

    /**
     * Returns a {@code NativeStatsSource} when at least one column chunk records a native
     * {@code geospatial_statistics.bbox}; otherwise empty, which sends the dispatch on to the next tier.
     *
     * <p>A footer whose count of geospatial extents is zero is declined outright. That covers every plain Parquet file
     * and every GeoParquet 1.x file whose writer recorded none, neither of which then pays for a walk across the chunk
     * table.
     */
    static Optional<SpatialBoundsSource> tryBuild(CompactFooter footer, LeafIndex leaves) {
        if (footer.geoExtentCount() == 0) {
            return Optional.empty();
        }
        Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup = collectBoxesByColumn(footer, leaves);
        if (perRowGroup.isEmpty()) {
            return Optional.empty();
        }
        Map<ColumnPath, BoundingBox> fileLevel = unionAcrossRowGroups(perRowGroup);
        return Optional.of(new NativeStatsSource(Map.copyOf(perRowGroup), Map.copyOf(fileLevel)));
    }

    @Override
    public Optional<BoundingBox> fileBounds(ColumnPath geometryColumn) {
        return Optional.ofNullable(fileLevel.get(geometryColumn));
    }

    @Override
    public Optional<BoundingBox> rowGroupBounds(ColumnPath geometryColumn, int rowGroupIndex) {
        List<Optional<BoundingBox>> byGroup = perRowGroup.get(geometryColumn);
        if (byGroup == null || rowGroupIndex < 0 || rowGroupIndex >= byGroup.size()) {
            return Optional.empty();
        }
        return byGroup.get(rowGroupIndex);
    }

    /** One box per geometry column and row group with native statistics, plus that column's file-level union. */
    @Override
    public int retainedBoxCount() {
        return fileLevel.size() + PerRowGroupBoxes.presentBoxes(perRowGroup);
    }

    /**
     * Records the {@link BoundingBox} (if any) of every leaf column in every row group. Only columns with at least one
     * row-group bbox land in the result; their per-row-group list always has one slot per row group, with
     * {@link Optional#empty()} for the misses.
     */
    private static Map<ColumnPath, List<Optional<BoundingBox>>> collectBoxesByColumn(
            CompactFooter footer, LeafIndex leaves) {
        Map<ColumnPath, List<Optional<BoundingBox>>> perColumn = new HashMap<>();
        for (int leaf = 0; leaf < footer.leafCount(); leaf++) {
            if (recordsAnyGeoExtent(footer, leaf)) {
                perColumn.put(leaves.pathOf(leaf), boxesAcrossRowGroups(footer, leaf));
            }
        }
        return perColumn;
    }

    /**
     * Whether any row group records a geospatial extent for this leaf. Most leaves of a geo file are attribute columns
     * with none, and answering from the chunk flags leaves their per-row-group list unallocated - one slot per row
     * group, on a file whose row groups run to the tens of thousands.
     */
    private static boolean recordsAnyGeoExtent(CompactFooter footer, int leaf) {
        for (int rowGroup = 0; rowGroup < footer.rowGroupCount(); rowGroup++) {
            if (footer.recordsGeoExtent(rowGroup, leaf)) {
                return true;
            }
        }
        return false;
    }

    private static List<Optional<BoundingBox>> boxesAcrossRowGroups(CompactFooter footer, int leaf) {
        List<Optional<BoundingBox>> boxes = new ArrayList<>(footer.rowGroupCount());
        for (int rowGroup = 0; rowGroup < footer.rowGroupCount(); rowGroup++) {
            boxes.add(boxAt(footer, rowGroup, leaf));
        }
        return boxes;
    }

    private static Optional<BoundingBox> boxAt(CompactFooter footer, int rowGroup, int leaf) {
        Optional<ChunkMeta> chunk = footer.chunkIfPresent(rowGroup, leaf);
        return chunk.flatMap(ChunkMeta::geoExtent);
    }

    /**
     * Returns the per-column union (file-level bbox) of every row-group bbox known to this source. Antimeridian-wrap
     * boxes ({@link BoundingBox#wrapsAntimeridian()}) are passed through verbatim: combining wrap-aware boxes requires
     * splitting them in two, and that policy belongs at the aggregator layer, not here.
     */
    private static Map<ColumnPath, BoundingBox> unionAcrossRowGroups(
            Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup) {
        Map<ColumnPath, BoundingBox> result = HashMap.newHashMap(perRowGroup.size());
        perRowGroup.forEach((path, list) -> {
            BoundingBox acc = null;
            for (Optional<BoundingBox> slot : list) {
                if (slot.isPresent()) {
                    acc = acc == null ? slot.orElseThrow() : union(acc, slot.orElseThrow());
                }
            }
            if (acc != null) {
                result.put(path, acc);
            }
        });
        return result;
    }

    private static BoundingBox union(BoundingBox a, BoundingBox b) {
        return new BoundingBox(
                Math.min(a.xmin(), b.xmin()),
                Math.max(a.xmax(), b.xmax()),
                Math.min(a.ymin(), b.ymin()),
                Math.max(a.ymax(), b.ymax()),
                minOptional(a.zmin(), b.zmin()),
                maxOptional(a.zmax(), b.zmax()),
                minOptional(a.mmin(), b.mmin()),
                maxOptional(a.mmax(), b.mmax()));
    }

    /**
     * Element-wise min for two optional axes. When either side is absent, the other is preserved; when both are absent,
     * the result is absent too. Z/M axes can appear on only some row groups and the file-level union must keep them
     * whenever at least one row group recorded them.
     */
    private static OptionalDouble minOptional(OptionalDouble a, OptionalDouble b) {
        if (a.isEmpty()) {
            return b;
        }
        if (b.isEmpty()) {
            return a;
        }
        return OptionalDouble.of(Math.min(a.getAsDouble(), b.getAsDouble()));
    }

    private static OptionalDouble maxOptional(OptionalDouble a, OptionalDouble b) {
        if (a.isEmpty()) {
            return b;
        }
        if (b.isEmpty()) {
            return a;
        }
        return OptionalDouble.of(Math.max(a.getAsDouble(), b.getAsDouble()));
    }
}
