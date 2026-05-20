/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.filter.spatial;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * {@link SpatialBoundsSource} backed by GeoParquet 2.0 native {@code GeospatialStatistics.bbox} carried inside each row
 * group's {@link ColumnMetaData}. Most precise of the three tiers: per-row-group bounds are exact, and the file bounds
 * are the union of those.
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
     * Returns a {@code NativeStatsSource} when at least one row-group column metadata carries a native
     * {@code geospatial_statistics.bbox}; otherwise empty so the dispatch falls through to the next tier.
     */
    static Optional<SpatialBoundsSource> tryBuild(FileMetaData footer) {
        Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup = collectPerRowGroupBoxes(footer);
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

    /**
     * Walks every row group and records the {@link BoundingBox} (if any) for each column under
     * {@code geospatial_statistics}. Columns with at least one row group that carries a bbox land in the result; the
     * per-row-group list always has one slot per row group, with {@link Optional#empty()} for the misses.
     */
    private static Map<ColumnPath, List<Optional<BoundingBox>>> collectPerRowGroupBoxes(FileMetaData footer) {
        List<RowGroup> rowGroups = footer.rowGroups();
        Map<ColumnPath, List<Optional<BoundingBox>>> perColumn = new LinkedHashMap<>();
        for (int i = 0; i < rowGroups.size(); i++) {
            for (ColumnChunk chunk : rowGroups.get(i).columns()) {
                recordChunk(chunk, i, rowGroups.size(), perColumn);
            }
        }
        // Drop columns where no row group carried a bbox.
        perColumn.entrySet().removeIf(e -> e.getValue().stream().allMatch(Optional::isEmpty));
        return perColumn;
    }

    private static void recordChunk(
            ColumnChunk chunk,
            int rowGroupIndex,
            int rowGroupCount,
            Map<ColumnPath, List<Optional<BoundingBox>>> perColumn) {
        Optional<ColumnMetaData> meta = chunk.metaData();
        if (meta.isEmpty()) {
            return;
        }
        ColumnMetaData m = meta.orElseThrow();
        Optional<BoundingBox> bbox = m.geospatialStatistics().flatMap(s -> s.bbox());
        if (bbox.isEmpty()) {
            return;
        }
        ColumnPath path = new ColumnPath(m.pathInSchema());
        List<Optional<BoundingBox>> list = perColumn.computeIfAbsent(path, _ -> emptySlots(rowGroupCount));
        list.set(rowGroupIndex, bbox);
    }

    private static List<Optional<BoundingBox>> emptySlots(int count) {
        List<Optional<BoundingBox>> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(Optional.empty());
        }
        return list;
    }

    /**
     * Returns the per-column union (file-level bbox) of every row-group bbox known to this source. Antimeridian-wrap
     * boxes ({@link BoundingBox#wrapsAntimeridian()}) are passed through verbatim: combining wrap-aware boxes requires
     * splitting them in two, and that policy belongs at the aggregator layer, not here.
     */
    private static Map<ColumnPath, BoundingBox> unionAcrossRowGroups(
            Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup) {
        Map<ColumnPath, BoundingBox> result = new LinkedHashMap<>();
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
     * the result is absent too. Z/M axes can appear on only some row groups and the file-level union must carry them
     * whenever at least one row group did.
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
