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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The bounds held by a {@link SpatialBoundsSource} with a box per row group. For each geometry column it keeps one slot
 * per row group, holding the box of that row group where its bounds are known and {@link Optional#empty()} where they
 * are not. A box proving nothing about its geometries ({@link BoundingBox#provesNothing()}) counts as not known. The
 * file bounds of a column are the union of its row-group boxes, known only when each row group has a box.
 */
final class PerRowGroupBoxes {

    private final Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup;
    private final Map<ColumnPath, BoundingBox> fileLevel;

    /**
     * @param perRowGroup the slots of each geometry column, one per row group
     * @param union the union of two row-group boxes, which builds the file bounds
     */
    PerRowGroupBoxes(Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup, BinaryOperator<BoundingBox> union) {
        this.perRowGroup = Map.copyOf(perRowGroup);
        this.fileLevel = Map.copyOf(fileLevelUnions(perRowGroup, union));
    }

    /** See {@link SpatialBoundsSource#fileBounds}. */
    Optional<BoundingBox> fileBounds(ColumnPath geometryColumn) {
        return Optional.ofNullable(fileLevel.get(geometryColumn));
    }

    /** See {@link SpatialBoundsSource#rowGroupBounds}. */
    Optional<BoundingBox> rowGroupBounds(ColumnPath geometryColumn, int rowGroupIndex) {
        List<Optional<BoundingBox>> byGroup = perRowGroup.get(geometryColumn);
        if (byGroup == null || rowGroupIndex < 0 || rowGroupIndex >= byGroup.size()) {
            return Optional.empty();
        }
        return byGroup.get(rowGroupIndex);
    }

    /** One box per geometry column and row group with a box, plus the known file bounds of each column. */
    int retainedBoxCount() {
        return fileLevel.size() + presentBoxes();
    }

    /** The row-group slots filled with a box; the misses hold {@link Optional#empty()} and nothing more. */
    private int presentBoxes() {
        int boxes = 0;
        for (List<Optional<BoundingBox>> byGroup : perRowGroup.values()) {
            for (Optional<BoundingBox> box : byGroup) {
                if (box.isPresent()) {
                    boxes++;
                }
            }
        }
        return boxes;
    }

    /**
     * The file-level box of each column: the {@code union} of its row-group boxes, absent for a column with a row group
     * without a box. Such a row group could hold any geometry, and the file cannot bound it.
     */
    private static Map<ColumnPath, BoundingBox> fileLevelUnions(
            Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup, BinaryOperator<BoundingBox> union) {
        Map<ColumnPath, BoundingBox> fileLevel = HashMap.newHashMap(perRowGroup.size());
        perRowGroup.forEach(
                (column, byGroup) -> unionOfEach(byGroup, union).ifPresent(box -> fileLevel.put(column, box)));
        return fileLevel;
    }

    /**
     * The union of the row-group boxes, or empty as soon as one row group has no box. A box without an extent, recorded
     * for a row group holding no geometry with one, adds nothing.
     */
    private static Optional<BoundingBox> unionOfEach(
            List<Optional<BoundingBox>> byGroup, BinaryOperator<BoundingBox> union) {
        BoundingBox accumulated = null;
        for (Optional<BoundingBox> slot : byGroup) {
            if (slot.isEmpty()) {
                return Optional.empty();
            }
            BoundingBox box = slot.orElseThrow();
            if (!box.hasExtent()) {
                continue;
            }
            accumulated = accumulated == null ? box : union.apply(accumulated, box);
        }
        return Optional.ofNullable(accumulated);
    }
}
