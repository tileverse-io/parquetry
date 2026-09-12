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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The row-group bounds precomputed by a {@link SpatialBoundsSource}: per geometry column, one slot per row group,
 * holding that row group's box where its bounds are known and {@link Optional#empty()} where they are not.
 */
final class PerRowGroupBoxes {

    private PerRowGroupBoxes() {}

    /** The row-group slots filled with a box; the misses hold {@link Optional#empty()} and nothing more. */
    static int presentBoxes(Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup) {
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
}
