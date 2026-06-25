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
package io.tileverse.parquetry.stac;

import java.util.OptionalDouble;

import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;

import io.tileverse.stac.StacItem;

/**
 * Builds a core {@link FileStats} from a STAC item. The item's bbox becomes a geometry bound on the collection's
 * primary geometry column, which lets {@link io.tileverse.parquetry.filter.prune.FilePruner} skip a spatially disjoint
 * part before its footer opens. A STAC item does not declare a record count; the stats report {@code -1}. Mapping is
 * best-effort: an item with a missing or unrecognized bbox contributes no bound and is always a pruning survivor.
 */
final class StacFileStats {

    private StacFileStats() {}

    static FileStats from(StacItem item, String geometryColumn) {
        FileStats.Builder builder = FileStats.builder().recordCount(-1L);
        BoundingBox box = boundsOf(item.bbox());
        if (box != null) {
            builder.geometryBounds(ColumnPath.of(geometryColumn), box);
        }
        return builder.build();
    }

    private static BoundingBox boundsOf(double[] bbox) {
        if (bbox != null && bbox.length == 4) {
            return new BoundingBox(
                    bbox[0],
                    bbox[2],
                    bbox[1],
                    bbox[3],
                    OptionalDouble.empty(),
                    OptionalDouble.empty(),
                    OptionalDouble.empty(),
                    OptionalDouble.empty());
        }
        if (bbox != null && bbox.length == 6) {
            return new BoundingBox(
                    bbox[0],
                    bbox[3],
                    bbox[1],
                    bbox[4],
                    OptionalDouble.of(bbox[2]),
                    OptionalDouble.of(bbox[5]),
                    OptionalDouble.empty(),
                    OptionalDouble.empty());
        }
        return null;
    }
}
