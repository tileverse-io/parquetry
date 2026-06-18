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
package io.tileverse.parquetry.internal.filter.spatial;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * A bounds source backed by explicitly-supplied file-level bounding boxes, one per geometry column. Built by callers
 * that already hold a file's bounds from outside the Parquet footer (an Iceberg manifest, for instance). It has only
 * file-level bounds; every row-group query returns the same file box.
 */
public final class SuppliedBoundsSource implements SpatialBoundsSource {

    private final Map<ColumnPath, BoundingBox> fileBounds;

    public SuppliedBoundsSource(Map<ColumnPath, BoundingBox> fileBounds) {
        this.fileBounds = Map.copyOf(Objects.requireNonNull(fileBounds, "fileBounds"));
    }

    @Override
    public Optional<BoundingBox> fileBounds(ColumnPath geometryColumn) {
        return Optional.ofNullable(fileBounds.get(geometryColumn));
    }

    @Override
    public Optional<BoundingBox> rowGroupBounds(ColumnPath geometryColumn, int rowGroupIndex) {
        return fileBounds(geometryColumn);
    }
}
