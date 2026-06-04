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

import java.util.Optional;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Singleton fallback used by {@link SpatialBoundsSource#of} when the file carries no usable bounds in any tier: non-geo
 * files, geo files whose writer skipped all three bounds sources, or columns the chosen tier doesn't cover. Both
 * accessors return {@link Optional#empty()} unconditionally so callers don't have to special-case the absence.
 *
 * <p>Modeled as a single-constant enum: that is the JLS-blessed singleton form (lazy initialization, serializable, no
 * boilerplate) and it slots cleanly into the {@link SpatialBoundsSource} sealed permit list. Sonar's S6548
 * informational rule still flags single-constant enums to prompt a review; the suppression below is that confirmation,
 * since the no-op fallback genuinely is a unit-type value and not a state-bearing object.
 */
@SuppressWarnings("java:S6548")
public enum EmptyBoundsSource implements SpatialBoundsSource {
    INSTANCE;

    @Override
    public Optional<BoundingBox> fileBounds(ColumnPath geometryColumn) {
        return Optional.empty();
    }

    @Override
    public Optional<BoundingBox> rowGroupBounds(ColumnPath geometryColumn, int rowGroupIndex) {
        return Optional.empty();
    }
}
