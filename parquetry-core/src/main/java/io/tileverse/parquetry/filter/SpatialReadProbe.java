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
package io.tileverse.parquetry.filter;

/**
 * A stateful, single-use, single-threaded hook consulted during a spatial read at each structural level (file, row
 * group, row). Given a unit's 2D bounding box it decides whether to skip the whole unit, recurse into it, decode it
 * normally, or replace it with a caller-supplied geometry. All spatial policy (resolution, paint state, output needs)
 * lives in the implementation; core only extracts bounding boxes and acts on the decision.
 *
 * <p>The probe is consulted in spatial-visitation order on a single thread; it need not be thread-safe and must never
 * be shared across concurrent reads. Its answer for one unit may depend on what earlier units caused it to record (for
 * example, which pixels a renderer has already painted).
 *
 * <p>Two consultations split by structural granularity. A coarse unit that contains many rows (a file, a row group, a
 * page) is consulted through {@link #probeRegion}, which is read-only: it may only drop a unit already fully covered by
 * earlier rows, and must not record coverage of its own (the unit's individual rows have not been emitted yet). A
 * single row is consulted through {@link #probe}, which may record coverage (paint) when it keeps the row. Keeping the
 * two apart prevents a coarse unit from recording coverage that its rows never fill, which would drop those rows and
 * leave the covered space empty.
 */
@FunctionalInterface
public interface SpatialReadProbe {

    /**
     * Decides what to do with a single row given its 2D bounds. Z is ignored; edges are inclusive. This is the only
     * consultation allowed to record coverage (for example, mark a renderer pixel painted) when it keeps the row.
     */
    Decision probe(double minX, double minY, double maxX, double maxY);

    /**
     * Decides whether a coarse unit (a file, a row group, or a page) may be dropped before it is fetched or decoded,
     * given its 2D bounds. This consultation is READ-ONLY: it may return {@link Decision#skip()} only when the unit's
     * whole bounds are already covered by earlier rows, and otherwise returns {@link Decision#descend()} to recurse; it
     * must never record coverage (the unit's own rows have not been emitted). The default returns {@code Descend},
     * which performs no coarse pre-skip and recurses into every unit.
     */
    default Decision probeRegion(double minX, double minY, double maxX, double maxY) {
        return Decision.descend();
    }

    /** What core does with a probed unit. {@code Skip}/{@code Descend}/{@code Keep} are allocation-free singletons. */
    // S1845: the factories skip()/descend()/keep() intentionally mirror the SKIP/DESCEND/KEEP singletons they return.
    @SuppressWarnings("java:S1845")
    sealed interface Decision permits Decision.Skip, Decision.Descend, Decision.Keep, Decision.Replace {

        /** Drop the whole unit: no fetch, no decode, no recursion. */
        record Skip() implements Decision {}

        /** Cannot decide as a whole; recurse to the next finer level (at the leaf, behaves as {@link Keep}). */
        record Descend() implements Decision {}

        /** Decode and emit this unit normally. */
        record Keep() implements Decision {}

        /** Emit one row whose geometry is {@code geometry}; skip decoding the unit's own geometry. */
        record Replace(Object geometry) implements Decision {}

        Skip SKIP = new Skip();
        Descend DESCEND = new Descend();
        Keep KEEP = new Keep();

        static Skip skip() {
            return SKIP;
        }

        static Descend descend() {
            return DESCEND;
        }

        static Keep keep() {
            return KEEP;
        }

        static Replace replace(Object geometry) {
            return new Replace(geometry);
        }
    }
}
