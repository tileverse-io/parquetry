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
package io.tileverse.parquetry.filter;

/**
 * A stateful, single-use, single-threaded hook consulted during a spatial read at each structural level (file, row
 * group, page, row). Given a unit's 2D bounding box it decides whether to skip the whole unit, recurse into it, decode
 * it normally, or substitute it with output of its own. All spatial policy (resolution, paint state, output needs)
 * lives in the implementation; core only extracts bounding boxes and acts on the decision.
 *
 * <p>The probe is consulted in spatial-visitation order on a single thread; it need not be thread-safe and must never
 * be shared across concurrent reads. Its answer for one unit may depend on what earlier units caused it to record (for
 * example, which pixels a renderer has already painted).
 *
 * <p>Three consultations, split by what the reader knows about the unit. A single row is consulted through
 * {@link #probe}, which may record coverage (paint) when it keeps the row. A coarse unit (a file, a row group, a page)
 * of which the reader knows only the bounds is consulted through {@link #probeRegion}, which is read-only: it may drop
 * a unit already fully covered by earlier output and must not record coverage of its own, because the unit may hold
 * rows outside the query and none of its rows has been emitted. A coarse unit that the reader has proven to hold at
 * least one geometry, every non-null geometry of which satisfies the query, is consulted through
 * {@link #probeAcceptedRegion}, which may additionally answer {@link Decision#substitute()}: the probe has then
 * emitted, through an output of its own, one substitute standing for the whole unit, has recorded the coverage provided
 * by that substitute, and core drops the unit without fetching or decoding it.
 */
@FunctionalInterface
public interface SpatialReadProbe {

    /**
     * Decides what to do with a single row given its 2D bounds. Z is ignored; edges are inclusive. This is the only
     * consultation allowed to record coverage (for example, mark a renderer pixel painted) when it keeps the row. A
     * {@link Decision.Substitute} answer here is rejected by the reader: a row is never substituted.
     */
    Decision probe(double minX, double minY, double maxX, double maxY);

    /**
     * Decides whether a coarse unit (a file, a row group, or a page) may be dropped before it is fetched or decoded,
     * given its 2D bounds. This consultation is READ-ONLY: it may return {@link Decision#skip()} only when the unit's
     * whole bounds are already covered by earlier output, and otherwise returns {@link Decision#descend()} to recurse;
     * it must never record coverage (the unit's own rows have not been emitted). The default returns {@code Descend},
     * which performs no coarse pre-skip and recurses into every unit.
     */
    default Decision probeRegion(double minX, double minY, double maxX, double maxY) {
        return Decision.descend();
    }

    /**
     * Decides what to do with a coarse unit (a file, a row group, or a page) that the reader has proven to hold at
     * least one geometry, every non-null geometry of which satisfies the query, given its 2D bounds. Like
     * {@link #probeRegion} it answers {@link Decision#skip()} for a unit whose whole bounds are already covered and
     * {@link Decision#descend()} for a unit that it cannot decide as a whole. Beyond that it may answer
     * {@link Decision#substitute()}: the probe has emitted, through an output of its own, one substitute standing for
     * the unit, has recorded the coverage provided by that substitute, and core drops the unit without fetching or
     * decoding it. The default delegates to {@link #probeRegion}, hence a probe implementing only the read-only
     * contract never substitutes.
     */
    default Decision probeAcceptedRegion(double minX, double minY, double maxX, double maxY) {
        return probeRegion(minX, minY, maxX, maxY);
    }

    /** What core does with a probed unit. Every decision is an allocation-free singleton. */
    // S1845: the factories skip()/descend()/keep()/substitute() intentionally mirror the singletons that they return.
    @SuppressWarnings("java:S1845")
    sealed interface Decision permits Decision.Skip, Decision.Descend, Decision.Keep, Decision.Substitute {

        /** Drop the whole unit: no fetch, no decode, no recursion. */
        record Skip() implements Decision {}

        /** Cannot decide as a whole; recurse to the next finer level (at the leaf, behaves as {@link Keep}). */
        record Descend() implements Decision {}

        /** Decode and emit this unit normally. */
        record Keep() implements Decision {}

        /**
         * The probe has emitted a substitute for the whole unit through an output of its own: drop the unit without
         * fetching or decoding it. Answered only to {@link #probeAcceptedRegion}. The reader rejects it at row level
         * and from the read-only {@link #probeRegion}, whose unit is not proven to satisfy the query.
         *
         * <p>That output of the probe's own is drained by a row-producing read alone. A batch read
         * ({@code readBatches}) drops the substituted unit and emits nothing in its place, hence a substituting probe
         * belongs to a row read.
         */
        record Substitute() implements Decision {}

        Skip SKIP = new Skip();
        Descend DESCEND = new Descend();
        Keep KEEP = new Keep();
        Substitute SUBSTITUTE = new Substitute();

        static Skip skip() {
            return SKIP;
        }

        static Descend descend() {
            return DESCEND;
        }

        static Keep keep() {
            return KEEP;
        }

        static Substitute substitute() {
            return SUBSTITUTE;
        }
    }
}
