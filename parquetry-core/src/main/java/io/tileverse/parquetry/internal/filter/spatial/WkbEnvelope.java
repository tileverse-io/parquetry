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

import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.internal.wkb.CoordinateRun;
import io.tileverse.parquetry.internal.wkb.WkbCursor;
import io.tileverse.parquetry.internal.wkb.WkbTypeCode;

/**
 * 2D envelopes and bbox relations computed straight from a WKB (Well-Known Binary) geometry payload, without building
 * an engine geometry. The wire-level walk lives in {@link WkbCursor} and this class drives it through the geometry's
 * structure, folding each coordinate run into the result.
 *
 * <p>Both type-code encodings are accepted on input, ISO and EWKB (see {@link WkbTypeCode}); the type code reported to
 * a {@link Visitor} is the ISO form, which is what the parquet-format {@code geospatial_types} statistic mandates.
 *
 * <p>Every entry point reads one geometry starting at the byte-order byte and walks forward by structure within the
 * value's byte range; a truncated value throws {@link MalformedFileException} rather than reading adjacent bytes.
 *
 * <p>Each entry point starts its own cursor and one method consumes all of a geometry's counted runs, which keeps the
 * walk shallow: every frame between a caller's loop and the coordinate accessor counts against the JIT's inlining
 * budget, and the coordinate read nearly exhausts that budget on its own (see {@link WkbCursor}).
 */
public final class WkbEnvelope {

    private WkbEnvelope() {}

    /**
     * Receives the geometry data of a WKB payload during a structural walk: each geometry's ISO type code, then its
     * coordinate runs (a point, a linestring, each polygon ring, each multipoint member), one call per run.
     */
    public interface Visitor {

        /**
         * The ISO WKB type code (base kind plus any {@code +1000}/{@code +2000}/{@code +3000} Z/M offset) of a geometry
         * about to be traversed. An EWKB-flagged input is reported in its equivalent ISO form.
         */
        default void geometryType(int isoType) {}

        /**
         * Receives one coordinate run; the view is valid only for the duration of the call. An empty run is not
         * reported; a {@code POINT EMPTY} arrives as a run of one NaN coordinate. Return {@code false} to stop the walk
         * immediately.
         */
        boolean run(CoordinateRun run);
    }

    /**
     * Walks the WKB geometry, driving {@code visitor}; stops early when a run callback returns false.
     *
     * @param wkb one geometry's WKB, with the byte-order byte at offset 0
     * @param visitor receives each geometry's type code and coordinate runs
     */
    public static void walk(MemorySegment wkb, Visitor visitor) {
        walk(wkb, 0L, wkb.byteSize(), visitor);
    }

    /**
     * Walks the WKB geometry at {@code [offset, offset + length)} of {@code backing} in place, driving {@code visitor};
     * stops early when a run callback returns false.
     *
     * <p>{@link #compute} and {@link #matches} start their own cursor rather than calling this method, because they run
     * on the read's hot path and every frame counts there (see this class's javadoc).
     */
    public static void walk(MemorySegment backing, long offset, long length, Visitor visitor) {
        WkbCursor cursor = new WkbCursor(backing, offset, length);
        consumeGeometry(cursor, visitor);
    }

    /**
     * Computes the full 2D envelope of the WKB geometry without building an engine geometry; Z and M are ignored. The
     * spatial decimation gate uses this to bound a row's geometry directly from its WKB.
     *
     * @param wkb one geometry's WKB, with the byte-order byte at offset 0
     * @return the 2D bounding box spanning every coordinate
     */
    public static Bbox compute(MemorySegment wkb) {
        return compute(wkb, 0L, wkb.byteSize());
    }

    /** The full 2D envelope of the WKB geometry at {@code [offset, offset + length)} of {@code backing}, in place. */
    public static Bbox compute(MemorySegment backing, long offset, long length) {
        EnvelopeVisitor visitor = new EnvelopeVisitor();
        WkbCursor cursor = new WkbCursor(backing, offset, length);
        consumeGeometry(cursor, visitor);
        return visitor.toBbox();
    }

    /**
     * Decides a bbox relation between the WKB geometry and the predicate's query box, stopping the walk the moment the
     * answer is fixed.
     *
     * <p>The three open-ended relations exploit the running 2D envelope only ever widening as the walk proceeds: each
     * overlap or enclosure condition is monotone, hence once it holds it stays true, and the first escaping vertex of a
     * coveredBy test fixes the answer. Equality needs the exact final extent, hence it walks to completion.
     *
     * @param p the spatial relation and its query box
     * @param wkb one geometry's WKB, with the byte-order byte at offset 0
     * @return whether the geometry's 2D bbox satisfies the relation
     */
    public static boolean matches(Predicate.Spatial p, MemorySegment wkb) {
        return matches(p, wkb, 0L, wkb.byteSize());
    }

    /** {@link #matches(Predicate.Spatial, MemorySegment)} over the value at {@code [offset, offset + length)}. */
    public static boolean matches(Predicate.Spatial p, MemorySegment backing, long offset, long length) {
        Bbox q = p.bbox();
        // The walk's return value says only whether a visitor cut it short; each relation visitor holds the answer.
        return switch (p) {
            case Predicate.Spatial.BboxIntersects _ -> {
                IntersectsVisitor visitor = new IntersectsVisitor(q);
                WkbCursor cursor = new WkbCursor(backing, offset, length);
                consumeGeometry(cursor, visitor);
                yield visitor.decided;
            }
            case Predicate.Spatial.BboxContains _ -> {
                ContainsVisitor visitor = new ContainsVisitor(q);
                WkbCursor cursor = new WkbCursor(backing, offset, length);
                consumeGeometry(cursor, visitor);
                yield visitor.decided;
            }
            case Predicate.Spatial.BboxCoveredBy _ -> {
                CoveredByVisitor visitor = new CoveredByVisitor(q);
                WkbCursor cursor = new WkbCursor(backing, offset, length);
                consumeGeometry(cursor, visitor);
                yield visitor.coveredBy();
            }
            case Predicate.Spatial.BboxEquals _ ->
                compute(backing, offset, length).sameBox2d(q);
        };
    }

    /**
     * Consumes one geometry starting at the cursor's current position: its header, then its runs, recursing through
     * containers. Returns {@code false} when the visitor requested an early stop.
     */
    private static boolean consumeGeometry(WkbCursor cursor, Visitor visitor) {
        cursor.readHeader();
        visitor.geometryType(cursor.isoType());
        return switch (cursor.baseType()) {
            case WkbTypeCode.POINT -> visitor.run(cursor.run(1));
            case WkbTypeCode.LINESTRING -> consumeRuns(cursor, visitor, 1);
            case WkbTypeCode.POLYGON -> {
                int numRings = cursor.readCount(Integer.BYTES);
                yield consumeRuns(cursor, visitor, numRings);
            }
            case WkbTypeCode.MULTIPOINT,
                    WkbTypeCode.MULTILINESTRING,
                    WkbTypeCode.MULTIPOLYGON,
                    WkbTypeCode.GEOMETRYCOLLECTION -> consumeCollection(cursor, visitor);
            default -> throw new MalformedFileException("Unsupported WKB geometry type: " + cursor.baseType());
        };
    }

    /**
     * Consumes {@code runCount} counted runs of the geometry at the cursor: one for a linestring, one per ring for a
     * polygon. Each run states its own coordinate count; an empty run is not reported.
     */
    private static boolean consumeRuns(WkbCursor cursor, Visitor visitor, int runCount) {
        int bytesPerCoordinate = cursor.headerDimensions().count() * Double.BYTES;
        for (int i = 0; i < runCount; i++) {
            int numPoints = cursor.readCount(bytesPerCoordinate);
            if (numPoints == 0) {
                continue;
            }
            if (!visitor.run(cursor.run(numPoints))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Walks a collection (MULTIPOINT / MULTILINESTRING / MULTIPOLYGON / GEOMETRYCOLLECTION). Every member has its own
     * byte-order byte and type code; each one recurses from the top of {@link #consumeGeometry}.
     */
    private static boolean consumeCollection(WkbCursor cursor, Visitor visitor) {
        int numElements = cursor.readCount(WkbCursor.MIN_GEOMETRY_BYTES);
        for (int i = 0; i < numElements; i++) {
            if (!consumeGeometry(cursor, visitor)) {
                return false;
            }
        }
        return true;
    }

    /** Running 2D min/max envelope; the per-coordinate widen logic shared by the envelope-based visitors. */
    private abstract static class RunningEnvelope {
        double minX = Double.POSITIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;

        final void widen(double x, double y) {
            if (x < minX) {
                minX = x;
            }
            if (y < minY) {
                minY = y;
            }
            if (x > maxX) {
                maxX = x;
            }
            if (y > maxY) {
                maxY = y;
            }
        }
    }

    /** Accumulates the full 2D envelope; ignores Z/M. */
    private static final class EnvelopeVisitor extends RunningEnvelope implements Visitor {
        @Override
        public boolean run(CoordinateRun run) {
            int size = run.size();
            for (int i = 0; i < size; i++) {
                widen(run.x(i), run.y(i));
            }
            return true;
        }

        Bbox toBbox() {
            return Bbox.of2d(minX, minY, maxX, maxY);
        }
    }

    /** Stops true once the running envelope overlaps the query box on all four edges. */
    private static final class IntersectsVisitor extends RunningEnvelope implements Visitor {
        private final Bbox q;
        boolean decided;

        IntersectsVisitor(Bbox q) {
            this.q = q;
        }

        @Override
        public boolean run(CoordinateRun run) {
            int size = run.size();
            for (int i = 0; i < size; i++) {
                widen(run.x(i), run.y(i));
                if (minX <= q.maxX() && maxX >= q.minX() && minY <= q.maxY() && maxY >= q.minY()) {
                    decided = true;
                    return false;
                }
            }
            return true;
        }
    }

    /** Stops true once the running envelope encloses the query box on all four edges. */
    private static final class ContainsVisitor extends RunningEnvelope implements Visitor {
        private final Bbox q;
        boolean decided;

        ContainsVisitor(Bbox q) {
            this.q = q;
        }

        @Override
        public boolean run(CoordinateRun run) {
            int size = run.size();
            for (int i = 0; i < size; i++) {
                widen(run.x(i), run.y(i));
                if (minX <= q.minX() && maxX >= q.maxX() && minY <= q.minY() && maxY >= q.maxY()) {
                    decided = true;
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * True when at least one real vertex exists and none escapes the query box; the walk stops at the first escaping
     * vertex, which fixes the answer as false. Non-finite ordinates are not real vertices, hence an empty geometry is
     * covered by nothing.
     */
    private static final class CoveredByVisitor implements Visitor {
        private final Bbox q;
        private boolean sawVertex;
        private boolean escaped;

        CoveredByVisitor(Bbox q) {
            this.q = q;
        }

        @Override
        public boolean run(CoordinateRun run) {
            int size = run.size();
            for (int i = 0; i < size; i++) {
                double x = run.x(i);
                double y = run.y(i);
                if (Double.isNaN(x) || Double.isNaN(y)) {
                    continue;
                }
                sawVertex = true;
                if (x < q.minX() || x > q.maxX() || y < q.minY() || y > q.maxY()) {
                    escaped = true;
                    return false;
                }
            }
            return true;
        }

        boolean coveredBy() {
            return sawVertex && !escaped;
        }
    }
}
