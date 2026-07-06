/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.lang.foreign.MemorySegment;
import java.util.Optional;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * SPI for pushing an exact geometry filter into the parquetry read path.
 *
 * <p>Callers implement this interface to provide a geometry-engine-specific exact predicate. The reader uses it in two
 * ways:
 *
 * <ol>
 *   <li><b>Coarse pruning</b>: {@link #pruningPredicate()} returns a bbox relation that is a NECESSARY CONDITION of
 *       {@link #matches}. The reader evaluates that bbox relation via the standard spatial pruning tiers (row-group
 *       bounds, covering-column rewrite) to eliminate row groups and pages before any geometry decoding.
 *   <li><b>Exact per-row test</b>: {@link #gate(MemorySegment)} decodes the WKB and tests the resulting domain
 *       geometry; a row is retained only when {@code gate} returns a present value. The decoded geometry is used only
 *       for the test today - output materialization decodes the WKB again. Reusing it as the output value is a possible
 *       future optimization.
 * </ol>
 *
 * <p>Soundness contract: {@code matches(g)} being {@code true} must imply the bbox relation from
 * {@link #pruningPredicate()} holds for {@code g}'s bounding box. An unsound (too-tight) lowering silently drops rows
 * that would otherwise pass; the reader cannot verify soundness. Return {@link Optional#empty()} from
 * {@link #pruningPredicate()} when no sound bbox lowering exists - the read then scans every row without page pruning.
 *
 * <p>Coordinate reference system: the test runs in the file's native CRS. The caller reprojects the query geometry (and
 * any distance) into that CRS before building the filter; the reader does no reprojection. Output reprojection and
 * decimation, when needed, are the caller's concern downstream of the read.
 *
 * @param <T> the domain geometry type produced by {@link #decode(MemorySegment)}
 */
public interface GeometryFilter<T> {

    /** The geometry column this filter reads. */
    ColumnPath column();

    /**
     * A bbox relation that is a NECESSARY CONDITION of {@link #matches}: {@code matches(g)} being {@code true} implies
     * this relation holds for {@code g}'s bbox. The reader prunes with it. Return empty when no sound relation exists
     * (the read then scans every row without bbox pruning).
     */
    Optional<Predicate.Spatial> pruningPredicate();

    /** Builds the domain geometry from its WKB bytes. Called once per row that reaches the gate. */
    T decode(MemorySegment wkb);

    /** The exact test on the domain geometry, in the file CRS. */
    boolean matches(T geometry);

    /**
     * Decode-then-test in one call: returns the decoded geometry when {@link #matches} accepts it, empty otherwise. An
     * empty result drops the row, and under late materialization its other columns are never decoded. The returned
     * geometry is used only for the per-row test today; the read path does not yet reuse it as the column's output
     * value.
     *
     * <p>The default implementation calls {@link #decode} and then {@link #matches}.
     */
    default Optional<Object> gate(MemorySegment wkb) {
        T geometry = decode(wkb);
        if (matches(geometry)) {
            return Optional.of(geometry);
        }
        return Optional.empty();
    }
}
