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
package io.tileverse.parquetry.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.spatial.CoveringPageBounds;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.read.RowGroupGate;
import io.tileverse.parquetry.internal.read.RowGroupPlanner;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.SpatialDecimationGate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.geoparquet.GeometryColumns;

/**
 * Builds the spatial decimation gates for one file. A read whose {@link ReadOptions} supplies a
 * {@link SpatialReadProbe} consults the gates to drop already-covered units by bounding box: the row-group gate skips a
 * covered row group before any fetch, and the leaf gate narrows a decoded batch's surviving rows. When the file exposes
 * no geometry column, or a read supplies no probe, the gates are empty and the read is unchanged. Constructed once per
 * file.
 *
 * <p>The two gates of one read share the read's single probe instance, hence one accumulating coverage state spans the
 * row-group and leaf levels. The coarse row-group gate consults the read-only {@link SpatialReadProbe#probeRegion},
 * which records no coverage and may not stand in for a row group with a substitute; the leaf level paints, and the
 * planner's accepted-region consultation may let the probe paint through a substitute of its own.
 *
 * <p>The same probe also drives {@link #rowGroupPlanner}, which plans a row group down to the pages worth reading
 * before its fetch plan is built.
 */
final class SpatialReadGates {

    private final Optional<ColumnPath> primaryGeometry;
    private final SpatialBoundsSource boundsSource;
    private final ParquetSchema fileSchema;
    private final Optional<GeoParquetMetadata> geoMetadata;

    SpatialReadGates(
            SpatialBoundsSource boundsSource, ParquetSchema fileSchema, Optional<GeoParquetMetadata> geoMetadata) {
        this.primaryGeometry = GeometryColumns.primary(fileSchema, geoMetadata);
        this.boundsSource = boundsSource;
        this.fileSchema = fileSchema;
        this.geoMetadata = geoMetadata;
    }

    /**
     * The leaf (per-row) gate for this read, present only when {@code options} supplies a probe and the file has a
     * primary geometry column.
     */
    Optional<SpatialDecimationGate> leafGate(ReadOptions options) {
        return probeFor(options).map(probe -> new SpatialDecimationGate(primaryGeometry.orElseThrow(), probe));
    }

    /**
     * The row-group gate for this read, present only when {@code options} supplies a probe and the file has a primary
     * geometry column. The gate drops a survivor whose geometry bounds the probe reports as already covered through the
     * read-only {@link SpatialReadProbe#probeRegion}; a survivor without recorded bounds, or with bounds wrapping the
     * antimeridian, is never dropped. A substitute answered to that consultation is rejected with an
     * {@link UnsupportedOperationException}.
     */
    Optional<RowGroupGate> rowGroupGate(List<RowGroupSurvivor> survivors, ReadOptions options) {
        return probeFor(options).map(probe -> {
            List<Optional<Bbox>> perSurvivorBounds = perSurvivorBounds(survivors);
            return position -> probeSkips(probe, perSurvivorBounds.get(position));
        });
    }

    /**
     * The per-row-group planner for this read, present only when {@code options} supplies a probe, the file has a
     * primary geometry column, and {@code outputSchema} holds that column. It receives the normalized predicate
     * supplied by the caller, before any covering rewrite.
     *
     * <p>The geometry must be in the output for the plan to be sound. A unit is dropped as covered only on the strength
     * of paint recorded by the per-row gate or by the probe's own substitutes, and {@link SpatialDecimationGate} reads
     * the geometry off a batch shaped by {@code outputSchema}: absent the column, no row ever paints, hence a plan
     * would be judging units against paint that never accumulates.
     *
     * @param outputSchema the physical schema of the batches seen by the per-row gate
     */
    Optional<RowGroupPlanner> rowGroupPlanner(
            List<RowGroupSurvivor> survivors,
            Predicate normalizedPredicate,
            ReadOptions options,
            ParquetSchema outputSchema) {
        Optional<SpatialReadProbe> probe = probeFor(options);
        if (probe.isEmpty() || !outputHoldsGeometry(outputSchema)) {
            return Optional.empty();
        }
        ColumnPath geometry = primaryGeometry.orElseThrow();
        Optional<CoveringPageBounds> pageBounds = CoveringPageBounds.resolve(geometry, fileSchema, geoMetadata);
        RowGroupPlanner planner = new SpatialRowGroupPlanner(
                survivors, probe.orElseThrow(), normalizedPredicate, geometry, boundsSource, pageBounds);
        return Optional.of(planner);
    }

    /** Whether a batch shaped by {@code outputSchema} exposes the primary geometry column to the per-row gate. */
    private boolean outputHoldsGeometry(ParquetSchema outputSchema) {
        return outputSchema.leafColumns().contains(primaryGeometry.orElseThrow());
    }

    /** The read's probe, present only when the read supplies one and the file has a primary geometry column. */
    private Optional<SpatialReadProbe> probeFor(ReadOptions options) {
        if (primaryGeometry.isEmpty()) {
            return Optional.empty();
        }
        return options.spatialReadProbe();
    }

    private List<Optional<Bbox>> perSurvivorBounds(List<RowGroupSurvivor> survivors) {
        ColumnPath geometry = primaryGeometry.orElseThrow();
        List<Optional<Bbox>> bounds = new ArrayList<>(survivors.size());
        for (RowGroupSurvivor survivor : survivors) {
            Optional<BoundingBox> box = boundsSource.rowGroupBounds(geometry, survivor.index());
            Optional<BoundingBox> rectangle = box.filter(SpatialReadGates::isPlanarRectangle);
            bounds.add(rectangle.map(SpatialReadGates::toBbox));
        }
        return bounds;
    }

    /**
     * Whether {@code box} bounds a single planar rectangle: each minimum at or below its maximum. A box wrapping the
     * antimeridian fails, as does a NaN bound; offered to the probe, an inverted x interval would read as the gap
     * between the two longitude ranges of the box instead of the ranges themselves.
     */
    private static boolean isPlanarRectangle(BoundingBox box) {
        return box.xmin() <= box.xmax() && box.ymin() <= box.ymax();
    }

    /**
     * Whether the read-only consultation drops a row group of unknown content: it reports the row group's whole box
     * already covered, and nothing more. A substitute here would stand for rows never proven to satisfy the query,
     * hence it is rejected.
     */
    private static boolean probeSkips(SpatialReadProbe probe, Optional<Bbox> bounds) {
        if (bounds.isEmpty()) {
            return false;
        }
        Bbox box = bounds.orElseThrow();
        Decision decision = probe.probeRegion(box.minX(), box.minY(), box.maxX(), box.maxY());
        return switch (decision) {
            case Decision.Skip _ -> true;
            case Decision.Keep _, Decision.Descend _ -> false;
            case Decision.Substitute _ ->
                throw new UnsupportedOperationException("a row group of unknown content cannot be substituted: "
                        + "Substitute answers the accepted-region consultation only");
        };
    }

    static Bbox toBbox(BoundingBox box) {
        return Bbox.of2d(box.xmin(), box.ymin(), box.xmax(), box.ymax());
    }
}
