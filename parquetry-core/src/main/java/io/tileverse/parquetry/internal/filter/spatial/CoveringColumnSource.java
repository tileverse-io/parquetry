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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.filter.ColumnSummary;
import io.tileverse.parquetry.internal.filter.FilterPipeline;
import io.tileverse.parquetry.internal.filter.NaNCells;
import io.tileverse.parquetry.internal.filter.StatsEvaluator;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.footer.CompactFooter;
import io.tileverse.parquetry.internal.footer.LeafIndex;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.BboxCovering;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * {@link SpatialBoundsSource} backed by the bbox covering of each geometry column ({@link BboxCoverings}): the numeric
 * leaves declared by a GeoParquet 1.1 {@code covering.bbox}, or the conventional {@code bbox} struct of GeoParquet 1.0
 * producers. The per-row-group {@link Statistics} of the leaves give the row-group bbox: the {@code xmin} column's
 * stats.min is the row group's xmin, the {@code xmax} column's stats.max is the row group's xmax, and so on.
 *
 * <p>Construction precomputes per-row-group bboxes and a file-level union per geometry column, known only when each row
 * group has a box. A geometry column with unresolvable sidecar leaves (path missing, non-numeric leaf, no stats on any
 * row group) is dropped silently, and the dispatch falls through to the next tier.
 */
final class CoveringColumnSource implements SpatialBoundsSource {

    private final PerRowGroupBoxes boxes;

    private CoveringColumnSource(PerRowGroupBoxes boxes) {
        this.boxes = boxes;
    }

    /**
     * Returns a {@code CoveringColumnSource} when at least one geometry column in {@code geo} has a usable covering
     * (numeric sidecar leaves whose schema is resolvable and whose row groups record at least one usable stats entry).
     * Otherwise empty, which sends the dispatch on to the next tier.
     */
    static Optional<SpatialBoundsSource> tryBuild(
            CompactFooter footer, LeafIndex leaves, ParquetSchema schema, GeoParquetMetadata geo) {
        Map<ColumnPath, BboxAxes> axesByGeometry = resolveCoverings(leaves, schema, geo);
        if (axesByGeometry.isEmpty()) {
            return Optional.empty();
        }
        Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup = buildPerRowGroupBoxes(footer, axesByGeometry);
        // Empty if every row group's bbox failed to materialize - degrade to the next tier so the consumer still
        // gets something useful.
        perRowGroup.entrySet().removeIf(e -> e.getValue().stream().allMatch(Optional::isEmpty));
        if (perRowGroup.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new CoveringColumnSource(new PerRowGroupBoxes(perRowGroup, CoveringColumnSource::union)));
    }

    @Override
    public Optional<BoundingBox> fileBounds(ColumnPath geometryColumn) {
        return boxes.fileBounds(geometryColumn);
    }

    @Override
    public Optional<BoundingBox> rowGroupBounds(ColumnPath geometryColumn, int rowGroupIndex) {
        return boxes.rowGroupBounds(geometryColumn, rowGroupIndex);
    }

    /** One box per geometry column and row group with usable sidecar stats, plus the known file bounds per column. */
    @Override
    public int retainedBoxCount() {
        return boxes.retainedBoxCount();
    }

    /**
     * Builds the {@link BboxAxes} entry for each geometry column with a bbox covering ({@link BboxCoverings}). Columns
     * without a covering, or whose sidecar paths don't resolve to primitive numeric leaves, are skipped.
     */
    private static Map<ColumnPath, BboxAxes> resolveCoverings(
            LeafIndex leaves, ParquetSchema schema, GeoParquetMetadata geo) {
        Map<ColumnPath, BboxAxes> result = HashMap.newHashMap(geo.columns().size());
        for (String columnName : geo.columns().keySet()) {
            resolveOne(leaves, schema, geo, ColumnPath.of(columnName), result);
        }
        return result;
    }

    private static void resolveOne(
            LeafIndex leaves,
            ParquetSchema schema,
            GeoParquetMetadata geo,
            ColumnPath geometryColumn,
            Map<ColumnPath, BboxAxes> sink) {
        Optional<BboxCovering> covering = BboxCoverings.coveringOf(geometryColumn, schema, geo);
        if (covering.isEmpty()) {
            return;
        }
        BboxCovering b = covering.orElseThrow();
        Optional<AxisRef> xmin = axisRef(leaves, schema, b.xmin());
        Optional<AxisRef> xmax = axisRef(leaves, schema, b.xmax());
        Optional<AxisRef> ymin = axisRef(leaves, schema, b.ymin());
        Optional<AxisRef> ymax = axisRef(leaves, schema, b.ymax());
        if (xmin.isEmpty() || xmax.isEmpty() || ymin.isEmpty() || ymax.isEmpty()) {
            return;
        }
        sink.put(
                geometryColumn,
                new BboxAxes(xmin.orElseThrow(), xmax.orElseThrow(), ymin.orElseThrow(), ymax.orElseThrow()));
    }

    /**
     * Resolves one sidecar path to the leaf ordinal addressing its chunks plus the primitive kind needed to decode its
     * stats. Empty when the schema declares no such primitive leaf.
     */
    private static Optional<AxisRef> axisRef(LeafIndex leaves, ParquetSchema schema, ColumnPath path) {
        int ordinal = leaves.ordinalOf(path);
        if (ordinal == LeafIndex.UNKNOWN) {
            return Optional.empty();
        }
        return primitiveKindAt(schema, path).map(kind -> new AxisRef(ordinal, kind));
    }

    private static Optional<PrimitiveKind> primitiveKindAt(ParquetSchema schema, ColumnPath path) {
        return schema.find(path)
                .flatMap(f -> f instanceof SchemaNode.Primitive p ? Optional.of(p.kind()) : Optional.empty());
    }

    /**
     * For each geometry column with a resolvable covering, computes the per-row-group bbox from the four sidecar
     * columns' statistics. A row group missing any of the four bounds, or with bounds proving nothing about its
     * geometries, gets {@link Optional#empty()} for that slot.
     */
    private static Map<ColumnPath, List<Optional<BoundingBox>>> buildPerRowGroupBoxes(
            CompactFooter footer, Map<ColumnPath, BboxAxes> axesByGeometry) {
        Map<ColumnPath, List<Optional<BoundingBox>>> result = HashMap.newHashMap(axesByGeometry.size());
        int rowGroupCount = footer.rowGroupCount();
        axesByGeometry.forEach((geometryColumn, axes) -> {
            List<Optional<BoundingBox>> perGroup = new ArrayList<>(rowGroupCount);
            for (int rowGroup = 0; rowGroup < rowGroupCount; rowGroup++) {
                perGroup.add(buildBboxForRowGroup(footer, rowGroup, axes));
            }
            result.put(geometryColumn, perGroup);
        });
        return result;
    }

    private static Optional<BoundingBox> buildBboxForRowGroup(CompactFooter footer, int rowGroup, BboxAxes axes) {
        OptionalDouble xmin = lowerBound(footer, rowGroup, axes.xmin());
        OptionalDouble xmax = upperBound(footer, rowGroup, axes.xmax());
        OptionalDouble ymin = lowerBound(footer, rowGroup, axes.ymin());
        OptionalDouble ymax = upperBound(footer, rowGroup, axes.ymax());
        if (xmin.isEmpty() || xmax.isEmpty() || ymin.isEmpty() || ymax.isEmpty()) {
            return Optional.empty();
        }
        BoundingBox box = BoundingBox.builder()
                .xmin(xmin.getAsDouble())
                .xmax(xmax.getAsDouble())
                .ymin(ymin.getAsDouble())
                .ymax(ymax.getAsDouble())
                .build();
        if (box.provesNothing()) {
            return Optional.empty();
        }
        return Optional.of(box);
    }

    private static OptionalDouble lowerBound(CompactFooter footer, int rowGroup, AxisRef axis) {
        Optional<ColumnSummary> summary = summaryOf(footer, rowGroup, axis);
        return asDouble(summary.flatMap(ColumnSummary::min));
    }

    private static OptionalDouble upperBound(CompactFooter footer, int rowGroup, AxisRef axis) {
        Optional<ColumnSummary> summary = summaryOf(footer, rowGroup, axis);
        return asDouble(summary.flatMap(ColumnSummary::max));
    }

    /**
     * The statistics of the axis chunk of {@code rowGroup}, read by the rules of the statistics tier: a NaN bound is
     * left out, and a minimum above its maximum leaves both out. Both tiers then agree on the bounds of a covering
     * column. Empty when the row group has no such chunk, or when its bounds follow an order not applied by this
     * reader.
     */
    private static Optional<ColumnSummary> summaryOf(CompactFooter footer, int rowGroup, AxisRef axis) {
        Optional<ChunkMeta> chunk = footer.chunkIfPresent(rowGroup, axis.leaf()).filter(ChunkMeta::boundsOrdered);
        return chunk.map(meta -> StatsEvaluator.summarize(statisticsOf(meta, axis.kind())));
    }

    private static FilterPipeline.ColumnStats statisticsOf(ChunkMeta chunk, PrimitiveKind kind) {
        return new FilterPipeline.ColumnStats(
                kind,
                chunk.minValue(),
                chunk.maxValue(),
                OptionalLong.empty(),
                chunk.numValues(),
                Optional.empty(),
                NaNCells.POSSIBLE);
    }

    /** A FLOAT or DOUBLE bound as a {@code double}; a bound of any other kind leaves the box unknown. */
    private static OptionalDouble asDouble(Optional<Value> bound) {
        if (bound.isEmpty()) {
            return OptionalDouble.empty();
        }
        return switch (bound.orElseThrow()) {
            case Value.DoubleVal(double value) -> OptionalDouble.of(value);
            case Value.FloatVal(float value) -> OptionalDouble.of(value);
            default -> OptionalDouble.empty();
        };
    }

    private static BoundingBox union(BoundingBox a, BoundingBox b) {
        return BoundingBox.builder()
                .xmin(Math.min(a.xmin(), b.xmin()))
                .xmax(Math.max(a.xmax(), b.xmax()))
                .ymin(Math.min(a.ymin(), b.ymin()))
                .ymax(Math.max(a.ymax(), b.ymax()))
                .build();
    }

    /**
     * Where one bbox sidecar column's stats are read from: the leaf ordinal addressing its chunks, and the primitive
     * kind used to decode its min/max bytes.
     */
    private record AxisRef(int leaf, PrimitiveKind kind) {}

    /**
     * The four axes of one geometry column's covering. Z is intentionally out of scope for the current covering tier.
     */
    private record BboxAxes(AxisRef xmin, AxisRef xmax, AxisRef ymin, AxisRef ymax) {}
}
