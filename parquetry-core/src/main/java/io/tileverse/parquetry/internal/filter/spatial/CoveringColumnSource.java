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

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.footer.CompactFooter;
import io.tileverse.parquetry.internal.footer.LeafIndex;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.BboxCovering;
import io.tileverse.parquetry.schema.geo.geoparquet.Covering;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * {@link SpatialBoundsSource} backed by GeoParquet 1.1 {@code covering.bbox} sidecar columns. Each geometry column with
 * covering metadata declares four (or six) sibling numeric leaves whose per-row-group {@link Statistics} give the
 * row-group bbox: the {@code xmin} column's stats.min is the row group's xmin, the {@code xmax} column's stats.max is
 * the row group's xmax, and so on.
 *
 * <p>Construction precomputes per-row-group bboxes and a file-level union per geometry column. A geometry column whose
 * sidecar leaves cannot be resolved (path missing, non-numeric leaf, no stats on any row group) is dropped silently so
 * the dispatch falls through to the next tier.
 */
final class CoveringColumnSource implements SpatialBoundsSource {

    private final Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup;
    private final Map<ColumnPath, BoundingBox> fileLevel;

    private CoveringColumnSource(
            Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup, Map<ColumnPath, BoundingBox> fileLevel) {
        this.perRowGroup = perRowGroup;
        this.fileLevel = fileLevel;
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
        Map<ColumnPath, BoundingBox> fileLevel = unionAcrossRowGroups(perRowGroup);
        return Optional.of(new CoveringColumnSource(Map.copyOf(perRowGroup), Map.copyOf(fileLevel)));
    }

    @Override
    public Optional<BoundingBox> fileBounds(ColumnPath geometryColumn) {
        return Optional.ofNullable(fileLevel.get(geometryColumn));
    }

    @Override
    public Optional<BoundingBox> rowGroupBounds(ColumnPath geometryColumn, int rowGroupIndex) {
        List<Optional<BoundingBox>> byGroup = perRowGroup.get(geometryColumn);
        if (byGroup == null || rowGroupIndex < 0 || rowGroupIndex >= byGroup.size()) {
            return Optional.empty();
        }
        return byGroup.get(rowGroupIndex);
    }

    /** One box per geometry column and row group with usable sidecar stats, plus that column's file-level union. */
    @Override
    public int retainedBoxCount() {
        return fileLevel.size() + PerRowGroupBoxes.presentBoxes(perRowGroup);
    }

    /**
     * Builds the {@link BboxAxes} entry for each geometry column whose {@link Covering} resolves cleanly against
     * {@code schema}. Columns without a covering, or whose sidecar paths don't resolve to primitive numeric leaves, are
     * skipped.
     */
    private static Map<ColumnPath, BboxAxes> resolveCoverings(
            LeafIndex leaves, ParquetSchema schema, GeoParquetMetadata geo) {
        Map<ColumnPath, BboxAxes> result = HashMap.newHashMap(geo.columns().size());
        geo.columns().forEach((columnName, geoColumn) -> resolveOne(leaves, schema, columnName, geoColumn, result));
        return result;
    }

    private static void resolveOne(
            LeafIndex leaves,
            ParquetSchema schema,
            String columnName,
            GeoColumn geoColumn,
            Map<ColumnPath, BboxAxes> sink) {
        Optional<BboxCovering> covering = geoColumn.covering().map(Covering::bbox);
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
                ColumnPath.of(columnName),
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
     * columns' statistics. A row group that misses any of the four bounds gets {@link Optional#empty()} for that slot.
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
        OptionalDouble xmin = statsMin(footer, rowGroup, axes.xmin());
        OptionalDouble xmax = statsMax(footer, rowGroup, axes.xmax());
        OptionalDouble ymin = statsMin(footer, rowGroup, axes.ymin());
        OptionalDouble ymax = statsMax(footer, rowGroup, axes.ymax());
        if (xmin.isEmpty() || xmax.isEmpty() || ymin.isEmpty() || ymax.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(BoundingBox.builder()
                .xmin(xmin.getAsDouble())
                .xmax(xmax.getAsDouble())
                .ymin(ymin.getAsDouble())
                .ymax(ymax.getAsDouble())
                .build());
    }

    private static OptionalDouble statsMin(CompactFooter footer, int rowGroup, AxisRef axis) {
        Optional<MemorySegment> min = chunkAt(footer, rowGroup, axis).flatMap(ChunkMeta::minValue);
        return decodeDouble(axis.kind(), min);
    }

    private static OptionalDouble statsMax(CompactFooter footer, int rowGroup, AxisRef axis) {
        Optional<MemorySegment> max = chunkAt(footer, rowGroup, axis).flatMap(ChunkMeta::maxValue);
        return decodeDouble(axis.kind(), max);
    }

    private static Optional<ChunkMeta> chunkAt(CompactFooter footer, int rowGroup, AxisRef axis) {
        return footer.chunkIfPresent(rowGroup, axis.leaf());
    }

    /**
     * Decodes a Parquet PLAIN-encoded min/max value as a {@code double}. The Parquet spec writes FLOAT / DOUBLE
     * little-endian; other kinds (and short / malformed payloads) decode as empty, which leaves the row group's bbox
     * unknown rather than wrong.
     */
    private static OptionalDouble decodeDouble(PrimitiveKind kind, Optional<MemorySegment> raw) {
        if (raw.isEmpty()) {
            return OptionalDouble.empty();
        }
        MemorySegment value = raw.orElseThrow();
        long size = value.byteSize();
        return switch (kind) {
            case DOUBLE -> size >= 8 ? OptionalDouble.of(value.get(DOUBLE, 0)) : OptionalDouble.empty();
            case FLOAT -> size >= 4 ? OptionalDouble.of(value.get(FLOAT, 0)) : OptionalDouble.empty();
            default -> OptionalDouble.empty();
        };
    }

    /** See {@link NativeStatsSource} for the same union policy and rationale. */
    private static Map<ColumnPath, BoundingBox> unionAcrossRowGroups(
            Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup) {
        Map<ColumnPath, BoundingBox> result = HashMap.newHashMap(perRowGroup.size());
        perRowGroup.forEach((path, list) -> {
            BoundingBox acc = null;
            for (Optional<BoundingBox> slot : list) {
                if (slot.isPresent()) {
                    acc = acc == null ? slot.orElseThrow() : union(acc, slot.orElseThrow());
                }
            }
            if (acc != null) {
                result.put(path, acc);
            }
        });
        return result;
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
