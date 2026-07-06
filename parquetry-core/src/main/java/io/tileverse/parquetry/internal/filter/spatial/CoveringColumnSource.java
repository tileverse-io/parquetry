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
package io.tileverse.parquetry.internal.filter.spatial;

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.Statistics;
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
 * covering metadata declares four (or six) sibling numeric leaves whose per-row-group {@link Statistics} carry the
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
     * (numeric sidecar leaves whose schema is resolvable and whose row groups carry at least one usable stats entry).
     * Otherwise empty so the dispatch falls through.
     */
    static Optional<SpatialBoundsSource> tryBuild(FileMetaData footer, ParquetSchema schema, GeoParquetMetadata geo) {
        Map<ColumnPath, BboxAxes> axesByGeometry = resolveCoverings(schema, geo);
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

    /**
     * Builds the {@link BboxAxes} entry for each geometry column whose {@link Covering} resolves cleanly against
     * {@code schema}. Columns without a covering, or whose sidecar paths don't resolve to primitive numeric leaves, are
     * skipped.
     */
    private static Map<ColumnPath, BboxAxes> resolveCoverings(ParquetSchema schema, GeoParquetMetadata geo) {
        Map<ColumnPath, BboxAxes> result = new LinkedHashMap<>();
        geo.columns().forEach((columnName, geoColumn) -> resolveOne(schema, columnName, geoColumn, result));
        return result;
    }

    private static void resolveOne(
            ParquetSchema schema, String columnName, GeoColumn geoColumn, Map<ColumnPath, BboxAxes> sink) {
        Optional<BboxCovering> covering = geoColumn.covering().map(Covering::bbox);
        if (covering.isEmpty()) {
            return;
        }
        BboxCovering b = covering.orElseThrow();
        Optional<PrimitiveKind> xminKind = primitiveKindAt(schema, b.xmin());
        Optional<PrimitiveKind> xmaxKind = primitiveKindAt(schema, b.xmax());
        Optional<PrimitiveKind> yminKind = primitiveKindAt(schema, b.ymin());
        Optional<PrimitiveKind> ymaxKind = primitiveKindAt(schema, b.ymax());
        if (xminKind.isEmpty() || xmaxKind.isEmpty() || yminKind.isEmpty() || ymaxKind.isEmpty()) {
            return;
        }
        sink.put(
                ColumnPath.of(columnName),
                new BboxAxes(
                        new AxisRef(b.xmin(), xminKind.orElseThrow()),
                        new AxisRef(b.xmax(), xmaxKind.orElseThrow()),
                        new AxisRef(b.ymin(), yminKind.orElseThrow()),
                        new AxisRef(b.ymax(), ymaxKind.orElseThrow())));
    }

    private static Optional<PrimitiveKind> primitiveKindAt(ParquetSchema schema, ColumnPath path) {
        return schema.find(path)
                .flatMap(f -> f instanceof SchemaNode.Primitive p ? Optional.of(p.kind()) : Optional.empty());
    }

    /**
     * For each geometry column with a resolvable covering, computes the per-row-group bbox by reading the four sidecar
     * columns' {@link Statistics}. A row group that misses any of the four bounds surfaces as {@link Optional#empty()}
     * for that slot.
     */
    private static Map<ColumnPath, List<Optional<BoundingBox>>> buildPerRowGroupBoxes(
            FileMetaData footer, Map<ColumnPath, BboxAxes> axesByGeometry) {
        Map<ColumnPath, List<Optional<BoundingBox>>> result = new LinkedHashMap<>();
        List<RowGroup> rowGroups = footer.rowGroups();
        axesByGeometry.forEach((geometryColumn, axes) -> {
            List<Optional<BoundingBox>> perGroup = new ArrayList<>(rowGroups.size());
            for (RowGroup rg : rowGroups) {
                perGroup.add(buildBboxForRowGroup(rg, axes));
            }
            result.put(geometryColumn, perGroup);
        });
        return result;
    }

    private static Optional<BoundingBox> buildBboxForRowGroup(RowGroup rg, BboxAxes axes) {
        Map<ColumnPath, ColumnMetaData> byPath = indexColumnsInRowGroup(rg);
        OptionalDouble xmin = statsMin(byPath, axes.xmin());
        OptionalDouble xmax = statsMax(byPath, axes.xmax());
        OptionalDouble ymin = statsMin(byPath, axes.ymin());
        OptionalDouble ymax = statsMax(byPath, axes.ymax());
        if (xmin.isEmpty() || xmax.isEmpty() || ymin.isEmpty() || ymax.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new BoundingBox(
                xmin.getAsDouble(),
                xmax.getAsDouble(),
                ymin.getAsDouble(),
                ymax.getAsDouble(),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty()));
    }

    private static Map<ColumnPath, ColumnMetaData> indexColumnsInRowGroup(RowGroup rg) {
        Map<ColumnPath, ColumnMetaData> byPath = new LinkedHashMap<>();
        for (ColumnChunk chunk : rg.columns()) {
            chunk.metaData().ifPresent(m -> byPath.put(ColumnPath.of(m.pathInSchema()), m));
        }
        return byPath;
    }

    private static OptionalDouble statsMin(Map<ColumnPath, ColumnMetaData> byPath, AxisRef axis) {
        ColumnMetaData m = byPath.get(axis.path());
        if (m == null) {
            return OptionalDouble.empty();
        }
        Optional<Statistics> stats = m.statistics();
        if (stats.isEmpty()) {
            return OptionalDouble.empty();
        }
        MemorySegment chosen = preferLatest(stats.get().minValue(), stats.get().min());
        if (chosen == MemorySegment.NULL) {
            return OptionalDouble.empty();
        }
        return decodeDouble(axis.kind(), chosen);
    }

    private static OptionalDouble statsMax(Map<ColumnPath, ColumnMetaData> byPath, AxisRef axis) {
        ColumnMetaData m = byPath.get(axis.path());
        if (m == null) {
            return OptionalDouble.empty();
        }
        Optional<Statistics> stats = m.statistics();
        if (stats.isEmpty()) {
            return OptionalDouble.empty();
        }
        MemorySegment chosen = preferLatest(stats.get().maxValue(), stats.get().max());
        if (chosen == MemorySegment.NULL) {
            return OptionalDouble.empty();
        }
        return decodeDouble(axis.kind(), chosen);
    }

    private static MemorySegment preferLatest(MemorySegment latest, MemorySegment legacy) {
        return latest != MemorySegment.NULL ? latest : legacy;
    }

    /**
     * Decodes a Parquet PLAIN-encoded min/max value as a {@code double}. The Parquet spec writes FLOAT / DOUBLE
     * little-endian; other kinds (and short / malformed payloads) surface as empty so the row group's bbox stays
     * unknown rather than wrong.
     */
    private static OptionalDouble decodeDouble(PrimitiveKind kind, MemorySegment raw) {
        long size = raw.byteSize();
        return switch (kind) {
            case DOUBLE -> size >= 8 ? OptionalDouble.of(raw.get(DOUBLE, 0)) : OptionalDouble.empty();
            case FLOAT -> size >= 4 ? OptionalDouble.of(raw.get(FLOAT, 0)) : OptionalDouble.empty();
            default -> OptionalDouble.empty();
        };
    }

    /** See {@link NativeStatsSource} for the same union policy and rationale. */
    private static Map<ColumnPath, BoundingBox> unionAcrossRowGroups(
            Map<ColumnPath, List<Optional<BoundingBox>>> perRowGroup) {
        Map<ColumnPath, BoundingBox> result = new LinkedHashMap<>();
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
        return new BoundingBox(
                Math.min(a.xmin(), b.xmin()),
                Math.max(a.xmax(), b.xmax()),
                Math.min(a.ymin(), b.ymin()),
                Math.max(a.ymax(), b.ymax()),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty());
    }

    /**
     * Cached lookup carrier for one bbox sidecar column: its path and the primitive kind needed to decode its stats.
     */
    private record AxisRef(ColumnPath path, PrimitiveKind kind) {}

    /**
     * The four axes of one geometry column's covering. Z is intentionally out of scope for the current covering tier.
     */
    private record BboxAxes(AxisRef xmin, AxisRef xmax, AxisRef ymin, AxisRef ymax) {}
}
