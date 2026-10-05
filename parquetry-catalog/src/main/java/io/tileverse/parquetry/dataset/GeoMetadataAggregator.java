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
package io.tileverse.parquetry.dataset;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Unions the per-file GeoParquet metadata of one dataset's files into a single aggregated view. The input holds one
 * entry per file of the dataset, empty for a file without readable geo metadata.
 *
 * <p>The column SET is the union of every file's columns, in first-seen order. For each column, {@code edges},
 * {@code orientation}, {@code epoch}, and {@code covering} are taken from the first file that contains that column.
 * Only {@code bbox} and {@code geometry_types} are unioned across files. The primary column name, the per-column CRS,
 * and the per-column encoding are validated for consistency across the files that contain the column; a mismatch
 * throws, because a dataset whose files disagree on CRS or encoding is malformed (for GeoParquet 1.x the CRS lives only
 * in the geo metadata, and an unvalidated mismatch would silently merge files in different CRSes). The aggregated bbox
 * is 2D: Z and M extents are intentionally dropped on union.
 *
 * <p>A column's aggregated bbox is known only when each file of the dataset declares one. A file without geo metadata,
 * without the column, or without its bbox could hold geometries anywhere, and leaves the aggregated bbox empty.
 *
 * <p>The aggregate preserves the input's GeoParquet spec version subtype (a {@code V2} input yields a {@code V2}
 * aggregate), rather than forcing every result into one version.
 */
public final class GeoMetadataAggregator {

    private GeoMetadataAggregator() {}

    public static Optional<GeoParquetMetadata> aggregate(List<Optional<GeoParquetMetadata>> perFile) {
        List<GeoParquetMetadata> declaring = declaringFiles(perFile);
        if (declaring.isEmpty()) {
            return Optional.empty();
        }
        GeoParquetMetadata first = declaring.get(0);
        String primary = requireConsistentPrimaryColumn(declaring, first.primaryColumn());
        Map<String, GeoColumn> mergedColumns = new LinkedHashMap<>();
        for (String columnName : columnNamesAcrossFiles(declaring)) {
            mergedColumns.put(columnName, mergeColumn(columnName, declaring, perFile));
        }
        return Optional.of(rebuildPreservingVersion(first, primary, mergedColumns));
    }

    private static List<GeoParquetMetadata> declaringFiles(List<Optional<GeoParquetMetadata>> perFile) {
        List<GeoParquetMetadata> declaring = new ArrayList<>(perFile.size());
        for (Optional<GeoParquetMetadata> file : perFile) {
            file.ifPresent(declaring::add);
        }
        return declaring;
    }

    private static Set<String> columnNamesAcrossFiles(List<GeoParquetMetadata> declaring) {
        Set<String> names = new LinkedHashSet<>();
        for (GeoParquetMetadata file : declaring) {
            names.addAll(file.columns().keySet());
        }
        return names;
    }

    private static GeoParquetMetadata rebuildPreservingVersion(
            GeoParquetMetadata first, String primary, Map<String, GeoColumn> mergedColumns) {
        return switch (first) {
            case GeoParquetMetadata.V1_0 v -> new GeoParquetMetadata.V1_0(v.version(), primary, mergedColumns);
            case GeoParquetMetadata.V1_1 v -> new GeoParquetMetadata.V1_1(v.version(), primary, mergedColumns);
            case GeoParquetMetadata.V2 v -> new GeoParquetMetadata.V2(v.version(), primary, mergedColumns);
        };
    }

    private static String requireConsistentPrimaryColumn(List<GeoParquetMetadata> declaring, String primary) {
        for (GeoParquetMetadata file : declaring) {
            if (!primary.equals(file.primaryColumn())) {
                throw new IllegalStateException("files disagree on the GeoParquet primary column: '" + primary
                        + "' vs '" + file.primaryColumn() + "'");
            }
        }
        return primary;
    }

    private static void requireConsistentCrsAndEncoding(String columnName, GeoColumn template, GeoColumn column) {
        if (!column.crs().equals(template.crs())) {
            throw new IllegalStateException("files disagree on the crs of geo column '" + columnName + "'");
        }
        if (!column.encoding().equals(template.encoding())) {
            throw new IllegalStateException("files disagree on the encoding of geo column '" + columnName + "'");
        }
    }

    private static GeoColumn mergeColumn(
            String columnName, List<GeoParquetMetadata> declaring, List<Optional<GeoParquetMetadata>> perFile) {
        GeoColumn template = firstColumnNamed(columnName, declaring);
        Set<String> geometryTypes = new LinkedHashSet<>();
        for (GeoParquetMetadata file : declaring) {
            GeoColumn column = file.columns().get(columnName);
            if (column == null) {
                continue;
            }
            requireConsistentCrsAndEncoding(columnName, template, column);
            geometryTypes.addAll(column.geometryTypes());
        }
        return GeoColumn.builder()
                .encoding(template.encoding())
                .geometryTypes(new ArrayList<>(geometryTypes))
                .crs(template.crs())
                .edges(template.edges())
                .orientation(template.orientation())
                .bbox(bboxDeclaredByEachFile(columnName, perFile))
                .epoch(template.epoch())
                .covering(template.covering())
                .build();
    }

    private static GeoColumn firstColumnNamed(String columnName, List<GeoParquetMetadata> declaring) {
        for (GeoParquetMetadata file : declaring) {
            GeoColumn column = file.columns().get(columnName);
            if (column != null) {
                return column;
            }
        }
        throw new IllegalStateException("no file contains geo column '" + columnName + "'");
    }

    /**
     * The union of the bbox declared for {@code columnName} by each file, or empty as soon as one file declares none. A
     * declared bbox without an extent ({@link BoundingBox#hasExtent()}) adds nothing to the union.
     */
    private static Optional<BoundingBox> bboxDeclaredByEachFile(
            String columnName, List<Optional<GeoParquetMetadata>> perFile) {
        Optional<BoundingBox> union = Optional.empty();
        for (Optional<GeoParquetMetadata> file : perFile) {
            Optional<BoundingBox> declared = declaredBbox(file, columnName);
            if (declared.isEmpty()) {
                return Optional.empty();
            }
            union = unionBbox(union, declared.filter(BoundingBox::hasExtent));
        }
        return union;
    }

    private static Optional<BoundingBox> declaredBbox(Optional<GeoParquetMetadata> file, String columnName) {
        Optional<GeoColumn> column = file.map(geo -> geo.columns().get(columnName));
        return column.flatMap(GeoColumn::bbox);
    }

    private static Optional<BoundingBox> unionBbox(Optional<BoundingBox> acc, Optional<BoundingBox> next) {
        if (next.isEmpty()) {
            return acc;
        }
        if (acc.isEmpty()) {
            return next;
        }
        BoundingBox a = acc.get();
        BoundingBox b = next.get();
        if (a.wrapsAntimeridian() || b.wrapsAntimeridian()) {
            return Optional.of(fullLongitudeSuperset(a, b));
        }
        return Optional.of(new BoundingBox(
                Math.min(a.xmin(), b.xmin()),
                Math.max(a.xmax(), b.xmax()),
                Math.min(a.ymin(), b.ymin()),
                Math.max(a.ymax(), b.ymax()),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty()));
    }

    /**
     * A box spanning the whole longitude range with the unioned latitude extent. An antimeridian-wrapping box cannot be
     * unioned coordinate-wise; rather than drop either box (which would under-report bounds), widen longitude to the
     * full geographic range. Antimeridian wrapping is meaningful only for geographic longitude in degrees, where the
     * full range is {@code [-180, 180]}.
     */
    private static BoundingBox fullLongitudeSuperset(BoundingBox a, BoundingBox b) {
        return new BoundingBox(
                -180,
                180,
                Math.min(a.ymin(), b.ymin()),
                Math.max(a.ymax(), b.ymax()),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty());
    }
}
