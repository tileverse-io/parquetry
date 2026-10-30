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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.footer.CompactFooter;
import io.tileverse.parquetry.internal.footer.LeafIndex;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.geoparquet.GeometryColumns;

/**
 * Everything derived from a file's footer by a {@link ParquetFileReader}, in the immutable forms kept by the reader:
 * the packed planning footer, the parquetry schema, the leaf index addressing that schema's columns, the parsed
 * GeoParquet metadata, the spatial bounds of each geometry column, the collapsed key/value metadata, the public
 * row-group view, and the bytes taken by the column chunks.
 *
 * <p>These forms depend on nothing but the footer bytes: the readers opened over one file share one record, the unit
 * held by {@link FooterMetadataCache}.
 *
 * @param compactFooter the planning form of the footer, packed into one blob
 * @param fileSchema the file schema, with GeoParquet 1.x logical-type annotations folded in
 * @param leafIndex the mapping between the schema's leaf columns and the ordinals addressing them in the packed footer
 * @param geoMetadata the parsed GeoParquet {@code "geo"} metadata, empty for a file without one
 * @param primaryGeometry the primary geometry column of the file, see {@link GeometryColumns#primary}; empty for a file
 *     without geometry column
 * @param spatialBounds the most precise bounds tier applicable to the file, consulted for a geometry column's file and
 *     row-group extents
 * @param keyValueMetadata the file's key/value metadata, collapsed to the last value per key
 * @param rowGroups the public row-group view, in file order
 * @param columnChunkBytes the compressed bytes of the column chunks, as the footer declares them; the page indexes and
 *     the bloom filters lie in the rest of the file
 */
record FooterMetadata(
        CompactFooter compactFooter,
        ParquetSchema fileSchema,
        LeafIndex leafIndex,
        Optional<GeoParquetMetadata> geoMetadata,
        Optional<ColumnPath> primaryGeometry,
        SpatialBoundsSource spatialBounds,
        Map<String, String> keyValueMetadata,
        List<RowGroupSummary> rowGroups,
        long columnChunkBytes) {

    /**
     * Reads {@code source}'s footer and derives every retained form from it. This is the only scope where the decoded
     * wire records exist; they are unreachable once the returned record is built.
     *
     * <p>The GeoParquet {@code "geo"} metadata is parsed once: the schema folds it into GeoParquet 1.x logical-type
     * annotations, and the reader keeps it.
     */
    static FooterMetadata parse(ByteRangeSource source) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        Map<String, String> kvMetadata = collapseKeyValueMetadata(footer.keyValueMetadata());
        Optional<GeoParquetMetadata> geoMetadata = SchemaBuilder.geoMetadata(kvMetadata);
        ParquetSchema fileSchema = SchemaBuilder.build(footer.schema(), geoMetadata);
        Optional<ColumnPath> primaryGeometry = GeometryColumns.primary(fileSchema, geoMetadata);
        List<RowGroupSummary> rowGroups = rowGroupView(footer);
        LeafIndex leafIndex = LeafIndex.of(fileSchema);
        CompactFooter compactFooter = CompactFooter.encode(footer, leafIndex);
        SpatialBoundsSource spatialBounds = SpatialBoundsSource.of(compactFooter, leafIndex, fileSchema, geoMetadata);
        long columnChunkBytes = columnChunkBytes(footer);
        return new FooterMetadata(
                compactFooter,
                fileSchema,
                leafIndex,
                geoMetadata,
                primaryGeometry,
                spatialBounds,
                kvMetadata,
                rowGroups,
                columnChunkBytes);
    }

    /** The key/value metadata of the file, collapsed to the last value per key. */
    private static Map<String, String> collapseKeyValueMetadata(List<KeyValue> entries) {
        if (entries.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> collapsed = LinkedHashMap.newLinkedHashMap(entries.size());
        for (KeyValue entry : entries) {
            collapsed.put(entry.key(), entry.value().orElse(""));
        }
        return Collections.unmodifiableMap(collapsed);
    }

    /** The public view of the row groups, in file order. */
    private static List<RowGroupSummary> rowGroupView(FileMetaData footer) {
        List<RowGroup> rowGroups = footer.rowGroups();
        List<RowGroupSummary> view = new ArrayList<>(rowGroups.size());
        for (int index = 0; index < rowGroups.size(); index++) {
            RowGroup rowGroup = rowGroups.get(index);
            view.add(new RowGroupSummary(
                    index,
                    rowGroup.numRows(),
                    rowGroup.totalByteSize(),
                    rowGroup.totalCompressedSize().orElse(-1L)));
        }
        return Collections.unmodifiableList(view);
    }

    /**
     * The compressed bytes of the column chunks of the file, as its footer declares them. The sizes of a corrupt footer
     * sum to anything: the readers bound the sum by the size of the file.
     */
    static long columnChunkBytes(FileMetaData footer) {
        long total = 0L;
        for (RowGroup rowGroup : footer.rowGroups()) {
            for (ColumnChunk chunk : rowGroup.columns()) {
                Optional<ColumnMetaData> metaData = chunk.metaData();
                total += metaData.map(ColumnMetaData::totalCompressedSize).orElse(0L);
            }
        }
        return total;
    }
}
