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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.footer.CompactFooter;
import io.tileverse.parquetry.internal.footer.LeafIndex;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Everything derived from a file's footer by a {@link ParquetFileReader}, in the immutable forms kept by the reader:
 * the packed planning footer, the parquetry schema, the leaf index addressing that schema's columns, the parsed
 * GeoParquet metadata, the spatial bounds of each geometry column, the collapsed key/value metadata, and the public
 * row-group view.
 *
 * <p>All seven forms depend on nothing but the footer bytes, which makes the whole record shareable between every
 * reader opened over one file - the unit held by {@link FooterMetadataCache}.
 *
 * @param compactFooter the planning form of the footer, packed into one blob
 * @param fileSchema the file schema, with GeoParquet 1.x logical-type annotations folded in
 * @param leafIndex the mapping between the schema's leaf columns and the ordinals addressing them in the packed footer
 * @param geoMetadata the parsed GeoParquet {@code "geo"} metadata, empty for a file without one
 * @param spatialBounds the most precise bounds tier applicable to the file, consulted for a geometry column's file and
 *     row-group extents
 * @param keyValueMetadata the file's key/value metadata, collapsed to the last value per key
 * @param rowGroups the public row-group view, in file order
 */
record FooterMetadata(
        CompactFooter compactFooter,
        ParquetSchema fileSchema,
        LeafIndex leafIndex,
        Optional<GeoParquetMetadata> geoMetadata,
        SpatialBoundsSource spatialBounds,
        Map<String, String> keyValueMetadata,
        List<RowGroupSummary> rowGroups) {

    /**
     * Reads {@code source}'s footer and derives every retained form from it. This is the only scope where the decoded
     * wire records exist; they are unreachable once the returned record is built.
     */
    static FooterMetadata parse(ByteRangeSource source) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        Map<String, String> kvMetadata = FooterModel.collapseKeyValueMetadata(footer.keyValueMetadata());
        ParquetSchema fileSchema = FooterModel.buildFileSchema(footer, kvMetadata);
        Optional<GeoParquetMetadata> geoMetadata = FooterModel.parseGeoMetadata(kvMetadata);
        List<RowGroupSummary> rowGroups = FooterModel.toRowGroupView(footer);
        LeafIndex leafIndex = LeafIndex.of(fileSchema);
        CompactFooter compactFooter = CompactFooter.encode(footer, leafIndex);
        SpatialBoundsSource spatialBounds = SpatialBoundsSource.of(compactFooter, leafIndex, fileSchema, geoMetadata);
        return new FooterMetadata(
                compactFooter, fileSchema, leafIndex, geoMetadata, spatialBounds, kvMetadata, rowGroups);
    }
}
