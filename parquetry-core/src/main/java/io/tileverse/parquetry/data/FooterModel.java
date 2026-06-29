/*
 * Copyright (c) 2026 Multivers.io
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

import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Derives the cached, immutable views {@link ParquetFileReader} keeps off the footer it read once: the parquetry
 * {@link ParquetSchema}, the parsed GeoParquet metadata, the collapsed key/value metadata, and the public row-group
 * view.
 */
final class FooterModel {

    private static final String GEO_KEY = "geo";

    private FooterModel() {}

    /**
     * Builds the parquetry {@link ParquetSchema} from the footer, folding GeoParquet 1.x's {@code "geo"} key-value
     * metadata into native Geometry / Geography logical-type annotations on WKB columns that lack one. Downstream code
     * (e.g. the JtsMaterializer in {@code io.tileverse.parquetry.geo}) then sees one shape regardless of file version.
     */
    static ParquetSchema buildFileSchema(FileMetaData footer, Map<String, String> kvMetadata) {
        return SchemaBuilder.build(footer.schema(), kvMetadata);
    }

    /**
     * Parses the GeoParquet {@code "geo"} key-value entry once, returning empty when it is absent or cannot be parsed.
     * The covering-column lowering consults it to replace spatial predicate leaves.
     */
    static Optional<GeoParquetMetadata> parseGeoMetadata(Map<String, String> kvMetadata) {
        String geoJson = kvMetadata.get(GEO_KEY);
        if (geoJson == null || geoJson.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(GeoParquetMetadata.parse(geoJson));
        } catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    static Map<String, String> collapseKeyValueMetadata(List<KeyValue> entries) {
        if (entries.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> collapsed = LinkedHashMap.newLinkedHashMap(entries.size());
        for (KeyValue entry : entries) {
            collapsed.put(entry.key(), entry.value().orElse(""));
        }
        return Collections.unmodifiableMap(collapsed);
    }

    static List<RowGroupSummary> toRowGroupView(FileMetaData footer) {
        List<RowGroup> rgs = footer.rowGroups();
        List<RowGroupSummary> view = new ArrayList<>(rgs.size());
        for (int i = 0; i < rgs.size(); i++) {
            RowGroup rg = rgs.get(i);
            view.add(new RowGroupSummary(
                    i,
                    rg.numRows(),
                    rg.totalByteSize(),
                    rg.totalCompressedSize().orElse(-1L)));
        }
        return Collections.unmodifiableList(view);
    }
}
