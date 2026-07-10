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
package io.tileverse.parquetry.probes;

import java.nio.file.Path;

import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The resolved, immutable inputs every read engine shares: the file, the attribute equality filter (column name plus
 * the parquetry {@link ColumnPath} form and value), the spatial filter (geometry column plus the query diamond and its
 * envelope), and the GeoParquet 1.1 bbox covering prefix with whether those covering columns exist for the parquet-java
 * pushdown. The geometry and attribute fields are null when the file does not support that filter; the probe only runs
 * a scenario whose inputs resolved.
 */
record ReadContext(
        Path file,
        String attributeColumnName,
        ColumnPath attributeColumn,
        String attributeValue,
        String geometryColumnName,
        ColumnPath geometryColumn,
        Geometry queryDiamond,
        Bbox queryEnvelope,
        String bboxColumn,
        boolean bboxCoveringAvailable) {}
