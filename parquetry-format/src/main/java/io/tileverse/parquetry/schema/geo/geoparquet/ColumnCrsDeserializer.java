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
package io.tileverse.parquetry.schema.geo.geoparquet;

import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;

/**
 * Reads the {@code crs} key of a GeoParquet column into a {@link ColumnCrs}.
 *
 * <p>The three readings of the key reach this class by three different routes. An absent key never reaches
 * {@link #deserialize}, and Jackson asks for {@link #getAbsentValue} instead. A null key arrives through
 * {@link #getNullValue}. Anything else is an inline PROJJSON document.
 */
final class ColumnCrsDeserializer extends StdDeserializer<ColumnCrs> {

    ColumnCrsDeserializer() {
        super(ColumnCrs.class);
    }

    @Override
    public ColumnCrs deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
        CoordinateReferenceSystem projjson = ctxt.readValue(p, CoordinateReferenceSystem.class);
        return projjson == null ? ColumnCrs.UNKNOWN : ColumnCrs.of(projjson);
    }

    @Override
    public ColumnCrs getNullValue(DeserializationContext ctxt) {
        return ColumnCrs.UNKNOWN;
    }

    @Override
    public Object getAbsentValue(DeserializationContext ctxt) {
        return ColumnCrs.DEFAULTED;
    }
}
