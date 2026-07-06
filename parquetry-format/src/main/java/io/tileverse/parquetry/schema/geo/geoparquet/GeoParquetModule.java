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
package io.tileverse.parquetry.schema.geo.geoparquet;

import io.tileverse.parquetry.schema.ColumnPath;

import tools.jackson.core.Version;
import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.module.SimpleDeserializers;

/**
 * Jackson 3 module that wires the {@link GeoParquetMetadata} ADT into an {@link tools.jackson.databind.ObjectMapper}.
 *
 * <p><strong>Not auto-registered.</strong> Like {@link io.tileverse.parquetry.schema.geo.projjson.ProjJsonModule}, this
 * module is deliberately omitted from {@code META-INF/services} so that downstream consumers calling
 * {@code mapper.findAndRegisterModules()} do not pick it up. Register it by hand on the mappers parquetry owns; the
 * {@link io.tileverse.parquetry.schema.geo.projjson.ProjJsonModule} must be registered alongside this one for the
 * inline CRS PROJJSON to decode into typed records.
 */
public final class GeoParquetModule extends JacksonModule {

    @Override
    public String getModuleName() {
        return "parquetry-geoparquet";
    }

    @Override
    public Version version() {
        return Version.unknownVersion();
    }

    @Override
    public void setupModule(SetupContext context) {
        SimpleDeserializers deserializers = new SimpleDeserializers();
        deserializers.addDeserializer(GeoParquetMetadata.class, new GeoParquetMetadataDeserializer());
        deserializers.addDeserializer(ColumnPath.class, new ColumnPathDeserializer());
        context.addDeserializers(deserializers);
    }
}
