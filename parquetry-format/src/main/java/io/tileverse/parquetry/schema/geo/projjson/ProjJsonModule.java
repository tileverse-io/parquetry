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
package io.tileverse.parquetry.schema.geo.projjson;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import com.fasterxml.jackson.annotation.JsonInclude;

import tools.jackson.core.Version;
import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.module.SimpleDeserializers;
import tools.jackson.databind.module.SimpleSerializers;

/**
 * Jackson 3 module that wires the {@link CoordinateReferenceSystem} ADT into an
 * {@link tools.jackson.databind.ObjectMapper}.
 *
 * <p><strong>Not auto-registered.</strong> This module is deliberately omitted from {@code META-INF/services} so that
 * downstream consumers calling {@code mapper.findAndRegisterModules()} (Spring Boot, JAX-RS providers, test harnesses)
 * do not pick it up and re-interpret their own unrelated PROJJSON or {@code "geo"}-keyed JSON. Register it by hand on
 * the mappers parquetry owns.
 */
public final class ProjJsonModule extends JacksonModule {

    @Override
    public String getModuleName() {
        return "parquetry-projjson";
    }

    @Override
    public Version version() {
        return Version.unknownVersion();
    }

    @Override
    public void setupModule(SetupContext context) {
        SimpleDeserializers deserializers = new SimpleDeserializers();
        deserializers.addDeserializer(CoordinateReferenceSystem.class, new CoordinateReferenceSystemDeserializer());
        deserializers.addDeserializer(Identifier.class, new IdentifierDeserializer());
        context.addDeserializers(deserializers);

        SimpleSerializers serializers = new SimpleSerializers();
        serializers.addSerializer(CoordinateReferenceSystem.class, new CoordinateReferenceSystemSerializer());
        context.addSerializers(serializers);

        // Drop Optional / OptionalInt / OptionalDouble / OptionalLong fields whose value is empty from the JSON output.
        // Without this, Jackson 3 renders Optional.empty() as a JSON null, and the symmetric deserializer reads that
        // null back as Optional[null] / OptionalInt.empty() inconsistently across types - breaking round-trip equality
        // (an empty Optional<JsonNode> becomes Optional[NullNode] on the next read).
        JsonInclude.Value nonAbsent = JsonInclude.Value.construct(JsonInclude.Include.NON_ABSENT, null);
        context.configOverride(Optional.class).setInclude(nonAbsent);
        context.configOverride(OptionalInt.class).setInclude(nonAbsent);
        context.configOverride(OptionalLong.class).setInclude(nonAbsent);
        context.configOverride(OptionalDouble.class).setInclude(nonAbsent);
    }
}
