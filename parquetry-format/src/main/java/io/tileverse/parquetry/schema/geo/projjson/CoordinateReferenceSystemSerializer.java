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
package io.tileverse.parquetry.schema.geo.projjson;

import java.util.Optional;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;

/**
 * Polymorphic serializer mirror of {@link CoordinateReferenceSystemDeserializer}. Each concrete CRS record renders with
 * a leading {@code "type"} discriminator followed by its declared fields; {@link CoordinateReferenceSystem.Unknown}
 * re-emits the captured raw JSON tree verbatim so round-trip fidelity is preserved for {@code type} discriminators this
 * package does not yet model.
 *
 * <p><strong>Why the bodies are written field-by-field.</strong> Jackson 3's {@code SimpleSerializers} resolves
 * serializer lookups by walking class mappings, superclasses, and interfaces in turn. The polymorphic registration
 * binds this serializer at {@link CoordinateReferenceSystem} (the sealed interface); every concrete permit
 * ({@code GeographicCRS}, {@code ProjectedCRS}, ...) implements that interface, so any call to
 * {@code findValueSerializer(GeographicCRS.class)} reaches us again via the interface walk. Naive approaches that ask
 * Jackson to render the "body" of the concrete record (via {@code valueToTree}, {@code writePOJO},
 * {@code findValueSerializer}, or a sibling {@link tools.jackson.databind.json.JsonMapper JsonMapper}) therefore
 * recurse into the polymorphic serializer ad infinitum. Writing the body fields manually breaks the cycle while still
 * delegating nested values (Identifier, Ellipsoid, Axis, ...) to the surrounding context's default serializers via
 * {@link SerializationContext#defaultSerializeProperty(String, Object, JsonGenerator)} - nested {@code Optional} CRS
 * fields recursively dispatch back into {@link #serialize} naturally (one level per nested CRS).
 */
final class CoordinateReferenceSystemSerializer extends ValueSerializer<CoordinateReferenceSystem> {

    private static final String COORDINATE_SYSTEM = "coordinate_system";

    @Override
    public void serialize(CoordinateReferenceSystem value, JsonGenerator gen, SerializationContext ctxt)
            throws JacksonException {
        switch (value) {
            case CoordinateReferenceSystem.Unknown u -> gen.writeTree(u.raw());
            case GeographicCRS g -> writeGeographicCrs(g, gen, ctxt);
            case GeodeticCRS g -> writeGeodeticCrs(g, gen, ctxt);
            case ProjectedCRS p -> writeProjectedCrs(p, gen, ctxt);
            case CompoundCRS c -> writeCompoundCrs(c, gen, ctxt);
            case BoundCRS b -> writeBoundCrs(b, gen, ctxt);
            case VerticalCRS v -> writeVerticalCrs(v, gen, ctxt);
            case Transformation t -> writeTransformation(t, gen, ctxt);
        }
    }

    private static void writeGeographicCrs(GeographicCRS g, JsonGenerator gen, SerializationContext ctxt)
            throws JacksonException {
        gen.writeStartObject();
        writeDiscriminator(gen, "GeographicCRS");
        writeOptionalProperty(gen, ctxt, "name", g.name());
        writeOptionalProperty(gen, ctxt, "id", g.id());
        writeOptionalProperty(gen, ctxt, "datum", g.datum());
        writeOptionalProperty(gen, ctxt, COORDINATE_SYSTEM, g.coordinateSystem());
        gen.writeEndObject();
    }

    private static void writeGeodeticCrs(GeodeticCRS g, JsonGenerator gen, SerializationContext ctxt)
            throws JacksonException {
        gen.writeStartObject();
        writeDiscriminator(gen, "GeodeticCRS");
        writeOptionalProperty(gen, ctxt, "name", g.name());
        writeOptionalProperty(gen, ctxt, "id", g.id());
        writeOptionalProperty(gen, ctxt, "datum", g.datum());
        writeOptionalProperty(gen, ctxt, COORDINATE_SYSTEM, g.coordinateSystem());
        gen.writeEndObject();
    }

    private static void writeProjectedCrs(ProjectedCRS p, JsonGenerator gen, SerializationContext ctxt)
            throws JacksonException {
        gen.writeStartObject();
        writeDiscriminator(gen, "ProjectedCRS");
        writeOptionalProperty(gen, ctxt, "name", p.name());
        writeOptionalProperty(gen, ctxt, "id", p.id());
        writeOptionalProperty(gen, ctxt, "base_crs", p.baseCrs());
        writeOptionalProperty(gen, ctxt, "conversion", p.conversion());
        writeOptionalProperty(gen, ctxt, COORDINATE_SYSTEM, p.coordinateSystem());
        gen.writeEndObject();
    }

    private static void writeCompoundCrs(CompoundCRS c, JsonGenerator gen, SerializationContext ctxt)
            throws JacksonException {
        gen.writeStartObject();
        writeDiscriminator(gen, "CompoundCRS");
        writeOptionalProperty(gen, ctxt, "name", c.name());
        writeOptionalProperty(gen, ctxt, "id", c.id());
        ctxt.defaultSerializeProperty("components", c.components(), gen);
        gen.writeEndObject();
    }

    private static void writeBoundCrs(BoundCRS b, JsonGenerator gen, SerializationContext ctxt)
            throws JacksonException {
        gen.writeStartObject();
        writeDiscriminator(gen, "BoundCRS");
        writeOptionalProperty(gen, ctxt, "name", b.name());
        writeOptionalProperty(gen, ctxt, "id", b.id());
        writeOptionalProperty(gen, ctxt, "source_crs", b.sourceCrs());
        writeOptionalProperty(gen, ctxt, "target_crs", b.targetCrs());
        writeOptionalProperty(gen, ctxt, "transformation", b.transformation());
        gen.writeEndObject();
    }

    private static void writeVerticalCrs(VerticalCRS v, JsonGenerator gen, SerializationContext ctxt)
            throws JacksonException {
        gen.writeStartObject();
        writeDiscriminator(gen, "VerticalCRS");
        writeOptionalProperty(gen, ctxt, "name", v.name());
        writeOptionalProperty(gen, ctxt, "id", v.id());
        writeOptionalProperty(gen, ctxt, COORDINATE_SYSTEM, v.coordinateSystem());
        gen.writeEndObject();
    }

    private static void writeTransformation(Transformation t, JsonGenerator gen, SerializationContext ctxt)
            throws JacksonException {
        gen.writeStartObject();
        writeDiscriminator(gen, "Transformation");
        writeOptionalProperty(gen, ctxt, "name", t.name());
        writeOptionalProperty(gen, ctxt, "source_crs", t.sourceCrs());
        writeOptionalProperty(gen, ctxt, "target_crs", t.targetCrs());
        writeOptionalProperty(gen, ctxt, "method", t.method());
        ctxt.defaultSerializeProperty("parameters", t.parameters(), gen);
        writeOptionalProperty(gen, ctxt, "accuracy", t.accuracy());
        writeOptionalProperty(gen, ctxt, "id", t.id());
        gen.writeEndObject();
    }

    private static void writeDiscriminator(JsonGenerator gen, String typeName) throws JacksonException {
        gen.writeName("type");
        gen.writeString(typeName);
    }

    private static void writeOptionalProperty(
            JsonGenerator gen, SerializationContext ctxt, String name, Optional<?> value) throws JacksonException {
        if (value.isPresent()) {
            ctxt.defaultSerializeProperty(name, value.get(), gen);
        }
    }
}
