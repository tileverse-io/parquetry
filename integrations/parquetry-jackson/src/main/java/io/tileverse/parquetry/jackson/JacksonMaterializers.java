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
package io.tileverse.parquetry.jackson;

import java.io.StringWriter;

import io.tileverse.parquetry.materializer.Materializer;

import tools.jackson.core.JsonGenerator;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.util.TokenBuffer;

/**
 * Read-pipeline {@link Materializer}s that turn each row into JSON without exposing the caller to {@code ParquetRecord}
 * or {@code Variant}. Both wrap {@link JsonRecordEncoder#writeObject}.
 */
public final class JacksonMaterializers {

    private static final JsonFactory FACTORY = new JsonFactory();
    private static final JsonMapper MAPPER = JsonMapper.shared();

    private JacksonMaterializers() {}

    /** A materializer that yields one {@link JsonNode} per row. */
    public static Materializer<JsonNode> jsonNode() {
        return (schema, row) -> {
            TokenBuffer buffer = TokenBuffer.forGeneration();
            JsonRecordEncoder.writeObject(buffer, schema, row);
            return MAPPER.readTree(buffer.asParser());
        };
    }

    /** A materializer that yields one compact JSON string per row. */
    public static Materializer<String> jsonString() {
        return (schema, row) -> {
            StringWriter writer = new StringWriter();
            try (JsonGenerator generator = FACTORY.createGenerator(ObjectWriteContext.empty(), writer)) {
                JsonRecordEncoder.writeObject(generator, schema, row);
            }
            return writer.toString();
        };
    }
}
