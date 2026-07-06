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
package io.tileverse.parquetry.avro;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.json.JsonFactory;

/**
 * Parses a JSON document into a generic tree of {@link LinkedHashMap}, {@link ArrayList}, {@link String}, {@link Long},
 * {@link Double}, {@link Boolean}, and {@code null}. Insertion-ordered maps preserve Avro record field order. Streaming
 * via jackson-core only, no databind.
 */
final class JsonReader {

    private static final JsonFactory FACTORY = new JsonFactory();

    private JsonReader() {}

    static Object parse(String json) {
        try (JsonParser parser = FACTORY.createParser(json)) {
            JsonToken first = parser.nextToken();
            if (first == null) {
                throw new AvroFormatException("Empty JSON input");
            }
            return readValue(parser, first);
        } catch (JacksonException e) {
            throw new AvroFormatException("Malformed JSON", e);
        }
    }

    private static Object readValue(JsonParser parser, JsonToken token) {
        return switch (token) {
            case START_OBJECT -> readObject(parser);
            case START_ARRAY -> readArray(parser);
            case VALUE_STRING -> parser.getString();
            case VALUE_NUMBER_INT -> parser.getLongValue();
            case VALUE_NUMBER_FLOAT -> parser.getDoubleValue();
            case VALUE_TRUE -> Boolean.TRUE;
            case VALUE_FALSE -> Boolean.FALSE;
            case VALUE_NULL -> null;
            default -> throw new AvroFormatException("Unexpected JSON token: " + token);
        };
    }

    private static Map<String, Object> readObject(JsonParser parser) {
        Map<String, Object> object = new LinkedHashMap<>();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String name = parser.currentName();
            JsonToken valueToken = parser.nextToken();
            object.put(name, readValue(parser, valueToken));
        }
        return object;
    }

    private static List<Object> readArray(JsonParser parser) {
        List<Object> array = new ArrayList<>();
        JsonToken token = parser.nextToken();
        while (token != JsonToken.END_ARRAY) {
            array.add(readValue(parser, token));
            token = parser.nextToken();
        }
        return array;
    }
}
