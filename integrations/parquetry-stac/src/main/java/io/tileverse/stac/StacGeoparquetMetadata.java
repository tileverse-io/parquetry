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
package io.tileverse.stac;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.SequencedMap;

import tools.jackson.databind.JsonNode;

/**
 * Parses the {@code stac-geoparquet} footer key-value metadata of a stac-geoparquet item table. The value is a JSON
 * object with an optional {@code version} ("1.0.0" or "1.1.0"), an optional {@code collections} mapping from collection
 * id to STAC Collection object (1.1.0), and an optional deprecated singular {@code collection} object (1.0.0). Neither
 * member is required, and the declared version is not needed to tell the two shapes apart. Only the collection metadata
 * the catalog model consumes is extracted: id, title, and extent.
 *
 * <p>The metadata enriches what the item rows already describe. A value that declares no collection object, or a
 * singular collection with no id to key it by, yields no collections rather than failing, leaving the reader to derive
 * collection metadata from the rows.
 */
public final class StacGeoparquetMetadata {

    private static final SequencedMap<String, EmbeddedCollection> NO_COLLECTIONS =
            Collections.unmodifiableSequencedMap(new LinkedHashMap<>());

    private StacGeoparquetMetadata() {}

    /**
     * A STAC Collection object embedded in an item table footer, reduced to the members the catalog model uses.
     *
     * @param id the collection id, unique within the item table
     * @param title a human-readable title, or null
     * @param extent the collection's declared extent, when present
     */
    public record EmbeddedCollection(String id, String title, Optional<StacExtent> extent) {

        public EmbeddedCollection {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(extent, "extent");
        }
    }

    /**
     * Extracts the embedded collections from the {@code stac-geoparquet} key-value metadata, keyed by collection id and
     * iterating in the order the metadata declares them. A null or blank value, a value with neither
     * {@code collections} nor {@code collection}, or a singular collection with no id, yields an empty map. Malformed
     * JSON fails loud.
     *
     * @throws StacFormatException if the value is not valid JSON
     */
    public static SequencedMap<String, EmbeddedCollection> parseCollections(String kvValueJson) {
        if (kvValueJson == null || kvValueJson.isBlank()) {
            return NO_COLLECTIONS;
        }
        JsonNode root = StacJson.parse(kvValueJson);
        JsonNode mapping = root.get("collections");
        if (mapping != null && mapping.isObject()) {
            return readCollectionsMapping(mapping);
        }
        return readSingularCollection(root.get("collection"));
    }

    private static SequencedMap<String, EmbeddedCollection> readCollectionsMapping(JsonNode mapping) {
        SequencedMap<String, EmbeddedCollection> collections = new LinkedHashMap<>();
        for (String id : mapping.propertyNames()) {
            collections.put(id, readEmbeddedCollection(id, mapping.get(id)));
        }
        return Collections.unmodifiableSequencedMap(collections);
    }

    /** Reads the deprecated 1.0.0 shape, whose single collection object supplies its own key. */
    private static SequencedMap<String, EmbeddedCollection> readSingularCollection(JsonNode collection) {
        if (collection == null || !collection.isObject()) {
            return NO_COLLECTIONS;
        }
        String id = collection.path("id").stringValue(null);
        if (id == null) {
            return NO_COLLECTIONS;
        }
        SequencedMap<String, EmbeddedCollection> collections = new LinkedHashMap<>();
        collections.put(id, readEmbeddedCollection(id, collection));
        return Collections.unmodifiableSequencedMap(collections);
    }

    private static EmbeddedCollection readEmbeddedCollection(String id, JsonNode collection) {
        String title = collection.path("title").stringValue(null);
        return new EmbeddedCollection(id, title, StacJson.readExtent(collection));
    }
}
