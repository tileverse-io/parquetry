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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.SequencedMap;

import org.junit.jupiter.api.Test;

class StacGeoparquetMetadataTest {

    private static final String BUILDING_COLLECTION = """
            {"id": "building", "title": "Buildings", "extent": {
               "spatial": {"bbox": [[-180.0, -84.3, 179.9, 83.1]]},
               "temporal": {"interval": [["2026-07-22T00:00:00Z", null]]}}}
            """;

    @Test
    void parsesTheCollectionsMapping() {
        String kv = "{\"version\": \"1.1.0\", \"collections\": {\"building\": " + BUILDING_COLLECTION + "}}";
        SequencedMap<String, StacGeoparquetMetadata.EmbeddedCollection> collections =
                StacGeoparquetMetadata.parseCollections(kv);
        assertThat(collections.keySet()).containsExactly("building");
        StacGeoparquetMetadata.EmbeddedCollection building = collections.get("building");
        assertThat(building.id()).isEqualTo("building");
        assertThat(building.title()).isEqualTo("Buildings");
        assertThat(building.extent()).isPresent();
        assertThat(building.extent().orElseThrow().bbox().orElseThrow()).containsExactly(-180.0, -84.3, 179.9, 83.1);
        assertThat(building.extent().orElseThrow().interval().orElseThrow())
                .containsExactly("2026-07-22T00:00:00Z", null);
    }

    @Test
    void parsesTheDeprecatedSingularCollection() {
        String kv = "{\"version\": \"1.0.0\", \"collection\": " + BUILDING_COLLECTION + "}";
        SequencedMap<String, StacGeoparquetMetadata.EmbeddedCollection> collections =
                StacGeoparquetMetadata.parseCollections(kv);
        assertThat(collections.keySet()).containsExactly("building");
    }

    @Test
    void keepsTheDeclaredCollectionOrder() {
        String kv = """
                {"collections": {
                   "water": {"id": "water"},
                   "building": {"id": "building"},
                   "address": {"id": "address"},
                   "place": {"id": "place"}}}
                """;
        SequencedMap<String, StacGeoparquetMetadata.EmbeddedCollection> collections =
                StacGeoparquetMetadata.parseCollections(kv);
        assertThat(collections.keySet()).containsExactly("water", "building", "address", "place");
    }

    @Test
    void versionOnlyAndAbsentValuesYieldNoCollections() {
        assertThat(StacGeoparquetMetadata.parseCollections("{\"version\": \"1.0.0\"}"))
                .isEmpty();
        assertThat(StacGeoparquetMetadata.parseCollections(null)).isEmpty();
        assertThat(StacGeoparquetMetadata.parseCollections("  ")).isEmpty();
    }

    /** The metadata is enrichment; an unusable collection object leaves the reader to derive metadata from the rows. */
    @Test
    void singularCollectionWithoutIdYieldsNoCollections() {
        assertThat(StacGeoparquetMetadata.parseCollections("{\"collection\": {\"title\": \"Buildings\"}}"))
                .isEmpty();
    }

    @Test
    void malformedJsonFailsLoud() {
        assertThatThrownBy(() -> StacGeoparquetMetadata.parseCollections("{not json"))
                .isInstanceOf(StacFormatException.class);
    }

    @Test
    void collectionWithoutExtentParses() {
        String kv = "{\"collections\": {\"c\": {\"id\": \"c\"}}}";
        StacGeoparquetMetadata.EmbeddedCollection c =
                StacGeoparquetMetadata.parseCollections(kv).get("c");
        assertThat(c.title()).isNull();
        assertThat(c.extent()).isEmpty();
    }

    /** A flat {@code spatial.bbox} misses the required nesting; the extent stands, without a spatial component. */
    @Test
    void unnestedSpatialBboxYieldsNoBbox() {
        String kv = "{\"collections\": {\"c\": {\"id\": \"c\", \"extent\": "
                + "{\"spatial\": {\"bbox\": [-180.0, -90.0, 180.0, 90.0]}}}}}";
        StacGeoparquetMetadata.EmbeddedCollection c =
                StacGeoparquetMetadata.parseCollections(kv).get("c");
        assertThat(c.extent()).isPresent();
        assertThat(c.extent().orElseThrow().bbox()).isEmpty();
        assertThat(c.extent().orElseThrow().interval()).isEmpty();
    }

    @Test
    void emptyFirstSpatialBboxYieldsNoBbox() {
        String kv = "{\"collections\": {\"c\": {\"id\": \"c\", \"extent\": {\"spatial\": {\"bbox\": [[]]}}}}}";
        StacGeoparquetMetadata.EmbeddedCollection c =
                StacGeoparquetMetadata.parseCollections(kv).get("c");
        assertThat(c.extent()).isPresent();
        assertThat(c.extent().orElseThrow().bbox()).isEmpty();
    }
}
