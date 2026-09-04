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
package io.tileverse.parquetry.geotools.export;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.nested.NestedType;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.geotools.data.CatalogDataStore;
import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStore;
import io.tileverse.parquetry.io.LocalFileSource;

/**
 * Round-trips feature types with nested attributes through {@link GeoParquetExporter} and back through
 * {@link GeoParquetDataStore}: {@link NestedSchemaNodes} converts the attributes' recorded {@link NestedType} shapes
 * into their Parquet groups, and {@link NestedValueAuthor} authors each feature's list and map values onto the batch
 * builder. Covers struct-mediated nesting (list-of-struct, map-of-struct) and container-in-container shapes
 * (list-of-list, list-of-map, map-of-list), the latter authored through the builder's nested container scopes.
 */
class NestedWriteRoundTripIT {

    @Test
    void nestedAttributesRoundTrip(@TempDir Path dir) throws Exception {
        SimpleFeatureType featureType = nestedFeatureType();
        List<SimpleFeature> sourceFeatures = nestedFeatures(featureType);
        Path file = writeToFile(dir, featureType, sourceFeatures);

        Map<String, SimpleFeature> readById = readAllById(file);
        assertThat(readById.keySet()).containsExactlyInAnyOrder("f1", "f2", "f3");

        assertPartialStructAndMaps(readById.get("f1"));
        assertEmptyContainers(readById.get("f2"));
        assertNullContainers(readById.get("f3"));
    }

    @Test
    void structAttributeRoundTrips(@TempDir Path dir) throws Exception {
        SimpleFeatureType featureType = structFeatureType();
        List<SimpleFeature> sourceFeatures = structFeatures(featureType);
        Path file = writeToFile(dir, featureType, sourceFeatures);

        Map<String, SimpleFeature> readById = readAllById(file);
        assertThat(readById.keySet()).containsExactlyInAnyOrder("s1", "s2");

        Map<?, ?> address = (Map<?, ?>) readById.get("s1").getAttribute("address");
        assertThat(address).isEqualTo(Map.of("street", "Main St", "zip", 12345));

        assertThat(readById.get("s2").getAttribute("address"))
                .as("a null struct round-trips as null")
                .isNull();
    }

    @Test
    void mapOfStructValuesRoundTrips(@TempDir Path dir) throws Exception {
        SimpleFeatureType featureType = ratingsFeatureType();
        List<SimpleFeature> sourceFeatures = ratingsFeatures(featureType);
        Path file = writeToFile(dir, featureType, sourceFeatures);

        Map<String, SimpleFeature> readById = readAllById(file);
        assertThat(readById.keySet()).containsExactlyInAnyOrder("r1");

        Map<?, ?> ratings = (Map<?, ?>) readById.get("r1").getAttribute("ratings");
        assertThat(ratings)
                .isEqualTo(Map.of(
                        "alice", Map.of("score", 5, "reviewer", "Alice R"),
                        "bob", Map.of("score", 3, "reviewer", "Bob R")));
    }

    @Test
    void containerInContainerAttributesRoundTrip(@TempDir Path dir) throws Exception {
        SimpleFeatureType featureType = containerInContainerFeatureType();
        List<SimpleFeature> sourceFeatures = containerInContainerFeatures(featureType);
        Path file = writeToFile(dir, featureType, sourceFeatures);

        Map<String, SimpleFeature> readById = readAllById(file);
        assertThat(readById.keySet()).containsExactlyInAnyOrder("c1", "c2");

        assertPopulatedContainerInContainer(readById.get("c1"));
        assertSparseContainerInContainer(readById.get("c2"));
    }

    private void assertPopulatedContainerInContainer(SimpleFeature feature) {
        List<?> hierarchies = (List<?>) feature.getAttribute("hierarchies");
        assertThat(hierarchies).hasSize(2);
        assertThat(hierarchies.get(0))
                .as("a populated inner list of structs round-trips, null struct fields included")
                .isEqualTo(List.of(hierarchyOf("dv1", "country"), hierarchyOf("dv2", null)));
        assertThat(hierarchies.get(1))
                .as("an empty inner list round-trips as present and empty")
                .isEqualTo(List.of());

        Map<?, ?> translations = (Map<?, ?>) feature.getAttribute("translations");
        assertThat(translations).hasSize(3);
        assertThat(translations.get("en")).isEqualTo(List.of("one", "two"));
        assertThat(translations.get("empty"))
                .as("an empty list value round-trips as present and empty")
                .isEqualTo(List.of());
        assertThat(translations.containsKey("none"))
                .as("a null list value round-trips as a present key")
                .isTrue();
        assertThat(translations.get("none")).isNull();

        List<?> properties = (List<?>) feature.getAttribute("properties");
        assertThat(properties).hasSize(3);
        assertThat(properties.get(0)).isEqualTo(Map.of("a", 1, "b", 2));
        assertThat(properties.get(1))
                .as("an empty inner map round-trips as present and empty")
                .isEqualTo(Map.of());
        assertThat(properties.get(2))
                .as("a null inner map element round-trips as a null slot")
                .isNull();
    }

    private void assertSparseContainerInContainer(SimpleFeature feature) {
        List<?> hierarchies = (List<?>) feature.getAttribute("hierarchies");
        assertThat(hierarchies).hasSize(2);
        assertThat(hierarchies.get(0))
                .as("a null inner list element round-trips as a null slot")
                .isNull();
        assertThat(hierarchies.get(1)).isEqualTo(List.of(hierarchyOf("dv3", "region")));

        assertThat(feature.getAttribute("translations"))
                .as("a null map attribute round-trips as null")
                .isNull();
        assertThat((List<?>) feature.getAttribute("properties"))
                .as("an empty outer list round-trips as present and empty")
                .isEmpty();
    }

    @Test
    void variantNestedTypeIsRejected() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("variantHolder");
        typeBuilder.userData(NestedType.USER_DATA_KEY, new NestedType.ListType(new NestedType.VariantType()));
        typeBuilder.add("v", List.class);
        SimpleFeatureType featureType = typeBuilder.buildFeatureType();

        assertThatThrownBy(() -> FeatureWriteSchema.of(featureType)).isInstanceOf(IllegalArgumentException.class);
    }

    // --- assertions ---

    private void assertPartialStructAndMaps(SimpleFeature feature) {
        List<?> addresses = (List<?>) feature.getAttribute("addresses");
        assertThat(addresses).hasSize(2);
        assertThat(addresses.get(0)).isEqualTo(addressOf("NYC", 10001));
        assertThat(addresses.get(1))
                .as("a struct element missing a field reads it back as a null entry")
                .isEqualTo(addressOf("SFO", null));

        Map<?, ?> categories = (Map<?, ?>) feature.getAttribute("categories");
        assertThat(categories).isEqualTo(Map.of("primary", "food", "alt", "cafe"));
    }

    private void assertEmptyContainers(SimpleFeature feature) {
        assertThat((List<?>) feature.getAttribute("addresses"))
                .as("an empty list round-trips as present and empty")
                .isEmpty();
        assertThat((Map<?, ?>) feature.getAttribute("categories"))
                .as("an empty map round-trips as present and empty")
                .isEmpty();
    }

    private void assertNullContainers(SimpleFeature feature) {
        assertThat(feature.getAttribute("addresses"))
                .as("a null list round-trips as null")
                .isNull();
        assertThat(feature.getAttribute("categories"))
                .as("a null map round-trips as null")
                .isNull();
    }

    private static Map<String, Object> addressOf(String locality, Integer postcode) {
        Map<String, Object> address = new LinkedHashMap<>();
        address.put("locality", locality);
        address.put("postcode", postcode);
        return address;
    }

    // --- fixture ---

    private static SimpleFeatureType nestedFeatureType() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("nested");
        typeBuilder.add("id", String.class);
        typeBuilder.userData(NestedType.USER_DATA_KEY, addressListType());
        typeBuilder.add("addresses", List.class);
        typeBuilder.userData(NestedType.USER_DATA_KEY, categoriesMapType());
        typeBuilder.add("categories", Map.class);
        return typeBuilder.buildFeatureType();
    }

    private static NestedType.ListType addressListType() {
        return new NestedType.ListType(new NestedType.StructType(List.of(
                new NestedType.Field("locality", new NestedType.ScalarType(String.class)),
                new NestedType.Field("postcode", new NestedType.ScalarType(Integer.class)))));
    }

    private static NestedType.MapType categoriesMapType() {
        return new NestedType.MapType(new NestedType.ScalarType(String.class), new NestedType.ScalarType(String.class));
    }

    /** f1: a partial struct element and a two-entry map. f2: present but empty containers. f3: null containers. */
    private static List<SimpleFeature> nestedFeatures(SimpleFeatureType featureType) {
        SimpleFeature f1 = feature(
                featureType,
                "f1",
                List.of(Map.of("locality", "NYC", "postcode", 10001), Map.of("locality", "SFO")),
                Map.of("primary", "food", "alt", "cafe"));
        SimpleFeature f2 = feature(featureType, "f2", List.of(), Map.of());
        SimpleFeature f3 = feature(featureType, "f3", null, null);
        return List.of(f1, f2, f3);
    }

    private static SimpleFeature feature(
            SimpleFeatureType featureType, String id, List<?> addresses, Map<?, ?> categories) {
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
        builder.add(id);
        builder.add(addresses);
        builder.add(categories);
        return builder.buildFeature(id);
    }

    // --- struct-attribute fixture ---

    private static SimpleFeatureType structFeatureType() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("structHolder");
        typeBuilder.add("id", String.class);
        typeBuilder.userData(NestedType.USER_DATA_KEY, addressStructType());
        typeBuilder.add("address", Map.class);
        return typeBuilder.buildFeatureType();
    }

    private static NestedType.StructType addressStructType() {
        return new NestedType.StructType(List.of(
                new NestedType.Field("street", new NestedType.ScalarType(String.class)),
                new NestedType.Field("zip", new NestedType.ScalarType(Integer.class))));
    }

    /** s1: a present struct. s2: a null struct. */
    private static List<SimpleFeature> structFeatures(SimpleFeatureType featureType) {
        SimpleFeatureBuilder b1 = new SimpleFeatureBuilder(featureType);
        b1.add("s1");
        b1.add(Map.of("street", "Main St", "zip", 12345));
        SimpleFeature f1 = b1.buildFeature("s1");

        SimpleFeatureBuilder b2 = new SimpleFeatureBuilder(featureType);
        b2.add("s2");
        b2.add(null);
        SimpleFeature f2 = b2.buildFeature("s2");

        return List.of(f1, f2);
    }

    // --- map-of-struct-values fixture ---

    private static SimpleFeatureType ratingsFeatureType() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("ratingsHolder");
        typeBuilder.add("id", String.class);
        typeBuilder.userData(NestedType.USER_DATA_KEY, ratingsMapType());
        typeBuilder.add("ratings", Map.class);
        return typeBuilder.buildFeatureType();
    }

    private static NestedType.MapType ratingsMapType() {
        NestedType.StructType ratingType = new NestedType.StructType(List.of(
                new NestedType.Field("score", new NestedType.ScalarType(Integer.class)),
                new NestedType.Field("reviewer", new NestedType.ScalarType(String.class))));
        return new NestedType.MapType(new NestedType.ScalarType(String.class), ratingType);
    }

    private static List<SimpleFeature> ratingsFeatures(SimpleFeatureType featureType) {
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
        builder.add("r1");
        builder.add(Map.of(
                "alice", Map.of("score", 5, "reviewer", "Alice R"),
                "bob", Map.of("score", 3, "reviewer", "Bob R")));
        return List.of(builder.buildFeature("r1"));
    }

    // --- container-in-container fixture ---

    private static SimpleFeatureType containerInContainerFeatureType() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("containers");
        typeBuilder.add("id", String.class);
        typeBuilder.userData(NestedType.USER_DATA_KEY, hierarchiesListOfListType());
        typeBuilder.add("hierarchies", List.class);
        typeBuilder.userData(NestedType.USER_DATA_KEY, translationsMapOfListType());
        typeBuilder.add("translations", Map.class);
        typeBuilder.userData(NestedType.USER_DATA_KEY, propertiesListOfMapType());
        typeBuilder.add("properties", List.class);
        return typeBuilder.buildFeatureType();
    }

    /** The Overture divisions {@code hierarchies} shape: a list whose element is a list of structs. */
    private static NestedType.ListType hierarchiesListOfListType() {
        NestedType.StructType hierarchyEntry = new NestedType.StructType(List.of(
                new NestedType.Field("division_id", new NestedType.ScalarType(String.class)),
                new NestedType.Field("subtype", new NestedType.ScalarType(String.class))));
        return new NestedType.ListType(new NestedType.ListType(hierarchyEntry));
    }

    private static NestedType.MapType translationsMapOfListType() {
        return new NestedType.MapType(
                new NestedType.ScalarType(String.class),
                new NestedType.ListType(new NestedType.ScalarType(String.class)));
    }

    private static NestedType.ListType propertiesListOfMapType() {
        return new NestedType.ListType(new NestedType.MapType(
                new NestedType.ScalarType(String.class), new NestedType.ScalarType(Integer.class)));
    }

    /**
     * c1: populated nested containers, empties and nulls at the inner depth. c2: nulls and empties at the outer depth.
     */
    private static List<SimpleFeature> containerInContainerFeatures(SimpleFeatureType featureType) {
        Map<String, Object> translations = new LinkedHashMap<>();
        translations.put("en", List.of("one", "two"));
        translations.put("empty", List.of());
        translations.put("none", null);

        SimpleFeatureBuilder b1 = new SimpleFeatureBuilder(featureType);
        b1.add("c1");
        b1.add(List.of(List.of(hierarchyOf("dv1", "country"), hierarchyOf("dv2", null)), List.of()));
        b1.add(translations);
        b1.add(Arrays.asList(Map.of("a", 1, "b", 2), Map.of(), null));
        SimpleFeature c1 = b1.buildFeature("c1");

        SimpleFeatureBuilder b2 = new SimpleFeatureBuilder(featureType);
        b2.add("c2");
        b2.add(Arrays.asList(null, List.of(hierarchyOf("dv3", "region"))));
        b2.add(null);
        b2.add(List.of());
        SimpleFeature c2 = b2.buildFeature("c2");

        return List.of(c1, c2);
    }

    private static Map<String, Object> hierarchyOf(String divisionId, String subtype) {
        Map<String, Object> hierarchy = new LinkedHashMap<>();
        hierarchy.put("division_id", divisionId);
        hierarchy.put("subtype", subtype);
        return hierarchy;
    }

    // --- write/read plumbing ---

    private static Path writeToFile(Path dir, SimpleFeatureType featureType, List<SimpleFeature> sourceFeatures)
            throws IOException {
        FeatureCollection<SimpleFeatureType, SimpleFeature> features =
                new ListFeatureCollection(featureType, sourceFeatures);
        Path file = dir.resolve("nested_write.parquet");
        try (OutputStream out = Files.newOutputStream(file)) {
            GeoParquetExporter.export(features, out, WriteOptions.defaults());
        }
        return file;
    }

    private static Map<String, SimpleFeature> readAllById(Path file) throws IOException {
        Map<String, SimpleFeature> byId = new LinkedHashMap<>();
        FilesetCatalog catalog = FilesetCatalog.open(
                LocalFileSource.file(file),
                CatalogOptions.builder().datasetName("nested_write").build());
        try (CatalogDataStore store = new GeoParquetDataStore(catalog)) {
            ContentFeatureSource fs = (ContentFeatureSource) store.getFeatureSource("nested_write");
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(Query.ALL)) {
                while (reader.hasNext()) {
                    SimpleFeature feature = reader.next();
                    byId.put((String) feature.getAttribute("id"), feature);
                }
            }
        }
        return byId;
    }
}
