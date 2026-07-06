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
package org.geotools.data.nested;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.MultiValuedFilter.MatchAction;
import org.geotools.data.nested.NestedType.Field;
import org.geotools.data.nested.NestedType.ListType;
import org.geotools.data.nested.NestedType.ScalarType;
import org.geotools.data.nested.NestedType.StructType;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.expression.PropertyAccessor;
import org.geotools.filter.expression.PropertyAccessors;
import org.junit.jupiter.api.Test;

/**
 * Verifies the registered {@link NestedPropertyAccessor} navigates a slash path into nested value objects with the same
 * semantics core's quantified pushdown uses, including keeping null leaf members so a residual {@code MatchAction}
 * agrees with the push-down answer.
 *
 * <p>Features are built by hand with the {@link NestedType#USER_DATA_KEY} user data the reader attaches, matching the
 * {@code NestedFixtures} rows: addresses {@code LIST<STRUCT(locality, postcode)>} and a brand {@code STRUCT(name)}.
 */
class NestedPropertyAccessorTest {

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    private static final StructType ADDRESS_TYPE = new StructType(List.of(
            new Field("locality", new ScalarType(String.class)), new Field("postcode", new ScalarType(String.class))));
    private static final ListType ADDRESSES_TYPE = new ListType(ADDRESS_TYPE);
    private static final StructType BRAND_TYPE =
            new StructType(List.of(new Field("name", new ScalarType(String.class))));

    @Test
    void factoryClaimsSlashPathsOnly() {
        NestedPropertyAccessorFactory factory = new NestedPropertyAccessorFactory();
        assertThat(factory.createPropertyAccessor(SimpleFeature.class, "addresses/locality", null, null))
                .isNotNull();
        assertThat(factory.createPropertyAccessor(SimpleFeature.class, "addresses", null, null))
                .isNull();
    }

    @Test
    void ourAccessorWinsTheChainForNestedPaths() {
        SimpleFeature feature = featureOne();
        List<PropertyAccessor> accessors =
                PropertyAccessors.findPropertyAccessors(feature, "addresses/locality", null, null);
        assertThat(accessors).isNotEmpty();
        assertThat(accessors.get(0)).isInstanceOf(NestedPropertyAccessor.class);
    }

    @Test
    void listPathFlattensLeafValues() {
        SimpleFeature feature = featureOne();
        Object result = accessor().get(feature, "addresses/locality", Object.class);
        assertThat(result).isInstanceOf(List.class);
        assertThat(asObjectList(result)).contains("NYC");
    }

    @Test
    void listPathKeepsNullLeafMembers() {
        SimpleFeature feature = featureThree();
        Object result = accessor().get(feature, "addresses/locality", Object.class);
        assertThat(result).isInstanceOf(List.class);
        List<Object> localities = asObjectList(result);
        assertThat(localities).hasSize(2);
        assertThat(localities).contains("NYC");
        assertThat(localities).containsNull();
    }

    @Test
    void emptyListPathReturnsEmptyList() {
        SimpleFeature feature = featureTwo();
        Object result = accessor().get(feature, "addresses/locality", Object.class);
        assertThat(result).isInstanceOf(List.class);
        assertThat((List<?>) result).isEmpty();
    }

    @Test
    void singleValuedStructPathReturnsScalar() {
        SimpleFeature feature = featureOne();
        Object result = accessor().get(feature, "brand/name", Object.class);
        assertThat(result).isEqualTo("Acme");
    }

    @Test
    void canHandleRejectsForeignFeaturesAndNonSlashPaths() {
        SimpleFeature foreign = foreignFeature();
        assertThat(accessor().canHandle(foreign, "addresses/locality", null)).isFalse();
        assertThat(accessor().canHandle(featureOne(), "id", null)).isFalse();
    }

    @Test
    void residualAnyMatchesAcrossFeatures() {
        assertThat(anyLocalityEquals(featureOne(), "NYC")).isTrue();
        assertThat(anyLocalityEquals(featureTwo(), "NYC")).isFalse();
        assertThat(anyLocalityEquals(featureThree(), "NYC")).isTrue();
    }

    @Test
    void residualAllTreatsNullMemberAsNonMatch() {
        assertThat(allLocalityEquals(featureThree(), "NYC")).isFalse();
    }

    private boolean anyLocalityEquals(SimpleFeature feature, String literal) {
        return FF.equal(FF.property("addresses/locality"), FF.literal(literal), true, MatchAction.ANY)
                .evaluate(feature);
    }

    private boolean allLocalityEquals(SimpleFeature feature, String literal) {
        return FF.equal(FF.property("addresses/locality"), FF.literal(literal), true, MatchAction.ALL)
                .evaluate(feature);
    }

    @SuppressWarnings("unchecked")
    private List<Object> asObjectList(Object value) {
        return (List<Object>) value;
    }

    private PropertyAccessor accessor() {
        return new NestedPropertyAccessorFactory().createPropertyAccessor(SimpleFeature.class, "x/y", null, null);
    }

    private SimpleFeature featureOne() {
        TypedList addresses = addresses(address("NYC", "10001"), address("LA", "90001"));
        return nestedFeature("1", addresses, brand("Acme"));
    }

    private SimpleFeature featureTwo() {
        TypedList addresses = addresses();
        return nestedFeature("2", addresses, brand("Globex"));
    }

    private SimpleFeature featureThree() {
        TypedList addresses = addresses(address("NYC", "10001"), address(null, "00000"));
        return nestedFeature("3", addresses, brand("Initech"));
    }

    private SimpleFeature nestedFeature(String id, TypedList addresses, Struct brand) {
        SimpleFeatureType type = nestedType();
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(type);
        builder.set("id", id);
        builder.set("addresses", addresses);
        builder.set("brand", brand);
        return builder.buildFeature(id);
    }

    private SimpleFeatureType nestedType() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("nested");
        typeBuilder.add("id", String.class);
        typeBuilder.userData(NestedType.USER_DATA_KEY, ADDRESSES_TYPE);
        typeBuilder.add("addresses", List.class);
        typeBuilder.userData(NestedType.USER_DATA_KEY, BRAND_TYPE);
        typeBuilder.add("brand", Map.class);
        return typeBuilder.buildFeatureType();
    }

    private SimpleFeature foreignFeature() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("foreign");
        typeBuilder.add("id", String.class);
        typeBuilder.add("addresses", List.class);
        SimpleFeatureType type = typeBuilder.buildFeatureType();
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(type);
        builder.set("id", "f");
        builder.set("addresses", List.of(Map.of("locality", "NYC")));
        return builder.buildFeature("f");
    }

    private TypedList addresses(Struct... entries) {
        List<Object> backing = new ArrayList<>(Arrays.asList(entries));
        return new TypedList(ADDRESS_TYPE, backing);
    }

    private Struct address(String locality, String postcode) {
        return new Struct(ADDRESS_TYPE, new Object[] {locality, postcode});
    }

    private Struct brand(String name) {
        return new Struct(BRAND_TYPE, new Object[] {name});
    }
}
