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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.MultiValuedFilter.MatchAction;
import org.geotools.api.filter.PropertyIsEqualTo;
import org.geotools.api.filter.expression.PropertyName;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.expression.PropertyAccessor;
import org.geotools.filter.expression.PropertyAccessors;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.Test;

/**
 * Regression tests that pin the GeoTools runtime behaviors the nested-attributes design depends on. A future GeoTools
 * upgrade that changes any of these contracts fails here loudly rather than silently breaking the design.
 */
class NestedAttributeProbesTest {

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    /**
     * GeoTools applies {@link MatchAction} over a collection-valued {@link PropertyName} with the same semantics core's
     * quantified pushdown uses. Core pushdown and the GeoTools residual filter therefore agree on every row.
     */
    @Test
    void geotoolsAppliesMatchActionOverCollections() {
        SimpleFeature abc = featureWithVals(List.of("a", "b", "c"));
        assertThat(eval(abc, "b", MatchAction.ANY)).isTrue();
        assertThat(eval(abc, "z", MatchAction.ANY)).isFalse();
        assertThat(eval(abc, "b", MatchAction.ALL)).isFalse();
        assertThat(eval(abc, "b", MatchAction.ONE)).isTrue();

        SimpleFeature bbb = featureWithVals(List.of("b", "b", "b"));
        assertThat(eval(bbb, "b", MatchAction.ALL)).isTrue();

        SimpleFeature bbc = featureWithVals(List.of("b", "b", "c"));
        assertThat(eval(bbc, "b", MatchAction.ONE)).isFalse();

        SimpleFeature empty = featureWithVals(List.of());
        assertThat(eval(empty, "b", MatchAction.ANY)).isFalse();
        assertThat(eval(empty, "b", MatchAction.ALL)).isTrue();
        assertThat(eval(empty, "b", MatchAction.ONE)).isFalse();

        SimpleFeature aNull = featureWithVals(Arrays.asList("a", null));
        assertThat(eval(aNull, "a", MatchAction.ANY)).isTrue();
        assertThat(eval(aNull, "a", MatchAction.ALL)).isFalse();
        assertThat(eval(aNull, "a", MatchAction.ONE)).isTrue();
    }

    /**
     * No base-classpath {@link PropertyAccessor} claims a nested path. A simple top-level attribute is claimed and
     * reads back its collection value, but dotted and slashed nested names go unclaimed, which is what lets the
     * design's HIGHEST_PRIORITY accessor win for those paths later.
     */
    @Test
    void noBaseAccessorClaimsNestedPaths() {
        SimpleFeature feature = featureWithNestedAttributes();

        List<PropertyAccessor> topLevel = findAccessors(feature, "addresses");
        assertThat(topLevel).isNotEmpty();
        assertThat(topLevel.get(0).get(feature, "addresses", Object.class)).isInstanceOf(List.class);

        assertThat(findAccessors(feature, "addresses/locality")).isEmpty();
        assertThat(findAccessors(feature, "addresses.locality")).isEmpty();
        assertThat(findAccessors(feature, "categories/primary")).isEmpty();
        assertThat(findAccessors(feature, "categories.primary")).isEmpty();
        assertThat(findAccessors(feature, "missing/locality")).isEmpty();
    }

    /**
     * ECQL normalizes an unquoted dotted name to a slash-separated path, while a double-quoted name stays a literal
     * dotted property name. The design relies on this to tell a nested traversal apart from a single attribute whose
     * name happens to contain dots.
     */
    @Test
    void ecqlNormalizesUnquotedDottedToSlash() throws Exception {
        assertThat(propertyNameOf("categories.primary = 'x'")).isEqualTo("categories/primary");
        assertThat(propertyNameOf("\"categories.primary\" = 'x'")).isEqualTo("categories.primary");
        assertThat(propertyNameOf("brand.names.primary = 'x'")).isEqualTo("brand/names/primary");
        assertThat(propertyNameOf("\"addresses.list.element.locality\" = 'x'"))
                .isEqualTo("addresses.list.element.locality");
    }

    private boolean eval(SimpleFeature feature, String target, MatchAction matchAction) {
        PropertyIsEqualTo filter = FF.equal(FF.property("vals"), FF.literal(target), true, matchAction);
        return filter.evaluate(feature);
    }

    private List<PropertyAccessor> findAccessors(SimpleFeature feature, String path) {
        List<PropertyAccessor> accessors = PropertyAccessors.findPropertyAccessors(feature, path, null, null);
        return accessors == null ? List.of() : accessors;
    }

    private String propertyNameOf(String cql) throws Exception {
        Filter filter = ECQL.toFilter(cql);
        PropertyIsEqualTo equals = (PropertyIsEqualTo) filter;
        PropertyName propertyName = (PropertyName) equals.getExpression1();
        return propertyName.getPropertyName();
    }

    private SimpleFeature featureWithVals(List<?> vals) {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("probe");
        typeBuilder.add("id", String.class);
        typeBuilder.add("vals", List.class);
        SimpleFeatureType type = typeBuilder.buildFeatureType();
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(type);
        featureBuilder.set("id", "f1");
        featureBuilder.set("vals", vals);
        return featureBuilder.buildFeature("f1");
    }

    private SimpleFeature featureWithNestedAttributes() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("nested");
        typeBuilder.add("id", String.class);
        typeBuilder.add("addresses", List.class);
        typeBuilder.add("categories", Map.class);
        SimpleFeatureType type = typeBuilder.buildFeatureType();
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(type);
        featureBuilder.set("id", "f1");
        featureBuilder.set("addresses", List.of(Map.of("locality", "NYC")));
        featureBuilder.set("categories", Map.of("primary", "food"));
        return featureBuilder.buildFeature("f1");
    }
}
