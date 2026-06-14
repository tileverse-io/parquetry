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
package io.tileverse.parquetry.geotools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.MultiValuedFilter.MatchAction;
import org.geotools.api.filter.PropertyIsEqualTo;
import org.geotools.api.filter.expression.Expression;
import org.geotools.api.filter.expression.PropertyName;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.expression.PropertyAccessor;
import org.geotools.filter.expression.PropertyAccessors;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.Test;

/**
 * Exploratory spikes for the nested-attributes design. Each method prints what the GeoTools runtime actually does so
 * the implementation plan can be finalized against observed behavior rather than assumptions. Not assertions: the
 * output is read by hand.
 */
class NestedAttributeProbesTest {

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    @Test
    void probeA_matchActionOverCollections() {
        log("=== PROBE A: does GeoTools apply MatchAction over a collection-valued PropertyName? ===");
        SimpleFeature multi = featureWithVals(List.of("a", "b", "c"));
        SimpleFeature singleMatch = featureWithVals(List.of("b"));
        SimpleFeature allMatch = featureWithVals(List.of("b", "b", "b"));
        SimpleFeature twoMatches = featureWithVals(List.of("b", "b", "c"));
        SimpleFeature empty = featureWithVals(List.of());
        SimpleFeature withNull = featureWithVals(Arrays.asList("a", null, "b"));

        log("ANY [a,b,c]=b      -> " + eval(multi, "b", MatchAction.ANY) + "   (expect core: true)");
        log("ANY [a,b,c]=z      -> " + eval(multi, "z", MatchAction.ANY) + "   (expect core: false)");
        log("ALL [a,b,c]=b      -> " + eval(multi, "b", MatchAction.ALL) + "   (expect core: false)");
        log("ALL [b,b,b]=b      -> " + eval(allMatch, "b", MatchAction.ALL) + "   (expect core: true)");
        log("ONE [a,b,c]=b      -> " + eval(multi, "b", MatchAction.ONE) + "   (expect core: true, 1 match)");
        log("ONE [b,b,c]=b      -> " + eval(twoMatches, "b", MatchAction.ONE) + "   (expect core: false, 2 matches)");
        log("ANY []=b           -> " + eval(empty, "b", MatchAction.ANY) + "   (expect core: false)");
        log("ALL []=b           -> " + eval(empty, "b", MatchAction.ALL) + "   (expect core: true, vacuous)");
        log("ONE []=b           -> " + eval(empty, "b", MatchAction.ONE) + "   (expect core: false)");
        log("ANY [a,null,b]=b   -> " + eval(withNull, "b", MatchAction.ANY) + "   (null handling)");
        log("ALL [a,null,b]=a   -> " + eval(withNull, "a", MatchAction.ALL) + "   (non-discriminating: b!=a anyway)");
        SimpleFeature aNull = featureWithVals(Arrays.asList("a", null));
        log("ALL [a,null]=a     -> " + eval(aNull, "a", MatchAction.ALL)
                + "   (core and GT agree: a null member is a non-match -> false)");
        log("ONE [a,null]=a     -> " + eval(aNull, "a", MatchAction.ONE) + "   (core: 1 match -> true)");
    }

    @Test
    void probeB_accessorPrecedenceForDottedPaths() {
        log("=== PROBE B: which PropertyAccessors claim nested paths when the TOP attribute exists? ===");
        SimpleFeature feature = featureWithNestedAttributes();
        for (String path : List.of(
                "addresses",
                "addresses/locality",
                "addresses.locality",
                "categories/primary",
                "categories.primary",
                "missing/locality")) {
            describeAccessors(feature, path);
        }
    }

    private SimpleFeature featureWithNestedAttributes() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("nested");
        typeBuilder.add("id", String.class);
        typeBuilder.add("addresses", List.class);
        typeBuilder.add("categories", java.util.Map.class);
        SimpleFeatureType type = typeBuilder.buildFeatureType();
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(type);
        featureBuilder.set("id", "f1");
        featureBuilder.set("addresses", List.of(java.util.Map.of("locality", "NYC")));
        featureBuilder.set("categories", java.util.Map.of("primary", "food"));
        return featureBuilder.buildFeature("f1");
    }

    @Test
    void probeC_ecqlDottedVsQuoted() throws Exception {
        log("=== PROBE C: ECQL parse of unquoted dotted vs double-quoted nested name ===");
        describeEcql("categories.primary = 'x'");
        describeEcql("\"categories.primary\" = 'x'");
        describeEcql("brand.names.primary = 'x'");
        describeEcql("\"addresses.list.element.locality\" = 'x'");
    }

    private boolean eval(SimpleFeature feature, String target, MatchAction matchAction) {
        PropertyIsEqualTo filter = FF.equal(FF.property("vals"), FF.literal(target), true, matchAction);
        return filter.evaluate(feature);
    }

    private void describeAccessors(SimpleFeature feature, String path) {
        List<PropertyAccessor> accessors = PropertyAccessors.findPropertyAccessors(feature, path, null, null);
        if (accessors == null || accessors.isEmpty()) {
            log("path='" + path + "' -> NO accessor claims it");
            return;
        }
        List<String> described = new ArrayList<>();
        for (PropertyAccessor accessor : accessors) {
            described.add(accessor.getClass().getSimpleName() + "[" + readValue(accessor, feature, path) + "]");
        }
        log("path='" + path + "' -> " + described);
    }

    private String readValue(PropertyAccessor accessor, SimpleFeature feature, String path) {
        try {
            Object value = accessor.get(feature, path, Object.class);
            return "get=" + value;
        } catch (RuntimeException e) {
            return "get threw " + e.getClass().getSimpleName();
        }
    }

    private void describeEcql(String cql) throws Exception {
        PropertyIsEqualTo filter = (PropertyIsEqualTo) ECQL.toFilter(cql);
        log("ECQL " + cql);
        log("     expr1=" + describeExpression(filter.getExpression1()) + "  expr2="
                + describeExpression(filter.getExpression2()));
    }

    private String describeExpression(Expression expression) {
        String type = expression.getClass().getSimpleName();
        if (expression instanceof PropertyName propertyName) {
            return type + "(propertyName='" + propertyName.getPropertyName() + "')";
        }
        return type + "(" + expression + ")";
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

    private void log(String message) {
        System.out.println("[PROBE] " + message);
    }
}
