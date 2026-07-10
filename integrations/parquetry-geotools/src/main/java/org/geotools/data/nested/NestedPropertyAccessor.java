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
package org.geotools.data.nested;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.filter.expression.PropertyAccessor;

/**
 * Navigates a slash-separated path such as {@code addresses/locality} into the materialized nested value objects of a
 * {@link SimpleFeature} attribute, returning the result of residual filter evaluation and non-filter expression
 * evaluation (SLD, transforms, GetFeatureInfo).
 *
 * <p>The navigation mirrors parquetry core's {@code MultiValues} so an in-memory residual answer agrees with what the
 * push-down tier computed. A {@code STRUCT} ({@link Struct}) and a {@code MAP} ({@link TypedMap}) read by key; a
 * {@code LIST} ({@link TypedList}) is a repeated boundary: the remaining path is navigated into every element and the
 * per-element results are flattened into a single list. Null members are kept in the flattened list, which is what
 * GeoTools' {@code MatchAction} (ANY/ALL/ONE) and the core quantified universe expect: a present struct element whose
 * leaf is null, and a null list element, each contribute a {@code null} entry rather than vanishing.
 */
final class NestedPropertyAccessor implements PropertyAccessor {

    private static final char SEGMENT_SEPARATOR = '/';

    @Override
    public boolean canHandle(Object object, String xpath, Class<?> target) {
        if (!(object instanceof SimpleFeature feature)) {
            return false;
        }
        if (!isNestedPath(xpath)) {
            return false;
        }
        return headIsNestedAttribute(feature, headSegment(xpath));
    }

    @Override
    public <T> T get(Object object, String xpath, Class<T> target) {
        SimpleFeature feature = (SimpleFeature) object;
        String[] segments = split(xpath);
        Object head = feature.getAttribute(segments[0]);
        Object result = walk(head, segments, 1);
        return cast(result, target);
    }

    @Override
    public <T> void set(Object object, String xpath, T value, Class<T> target) {
        throw new UnsupportedOperationException("nested attributes are read-only");
    }

    /**
     * Walks the path segments from {@code index} into {@code current}, returning the scalar reached, or a flattened
     * {@link List} when a list boundary is crossed. Returns {@code current} once the path is exhausted.
     */
    private Object walk(Object current, String[] segments, int index) {
        if (index == segments.length) {
            return current;
        }
        if (current instanceof List<?> list) {
            return flattenOverList(list, segments, index);
        }
        if (current instanceof Map<?, ?> map) {
            return walk(map.get(segments[index]), segments, index + 1);
        }
        return null;
    }

    /**
     * Navigates the remaining path into each list element and flattens the results, keeping a {@code null} entry for an
     * absent member (a null element or a present element whose leaf field is null).
     */
    private Object flattenOverList(List<?> list, String[] segments, int index) {
        List<Object> flattened = new ArrayList<>(list.size());
        for (Object element : list) {
            flattened.add(walk(element, segments, index));
        }
        return flattened;
    }

    @SuppressWarnings("unchecked")
    private <T> T cast(Object value, Class<T> target) {
        if (target == null || target == Object.class) {
            return (T) value;
        }
        if (value == null || target.isInstance(value)) {
            return (T) value;
        }
        return null;
    }

    private boolean headIsNestedAttribute(SimpleFeature feature, String head) {
        SimpleFeatureType type = feature.getFeatureType();
        AttributeDescriptor descriptor = type.getDescriptor(head);
        if (descriptor == null) {
            return false;
        }
        return descriptor.getUserData().containsKey(NestedType.USER_DATA_KEY);
    }

    private static boolean isNestedPath(String xpath) {
        return xpath != null && xpath.indexOf(SEGMENT_SEPARATOR) >= 0;
    }

    private static String headSegment(String xpath) {
        return xpath.substring(0, xpath.indexOf(SEGMENT_SEPARATOR));
    }

    private static String[] split(String xpath) {
        return xpath.split(String.valueOf(SEGMENT_SEPARATOR));
    }
}
