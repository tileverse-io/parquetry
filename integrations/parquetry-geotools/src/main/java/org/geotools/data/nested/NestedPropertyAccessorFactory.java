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

import org.geotools.filter.expression.PropertyAccessor;
import org.geotools.filter.expression.PropertyAccessorFactory;
import org.geotools.util.factory.Hints;

/**
 * Hands GeoTools a {@link NestedPropertyAccessor} for slash-separated nested paths such as {@code addresses/locality},
 * letting residual filters and non-filter expressions (SLD, transforms, GetFeatureInfo) navigate into the materialized
 * value objects of a nested attribute.
 *
 * <p>The factory runs at {@link PropertyAccessorFactory#HIGHEST_PRIORITY}, ahead of the default
 * {@code SimpleFeaturePropertyAccessor} and any broad SimpleFeature claimer on a GeoServer classpath. It claims only
 * slash paths; the accessor's {@code canHandle} narrows that to features whose head attribute is a nested type, leaving
 * top-level names and foreign object types to the rest of the accessor chain.
 */
public final class NestedPropertyAccessorFactory implements PropertyAccessorFactory {

    private static final NestedPropertyAccessor ACCESSOR = new NestedPropertyAccessor();

    @Override
    public int getPriority() {
        return HIGHEST_PRIORITY;
    }

    @Override
    public PropertyAccessor createPropertyAccessor(Class<?> type, String xpath, Class<?> target, Hints hints) {
        if (isNestedPath(xpath)) {
            return ACCESSOR;
        }
        return null;
    }

    private static boolean isNestedPath(String xpath) {
        return xpath != null && xpath.indexOf('/') >= 0;
    }
}
