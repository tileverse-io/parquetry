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

import org.geotools.api.filter.Id;
import org.geotools.api.filter.PropertyIsBetween;
import org.geotools.api.filter.PropertyIsNull;
import org.geotools.api.filter.spatial.BBOX;
import org.geotools.api.filter.spatial.Contains;
import org.geotools.api.filter.spatial.Crosses;
import org.geotools.api.filter.spatial.DWithin;
import org.geotools.api.filter.spatial.Disjoint;
import org.geotools.api.filter.spatial.Equals;
import org.geotools.api.filter.spatial.Intersects;
import org.geotools.api.filter.spatial.Overlaps;
import org.geotools.api.filter.spatial.Touches;
import org.geotools.api.filter.spatial.Within;
import org.geotools.filter.Capabilities;

/**
 * Declares the GeoTools filter constructs that {@link QueryTranslator} attempts to push down to parquetry.
 *
 * <p>This is the COARSE pre/post split fed to {@link org.geotools.filter.visitor.CapabilitiesFilterSplitter}: a filter
 * leaf is a pre-filter candidate when its operator appears here. The split is intentionally conservative. Advertising
 * an operator does not promise an exact translation; {@link FilterToPredicate} runs afterward and pushes back to the
 * residual anything it cannot express (an unbound property, a non-literal operand, a mismatched binding). Operators
 * that parquetry never pushes - {@code PropertyIsLike}, functions, {@code In} - are deliberately omitted here.
 *
 * <p>{@code Id} is advertised so {@link FilterToPredicate} can lower it to an {@code IN} on the feature id column; it
 * still rides the residual for exact membership.
 */
final class PushdownFilterCapabilities {

    private PushdownFilterCapabilities() {}

    static Capabilities create() {
        Capabilities capabilities = new Capabilities();
        capabilities.addAll(Capabilities.LOGICAL_OPENGIS);
        capabilities.addAll(Capabilities.SIMPLE_COMPARISONS_OPENGIS);
        capabilities.addType(PropertyIsBetween.class);
        capabilities.addType(PropertyIsNull.class);
        capabilities.addType(Id.class);
        addSpatialOperators(capabilities);
        return capabilities;
    }

    private static void addSpatialOperators(Capabilities capabilities) {
        capabilities.addType(BBOX.class);
        capabilities.addType(Intersects.class);
        capabilities.addType(Contains.class);
        capabilities.addType(Within.class);
        capabilities.addType(Crosses.class);
        capabilities.addType(Overlaps.class);
        capabilities.addType(Touches.class);
        capabilities.addType(Disjoint.class);
        capabilities.addType(Equals.class);
        capabilities.addType(DWithin.class);
    }
}
