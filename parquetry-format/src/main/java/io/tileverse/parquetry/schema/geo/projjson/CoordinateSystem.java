/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.schema.geo.projjson;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A PROJJSON coordinate system: the ordered set of {@link Axis}es a CRS uses. {@link #subtype()} carries the schema's
 * coordinate-system subtype identifier (commonly {@code "ellipsoidal"}, {@code "Cartesian"}, or {@code "vertical"}).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CoordinateSystem(Optional<String> subtype, List<Axis> axes) {

    public CoordinateSystem {
        axes = axes == null ? List.of() : List.copyOf(axes);
    }
}
