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

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A PROJJSON geodetic reference frame (datum): the ellipsoid and prime meridian that anchor a {@link GeographicCRS} or
 * {@link GeodeticCRS} to the Earth.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GeodeticReferenceFrame(
        Optional<String> name,
        Ellipsoid ellipsoid,
        @JsonProperty("prime_meridian") Optional<PrimeMeridian> primeMeridian,
        Optional<Identifier> id) {}
