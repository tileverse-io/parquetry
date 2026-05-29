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
/**
 * Typed Java records mirroring the PROJJSON encoding of WKT2:2019 coordinate reference systems.
 *
 * <p>PROJJSON is the wire format the GeoParquet 2.0 native logical type carries inline, and the format GeoParquet 1.x
 * uses inside its {@code "geo"} key-value metadata. This package models the subset of PROJJSON that real-world
 * GeoParquet producers emit (Overture, ESA, OGC fixtures, the geotools test corpus): the six CRS variants permitted by
 * {@link io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem}, plus the supporting records they
 * reference (Identifier, Ellipsoid, CoordinateSystem, Axis, etc.).
 *
 * <h2>Forward compatibility</h2>
 *
 * <p>Every record is annotated {@code @JsonIgnoreProperties(ignoreUnknown = true)} so PROJJSON additions in later spec
 * versions read through without source changes. A CRS document whose {@code type} discriminator is one this package
 * does not yet model surfaces as {@link io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem.Unknown}
 * with the raw JSON preserved.
 *
 * <h2>Module registration</h2>
 *
 * <p>{@link io.tileverse.parquetry.schema.geo.projjson.ProjJsonModule} is <em>not</em> auto-registered via
 * {@code META-INF/services}. Library-internal mappers register it by hand; downstream consumers calling
 * {@code mapper.findAndRegisterModules()} do not pick it up. This avoids re-interpreting unrelated PROJJSON or
 * {@code "geo"}-keyed JSON inside Spring, JAX-RS, or other shared {@code ObjectMapper} setups.
 */
package io.tileverse.parquetry.schema.geo.projjson;
