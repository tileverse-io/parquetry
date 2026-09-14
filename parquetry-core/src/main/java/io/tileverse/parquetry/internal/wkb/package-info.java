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
/**
 * The WKB (Well-Known Binary) wire walker shared by the JTS reader and the envelope computation.
 *
 * <p>{@link io.tileverse.parquetry.internal.wkb.WkbCursor} is the single entry point: it decodes a geometry's header,
 * checks every count against the value's byte range, and exposes each coordinate run as a
 * {@link io.tileverse.parquetry.internal.wkb.CoordinateRun} that a consumer folds in place or bulk-copies. It has no
 * JTS dependency: {@code io.tileverse.parquetry.geo.MemorySegmentWkbReader} builds JTS geometries over it, and
 * {@code io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope} computes envelopes and bbox relations over it.
 */
package io.tileverse.parquetry.internal.wkb;
