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
 * Spatial pruning-tier accessor.
 *
 * <p>{@link io.tileverse.parquetry.filter.spatial.SpatialBoundsSource} is the single entry point: it answers "what
 * bounding box does column X cover (in this file, or in row group N)?" against whichever of the three GeoParquet bounds
 * sources the file actually carries. The GeoTools datastore uses it today for layer-extent aggregation; a future
 * in-pipeline row-group pruner will consume the same accessor.
 */
package io.tileverse.parquetry.filter.spatial;
