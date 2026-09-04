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
 * Exports GeoTools feature collections to GeoParquet files and Parquet record-batch streams.
 *
 * <p>This is a one-way encoding path: it turns a GeoTools {@code FeatureCollection} into Parquet output and nothing
 * more. The datastores in {@code io.tileverse.parquetry.geotools.data} remain read-only, and nothing in this package
 * implements the GeoTools {@code FeatureWriter} or transaction contracts.
 */
package io.tileverse.parquetry.geotools.export;
