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
 * Apache Iceberg GeoTools DataStore over a warehouse-based catalog, plus its {@code DataStoreFactorySpi}. One
 * {@code iceberg} URI opens either a single table or a warehouse of tables (one layer per table). The store is a thin
 * subclass of the shared catalog-backed store core in {@link io.tileverse.parquetry.geotools.data}; the future
 * REST-catalog-backed store will be a separate implementation.
 */
package io.tileverse.parquetry.geotools.iceberg;
