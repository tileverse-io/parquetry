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
 * Catalog abstraction: the {@link io.tileverse.parquetry.catalog.DatasetCatalog} SPI and
 * {@link io.tileverse.parquetry.catalog.CatalogCapabilities}, with
 * {@link io.tileverse.parquetry.catalog.FilesetCatalog} resolving the files of a
 * {@link io.tileverse.parquetry.io.FileSource} into one merged {@link io.tileverse.parquetry.dataset.ParquetDataset}.
 */
package io.tileverse.parquetry.catalog;
