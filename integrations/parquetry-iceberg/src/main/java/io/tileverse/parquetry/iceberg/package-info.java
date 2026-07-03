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
 * A clean-room Apache Iceberg table reader on the parquetry dataset/catalog SPI, with no dependency on
 * {@code iceberg-core}, {@code apache-avro}, or Hadoop.
 *
 * <p>The public API is small: {@link io.tileverse.parquetry.iceberg.IcebergTableCatalog} opens one table at a location
 * (with {@link io.tileverse.parquetry.iceberg.IcebergOptions}) and exposes its snapshot as one
 * {@link io.tileverse.parquetry.catalog.DatasetCatalog} / {@link io.tileverse.parquetry.dataset.ParquetDataset}.
 * {@link io.tileverse.parquetry.iceberg.IcebergWarehouseCatalog} opens a warehouse of many tables - discovered under a
 * warehouse root, or named by an explicit registry - as one dataset per table; a REST catalog is the planned third
 * member of the family. Data-file bytes come through an {@link io.tileverse.parquetry.iceberg.IcebergFileIO}:
 * {@link io.tileverse.parquetry.iceberg.StorageIcebergFileIO} serves bytes over a tileverse
 * {@link io.tileverse.storage.Storage} for local table directories and remote object stores (S3, Azure, GCS, HTTP)
 * alike. Unsupported inputs fail fast with an {@link io.tileverse.parquetry.iceberg.IcebergFormatException}. Everything
 * else in this package is package-private implementation detail.
 *
 * <p>Internally the catalog resolves the table from its metadata, pins a snapshot, follows the manifest list and data
 * manifests (read through the clean-room {@code parquetry-avro} reader), and prunes data files by their manifest bounds
 * before reading: the numeric (Iceberg single-value serialization) and geometry ({@code packed_xy} and
 * {@code wkb_point}) bounds are decoded into a core {@code FileStats} per file, and a file a predicate rules out is
 * skipped. Pruning never changes results; a kept file is still filtered at row-group and record level.
 *
 * <p>Copy-on-write and merge-on-read tables both read: v2 positional and equality deletes and v3 deletion vectors are
 * applied during the scan, and a deleted row is never returned. Schema evolution resolves top-level fields by id
 * (rename, add, drop, reorder, {@code int}-to-{@code long} and {@code float}-to-{@code double} promotion). The module
 * README tracks the remaining gaps feature by feature.
 */
package io.tileverse.parquetry.iceberg;
