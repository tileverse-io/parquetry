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
 * <p>The public API is small: {@link io.tileverse.parquetry.iceberg.IcebergCatalog} opens a table (with
 * {@link io.tileverse.parquetry.iceberg.IcebergOptions}) and exposes its snapshot as one
 * {@link io.tileverse.parquetry.catalog.DatasetCatalog} / {@link io.tileverse.parquetry.dataset.ParquetDataset}.
 * Data-file bytes come through an {@link io.tileverse.parquetry.iceberg.IcebergFileIO};
 * {@link io.tileverse.parquetry.iceberg.LocalIcebergFileIO} serves a local table directory, and a storage backend
 * implements the same interface for cloud storage. Unsupported inputs fail fast with an
 * {@link io.tileverse.parquetry.iceberg.IcebergFormatException}. Everything else in this package is package-private
 * implementation detail.
 *
 * <p>Internally the catalog resolves the table from its metadata, pins a snapshot, follows the manifest list and data
 * manifests (read through the clean-room {@code parquetry-avro} reader), and prunes data files by their manifest bounds
 * before reading: the numeric (Iceberg single-value serialization) and geometry ({@code packed_xy} and
 * {@code wkb_point}) bounds are decoded into a core {@code FileStats} per file, and a file a predicate rules out is
 * skipped. Pruning never changes results; a kept file is still filtered at row-group and record level.
 *
 * <p>Copy-on-write tables read; delete manifests and non-empty partition specs fail fast. Field-id-resolved projection,
 * schema evolution, merge-on-read, and cloud storage are future work; see the module README.
 */
package io.tileverse.parquetry.iceberg;
