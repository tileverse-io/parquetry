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
 * Logical dataset view over one or more Parquet files of a single schema.
 *
 * <p>A {@link io.tileverse.parquetry.dataset.ParquetDataset} reads 1..N files as one stream; it sits above the
 * single-file {@code io.tileverse.parquetry.data.ParquetReader} engine in {@code parquetry-core}.
 */
package io.tileverse.parquetry.dataset;
