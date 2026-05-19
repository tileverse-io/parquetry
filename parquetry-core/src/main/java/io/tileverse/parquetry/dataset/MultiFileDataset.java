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
package io.tileverse.parquetry.dataset;

/**
 * Extension point for partitioned multi-file datasets (Hive-style directory layouts, Iceberg / Delta manifests).
 *
 * <p>This ships zero implementations: the interface exists to preserve the public sealing seam in
 * {@link ParquetDataset} so the future multi-file spec can grow under here without breaking {@code permits} clauses.
 * Until then, the only way to land a ParquetDataset implementation is {@link FileDataset}.
 *
 * <p>{@code ParquetDataset} is {@code sealed permits FileDataset, MultiFileDataset}, so this interface must carry
 * either {@code sealed} or {@code non-sealed}. Java rejects {@code sealed} with zero permitted subtypes, and it has no
 * concrete multi-file implementation to name; {@code non-sealed} is therefore the only valid declaration here. Future
 * future work that introduces a concrete multi-file type should tighten this back to {@code sealed permits X}.
 */
public non-sealed interface MultiFileDataset extends ParquetDataset {}
