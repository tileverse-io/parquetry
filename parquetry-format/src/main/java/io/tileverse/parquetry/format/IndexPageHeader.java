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
package io.tileverse.parquetry.format;

/**
 * Placeholder for the {@code IndexPageHeader} struct in {@code parquet.thrift}.
 *
 * <p>Reserved in the spec but currently empty; page-level indexing is delivered by {@link ColumnIndex} and
 * {@link OffsetIndex} instead. Kept here so the {@link PageType#INDEX_PAGE} branch of {@link PageHeader} has a target.
 */
@SuppressWarnings("java:S2094")
public record IndexPageHeader() {}
