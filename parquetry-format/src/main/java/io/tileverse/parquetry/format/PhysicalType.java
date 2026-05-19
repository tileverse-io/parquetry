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
 * On-disk storage type for a leaf column; mirror of {@code Type} in {@code parquet.thrift}.
 *
 * <p>The "physical" qualifier distinguishes this from {@link LogicalType}, which annotates a {@link PhysicalType} with
 * higher-level semantics (date, decimal, UUID, geography, etc.). {@link #INT96} is deprecated and only emitted for
 * legacy timestamp interop.
 */
public enum PhysicalType {
    BOOLEAN,
    INT32,
    INT64,
    INT96,
    FLOAT,
    DOUBLE,
    BYTE_ARRAY,
    FIXED_LEN_BYTE_ARRAY
}
