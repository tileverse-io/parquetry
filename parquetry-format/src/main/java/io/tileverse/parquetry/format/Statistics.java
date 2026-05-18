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

import java.util.Optional;

/**
 * Per-column statistics carried in {@link ColumnMetaData}.
 *
 * <p>The (deprecated) {@code max}/{@code min} byte arrays are the legacy fields written
 * by older parquet writers; modern writers use {@code maxValue}/{@code minValue} which
 * obey the column's {@link ColumnOrder}. Both pairs are optional.
 */
public record Statistics(
        Optional<byte[]> max,
        Optional<byte[]> min,
        Optional<Long> nullCount,
        Optional<Long> distinctCount,
        Optional<byte[]> maxValue,
        Optional<byte[]> minValue,
        Optional<Boolean> isMaxValueExact,
        Optional<Boolean> isMinValueExact) {}
