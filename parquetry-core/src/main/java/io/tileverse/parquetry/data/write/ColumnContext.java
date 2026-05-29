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
package io.tileverse.parquetry.data.write;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.data.write.page.PageWriter;
import io.tileverse.parquetry.schema.PrimitiveKind;

import lombok.NonNull;

/**
 * Per-column configuration the {@link PageWriter} needs to build page headers and resolve the codec.
 *
 * <p>{@link #maxRepetitionLevel()} and {@link #maxDefinitionLevel()} are zero for flat (non-repeated, required)
 * primitive columns and grow with the depth of {@code REPEATED} / {@code OPTIONAL} ancestors in the schema.
 * {@link #kind()} drives the page-level statistics encoding choice and is the column's physical Parquet type.
 * {@link #parquetVersion()} selects {@code DATA_PAGE} versus {@code DATA_PAGE_V2} layout. {@link #compression()} is the
 * codec configuration the writer compresses page payloads with -- either the column-specific override or, in its
 * absence, the {@link WriteOptions#defaultCompression()} fallback.
 *
 * @param maxRepetitionLevel max repetition level for this column ({@code 0} for flat columns)
 * @param maxDefinitionLevel max definition level for this column ({@code 0} for {@code REQUIRED} flat columns,
 *     {@code 1} for {@code OPTIONAL} flat columns)
 * @param kind physical Parquet type of the column
 * @param parquetVersion page layout the writer should emit
 * @param compression codec configuration used to compress this column's page payloads
 */
public record ColumnContext(
        int maxRepetitionLevel,
        int maxDefinitionLevel,
        @NonNull PrimitiveKind kind,
        @NonNull ParquetVersion parquetVersion,
        @NonNull Compression compression) {

    public ColumnContext {
        if (maxRepetitionLevel < 0) {
            throw new IllegalArgumentException("maxRepetitionLevel must be non-negative; got " + maxRepetitionLevel);
        }
        if (maxDefinitionLevel < 0) {
            throw new IllegalArgumentException("maxDefinitionLevel must be non-negative; got " + maxDefinitionLevel);
        }
    }
}
