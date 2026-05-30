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
package io.tileverse.parquetry.batch;

import java.util.BitSet;

import io.tileverse.parquetry.data.variant.Variant;
import io.tileverse.parquetry.data.variant.VariantMetadata;

import lombok.NonNull;

/**
 * A column vector for Parquet Variant columns: the per-row metadata and value binary leaves plus a validity mask.
 * Reading a row builds a {@link Variant} navigator over that row's two segments.
 */
public record VariantVector(
        @NonNull BinaryVector metadataColumn,
        @NonNull BinaryVector valueColumn,
        @NonNull BitSet validity,
        int size) implements ColumnVector {

    public Variant variantAt(int row) {
        VariantMetadata metadata = new VariantMetadata(metadataColumn.get(row));
        return Variant.of(valueColumn.get(row), metadata);
    }
}
