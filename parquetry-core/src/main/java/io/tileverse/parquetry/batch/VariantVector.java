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
        @NonNull Validity validity,
        int size) implements ColumnVector {

    @Override
    @SuppressWarnings("unchecked")
    public Variant get(int row) {
        if (validity.isNull(row)) {
            return null;
        }
        VariantMetadata metadata = new VariantMetadata(metadataColumn.get(row));
        return Variant.of(valueColumn.get(row), metadata);
    }

    /**
     * A selected Variant propagates the selection into its row-parallel metadata and value columns rather than holding
     * it: both columns are one binary value per Variant row; selecting Variant rows selects the same rows in each. The
     * validity is projected and the size reduced to the selection length.
     */
    @Override
    public ColumnVector select(Selection selection) {
        if (selection == Selection.ALL) {
            return this;
        }
        return new VariantVector(
                (BinaryVector) metadataColumn.select(selection),
                (BinaryVector) valueColumn.select(selection),
                validity.select(selection),
                selection.length());
    }

    @Override
    public long approximateHeapBytes() {
        return validity.heapBytes() + metadataColumn.approximateHeapBytes() + valueColumn.approximateHeapBytes();
    }
}
