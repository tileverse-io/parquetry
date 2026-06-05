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

import lombok.NonNull;

public final class LongVector implements ColumnVector {

    private final long[] values;
    private final Validity validity;

    private LongVector(@NonNull long[] values, @NonNull Validity validity) {
        this.values = values;
        this.validity = validity;
    }

    public static LongVector materialized(@NonNull long[] values, @NonNull Validity validity) {
        return new LongVector(values, validity);
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public Validity validity() {
        return validity;
    }

    /**
     * Returns the value at {@code row}; throws {@link IllegalStateException} when the row is null. Guard with
     * {@link #isNull(int)} / {@link #hasNulls()}, or use {@link #get(int)} for a null-aware boxed read.
     */
    public long getLong(int row) {
        if (validity.isNull(row)) {
            throw new IllegalStateException("row %d is null; guard with isNull(row) or hasNulls()".formatted(row));
        }
        return values[row];
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int row) {
        return validity.isNull(row) ? null : (T) Long.valueOf(values[row]);
    }

    public long[] asArray() {
        return values;
    }

    @Override
    public long approximateHeapBytes() {
        return (long) values.length * Long.BYTES + validity.heapBytes();
    }
}
