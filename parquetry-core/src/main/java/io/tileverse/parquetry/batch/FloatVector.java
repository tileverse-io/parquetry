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

public final class FloatVector implements ColumnVector {

    private final float[] values;
    private final Validity validity;

    private FloatVector(@NonNull float[] values, @NonNull Validity validity) {
        this.values = values;
        this.validity = validity;
    }

    public static FloatVector materialized(@NonNull float[] values, @NonNull Validity validity) {
        return new FloatVector(values, validity);
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
    public float getFloat(int row) {
        if (validity.isNull(row)) {
            throw new IllegalStateException("row %d is null; guard with isNull(row) or hasNulls()".formatted(row));
        }
        return values[row];
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int row) {
        return validity.isNull(row) ? null : (T) Float.valueOf(values[row]);
    }

    public float[] asArray() {
        return values;
    }

    @Override
    public long approximateHeapBytes() {
        return (long) values.length * Integer.BYTES + validity.heapBytes();
    }
}
