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

import lombok.NonNull;

public final class BooleanVector implements ColumnVector {

    private final boolean[] values;
    private final BitSet validity;

    private BooleanVector(@NonNull boolean[] values, @NonNull BitSet validity) {
        this.values = values;
        this.validity = validity;
    }

    public static BooleanVector materialized(@NonNull boolean[] values, @NonNull BitSet validity) {
        return new BooleanVector(values, validity);
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public BitSet validity() {
        return validity;
    }

    public boolean get(int row) {
        return values[row];
    }

    public boolean[] asArray() {
        return values;
    }
}
