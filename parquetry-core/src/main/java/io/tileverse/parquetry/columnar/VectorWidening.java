/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.columnar;

import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Widens a decoded {@link ColumnVector} to a broader primitive type for the output shape. Only the sanctioned
 * promotions are accepted; an unsupported pair is rejected rather than guessed at. The widened vector copies the values
 * into a broader backing while reusing the source's {@link Validity} by reference. A null row stays null regardless of
 * the value copied into its slot.
 */
public final class VectorWidening {

    private VectorWidening() {}

    /** The widened equivalent of {@code source} at the {@code target} kind, or {@link IllegalArgumentException}. */
    public static ColumnVector widen(ColumnVector source, PrimitiveKind target) {
        return switch (source) {
            case IntVector intVector when target == PrimitiveKind.INT64 -> intToLong(intVector);
            case FloatVector floatVector when target == PrimitiveKind.DOUBLE -> floatToDouble(floatVector);
            default -> throw unsupported(source, target);
        };
    }

    private static LongVector intToLong(IntVector source) {
        int rowCount = source.size();
        long[] widened = new long[rowCount];
        for (int row = 0; row < rowCount; row++) {
            if (source.isValid(row)) {
                widened[row] = source.getInt(row);
            }
        }
        return LongVector.materialized(widened, source.validity());
    }

    private static DoubleVector floatToDouble(FloatVector source) {
        int rowCount = source.size();
        double[] widened = new double[rowCount];
        for (int row = 0; row < rowCount; row++) {
            if (source.isValid(row)) {
                widened[row] = source.getFloat(row);
            }
        }
        return DoubleVector.materialized(widened, source.validity());
    }

    private static IllegalArgumentException unsupported(ColumnVector source, PrimitiveKind target) {
        return new IllegalArgumentException("cannot widen " + source.getClass().getSimpleName() + " to " + target);
    }
}
