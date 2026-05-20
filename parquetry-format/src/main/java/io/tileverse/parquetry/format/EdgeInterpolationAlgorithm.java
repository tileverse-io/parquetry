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
 * Edge interpolation algorithm for {@link LogicalType.Geography} columns; mirror of {@code EdgeInterpolationAlgorithm}
 * in {@code parquet.thrift}.
 *
 * <p>Selects how to interpolate a geodesic edge between two vertices. {@link #SPHERICAL} treats the Earth as a sphere;
 * the others are progressively more accurate ellipsoidal models.
 *
 * <p>Each variant carries its Thrift wire code in {@link #value()}; resolve incoming i32 codes via
 * {@link #valueOf(int)}.
 */
public enum EdgeInterpolationAlgorithm {
    SPHERICAL(0),
    VINCENTY(1),
    THOMAS(2),
    ANDOYER(3),
    KARNEY(4);

    private final int value;

    EdgeInterpolationAlgorithm(int value) {
        this.value = value;
    }

    /** Thrift wire code for this variant, matching the value defined in {@code parquet.thrift}. */
    public int value() {
        return value;
    }

    /**
     * @throws UnknownVariantException if no defined variant carries that code (typically because a newer Parquet writer
     *     used an algorithm parquetry doesn't know yet - callers that want forward-compat tolerance should wrap this in
     *     a try/catch)
     */
    public static EdgeInterpolationAlgorithm valueOf(int code) {
        return switch (code) {
            case 0 -> SPHERICAL;
            case 1 -> VINCENTY;
            case 2 -> THOMAS;
            case 3 -> ANDOYER;
            case 4 -> KARNEY;
            default -> throw new UnknownVariantException("Unknown EdgeInterpolationAlgorithm wire code: " + code);
        };
    }
}
