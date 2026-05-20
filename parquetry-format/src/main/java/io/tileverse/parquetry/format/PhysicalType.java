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
 *
 * <p>Each variant carries its Thrift wire code in {@link #value()}; deserializers resolve incoming i32 codes via
 * {@link #valueOf(int)} rather than indexing into {@link #values()}, so enum declaration order is no longer
 * load-bearing.
 */
public enum PhysicalType {
    BOOLEAN(0),
    INT32(1),
    INT64(2),
    INT96(3),
    FLOAT(4),
    DOUBLE(5),
    BYTE_ARRAY(6),
    FIXED_LEN_BYTE_ARRAY(7);

    private final int value;

    PhysicalType(int value) {
        this.value = value;
    }

    /** Thrift wire code for this variant, matching the value defined in {@code parquet.thrift}. */
    public int value() {
        return value;
    }

    /**
     * Returns the variant whose {@link #value()} equals {@code code}. Compiles to a {@code tableswitch} bytecode - O(1)
     * lookup, no allocations on the hot decode path.
     *
     * @throws UnknownVariantException if no defined variant carries that code (forward-compatibility is the caller's
     *     responsibility - wrap this in a try/catch if you want to tolerate unknown values)
     */
    public static PhysicalType valueOf(int code) {
        return switch (code) {
            case 0 -> BOOLEAN;
            case 1 -> INT32;
            case 2 -> INT64;
            case 3 -> INT96;
            case 4 -> FLOAT;
            case 5 -> DOUBLE;
            case 6 -> BYTE_ARRAY;
            case 7 -> FIXED_LEN_BYTE_ARRAY;
            default -> throw new UnknownVariantException("Unknown PhysicalType wire code: " + code);
        };
    }
}
