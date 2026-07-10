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
package io.tileverse.parquetry.format;

/**
 * Order of the per-page min/max values in a {@link ColumnIndex}; mirror of {@code BoundaryOrder} in
 * {@code parquet.thrift}.
 *
 * <p>{@link #ASCENDING} or {@link #DESCENDING} lets a reader short-circuit page filtering with a binary search;
 * {@link #UNORDERED} forces a linear scan of the per-page bounds.
 *
 * <p>Each constant carries its Thrift wire code in {@link #value()}; resolve incoming i32 codes via
 * {@link #valueOf(int)}.
 */
public enum BoundaryOrder {
    UNORDERED(0),
    ASCENDING(1),
    DESCENDING(2);

    private final int value;

    BoundaryOrder(int value) {
        this.value = value;
    }

    /** Thrift wire code for this constant, matching the value defined in {@code parquet.thrift}. */
    public int value() {
        return value;
    }

    /** @throws UnknownCodeException if no defined variant carries that code */
    public static BoundaryOrder valueOf(int code) {
        return switch (code) {
            case 0 -> UNORDERED;
            case 1 -> ASCENDING;
            case 2 -> DESCENDING;
            default -> throw new UnknownCodeException("Unknown BoundaryOrder wire code: " + code);
        };
    }
}
