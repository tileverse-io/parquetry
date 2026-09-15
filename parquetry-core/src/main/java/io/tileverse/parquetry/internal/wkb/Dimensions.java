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
package io.tileverse.parquetry.internal.wkb;

/**
 * Which ordinates each coordinate of a WKB geometry has beyond X and Y, and the shape of the packed {@code double[]}
 * that holds a run of them: {@link #count()} ordinates per coordinate, of which {@link #measures()} are measures. That
 * pair is what a JTS packed coordinate sequence takes as {@code (dimension, measures)}.
 */
public enum Dimensions {
    XY(2, 0, false, false),
    Z(3, 0, true, false),
    M(3, 1, false, true),
    ZM(4, 1, true, true);

    private final int count;
    private final int measures;
    private final boolean hasZ;
    private final boolean hasM;

    Dimensions(int count, int measures, boolean hasZ, boolean hasM) {
        this.count = count;
        this.measures = measures;
        this.hasZ = hasZ;
        this.hasM = hasM;
    }

    /** The dimensions with the given Z and M ordinates. */
    public static Dimensions of(boolean hasZ, boolean hasM) {
        if (hasZ && hasM) {
            return ZM;
        }
        if (hasZ) {
            return Z;
        }
        if (hasM) {
            return M;
        }
        return XY;
    }

    /** Ordinates per coordinate: 2, 3 or 4. */
    public int count() {
        return count;
    }

    /** How many of the ordinates are measures: 0 or 1. */
    public int measures() {
        return measures;
    }

    public boolean hasZ() {
        return hasZ;
    }

    public boolean hasM() {
        return hasM;
    }
}
