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
 * One contiguous run of coordinates of a WKB geometry: a point, a linestring, a polygon ring, or a multipoint member. A
 * consumer reads the run through the accessors, with no copy, or bulk-copies it with {@link #copyTo(double[])}. The run
 * is a view over the cursor's current position and stays valid only until the cursor moves to the next run; a consumer
 * that needs the coordinates afterwards copies them out.
 */
public interface CoordinateRun {

    /** The number of coordinates in the run. */
    int size();

    /** The ordinates each coordinate has. */
    Dimensions dimensions();

    /** The X ordinate of coordinate {@code i}, for {@code 0 <= i < size()}. */
    double x(int i);

    /** The Y ordinate of coordinate {@code i}, for {@code 0 <= i < size()}. */
    double y(int i);

    /** The Z ordinate of coordinate {@code i}; meaningful only when {@link #dimensions()} has Z. */
    double z(int i);

    /** The M ordinate of coordinate {@code i}; meaningful only when {@link #dimensions()} has M. */
    double m(int i);

    /**
     * Copies every ordinate of the run in wire order (X, Y, then Z and M when present) into {@code dst[0 .. size() *
     * dimensions().count())}, converting the byte order in the copy.
     */
    void copyTo(double[] dst);
}
