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
package io.tileverse.stac;

import java.util.Objects;
import java.util.Optional;

/**
 * A collection's declared extent: an optional 2D spatial bounding box {@code [minx, miny, maxx, maxy]} and an optional
 * temporal interval {@code [start, end]} as ISO-8601 strings (either bound may be null in the source, kept as null).
 */
public record StacExtent(Optional<double[]> bbox, Optional<String[]> interval) {

    public StacExtent {
        Objects.requireNonNull(bbox, "bbox");
        Objects.requireNonNull(interval, "interval");
        bbox = bbox.map(double[]::clone);
        interval = interval.map(String[]::clone);
    }

    @Override
    public Optional<double[]> bbox() {
        return bbox.map(double[]::clone);
    }

    @Override
    public Optional<String[]> interval() {
        return interval.map(String[]::clone);
    }
}
