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
package io.tileverse.parquetry.dataset;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntFunction;

import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;
import io.tileverse.parquetry.format.BoundingBox;

/**
 * The file-level plan of a probe-bearing read over a dataset: the surviving files ordered by their geometry box, and a
 * gate that is consulted before each file opens and drops a file whose whole box the probe reports as already covered.
 * The boxes come from the dataset's per-file statistics, resolved once when the plan is built; no file is opened.
 *
 * <p>Visiting files ascending by their box's minimum corner lets a probe accumulate paint coherently from one
 * neighbouring file to the next; a file without a box sorts last, keeping its relative order among the other box-less
 * files. The gate uses the probe's read-only {@link SpatialReadProbe#probeRegion} consultation, which records no
 * coverage; it must be consulted only once every earlier file in the order has been drained, which is where the probe
 * has seen those files' paint.
 *
 * <p>A file without a box, or whose box wraps the antimeridian, is never dropped: an unknown extent might cover space
 * that the probe has not painted, and a wrapping box has {@code xmin > xmax}, which would reach the consultation as an
 * inverted rectangle. Both cases still take their place in the visit order.
 */
final class SpatialFileVisit {

    /** Sorts present boxes ascending by {@code (minX, minY)} and sinks an absent box to the end. */
    private static final Comparator<Optional<BoundingBox>> MIN_CORNER_LAST_IF_ABSENT = Comparator.comparingDouble(
                    (Optional<BoundingBox> box) -> box.map(BoundingBox::xmin).orElse(Double.POSITIVE_INFINITY))
            .thenComparingDouble(box -> box.map(BoundingBox::ymin).orElse(Double.POSITIVE_INFINITY));

    private final List<Integer> order;
    private final Map<Integer, Optional<BoundingBox>> boxes;
    private final SpatialReadProbe probe;

    private SpatialFileVisit(List<Integer> order, Map<Integer, Optional<BoundingBox>> boxes, SpatialReadProbe probe) {
        this.order = order;
        this.boxes = boxes;
        this.probe = probe;
    }

    /**
     * Plans the visit of {@code survivors}, resolving each file's box through {@code fileBox} once; the sort is stable.
     */
    static SpatialFileVisit plan(
            List<Integer> survivors, IntFunction<Optional<BoundingBox>> fileBox, SpatialReadProbe probe) {
        Map<Integer, Optional<BoundingBox>> boxes = HashMap.newHashMap(survivors.size());
        for (int index : survivors) {
            boxes.put(index, fileBox.apply(index));
        }
        List<Integer> ordered = new ArrayList<>(survivors);
        ordered.sort(Comparator.comparing(boxes::get, MIN_CORNER_LAST_IF_ABSENT));
        return new SpatialFileVisit(List.copyOf(ordered), boxes, probe);
    }

    /** The file indices in visit order. */
    List<Integer> order() {
        return order;
    }

    /**
     * Whether the file at {@code index} is dropped before it opens: it has a box bounding a single rectangle and the
     * probe reports that rectangle covered. A file with no box, a file whose box wraps the antimeridian, and an index
     * outside this plan are all kept.
     */
    boolean skips(int index) {
        Optional<BoundingBox> box = boxes.getOrDefault(index, Optional.empty());
        if (box.isEmpty()) {
            return false;
        }
        BoundingBox fileBox = box.orElseThrow();
        if (fileBox.wrapsAntimeridian()) {
            return false;
        }
        Decision decision = probe.probeRegion(fileBox.xmin(), fileBox.ymin(), fileBox.xmax(), fileBox.ymax());
        return decision instanceof Decision.Skip;
    }
}
