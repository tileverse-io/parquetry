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
package io.tileverse.stac;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One STAC item: a feature record that points at its data asset and declares the bounds of the data it covers. In this
 * model an item is a catalog entry (an index row), not the feature data itself; its bbox drives whole-file pruning and
 * its data asset names the GeoParquet part to read.
 *
 * @param id the item id, unique within its collection
 * @param bbox the item bounds as {@code [minx, miny, maxx, maxy]} or {@code [minx, miny, minz, maxx, maxy, maxz]}, or
 *     null when the item declared none
 * @param datetime the item datetime as an ISO-8601 string, when present
 * @param assets the item's assets, never null
 * @param links the item's links, never null
 */
// Value equality is not part of this record's contract; the bbox array is defensively copied and used by content,
// never as a key. Overriding equals/hashCode/toString to compare the array would add API with no consumer.
@SuppressWarnings("java:S6218")
public record StacItem(
        String id, double[] bbox, Optional<String> datetime, List<StacAsset> assets, List<StacLink> links) {

    public StacItem {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(datetime, "datetime");
        bbox = bbox == null ? null : bbox.clone();
        assets = assets == null ? List.of() : List.copyOf(assets);
        links = links == null ? List.of() : List.copyOf(links);
    }

    @Override
    public double[] bbox() {
        return bbox == null ? null : bbox.clone();
    }
}
