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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * One STAC collection: a named group of items sharing a schema. The item supplier is lazy; a catalog with hundreds of
 * item documents pulls them only when {@link #items()} is first called, never eagerly at open.
 *
 * @param id the collection id, unique within its catalog (used verbatim as the dataset and feature-type name)
 * @param title a human-readable title, or null
 * @param extent the collection's declared extent, when present
 * @param links the collection's links (retains non-data rels such as {@code "pmtiles"})
 * @param itemSupplier supplies the collection's items on demand
 */
public record StacCollection(
        String id,
        String title,
        Optional<StacExtent> extent,
        List<StacLink> links,
        Supplier<List<StacItem>> itemSupplier) {

    public StacCollection {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(extent, "extent");
        links = links == null ? List.of() : List.copyOf(links);
        Objects.requireNonNull(itemSupplier, "itemSupplier");
    }

    /** Pulls the collection's items on demand. */
    public List<StacItem> items() {
        return itemSupplier.get();
    }
}
