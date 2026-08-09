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
import java.util.function.Supplier;

/**
 * The root of a STAC tree: a node with child catalogs and collections, both pulled lazily. The tree is retained, never
 * flattened, which lets a consumer walk from a leaf collection up to a theme-level link.
 *
 * @param id the catalog id
 * @param title a human-readable title, or null
 * @param latest the id of the child catalog holding the latest release (the Overture releases-catalog property), or
 *     null when the document declares none
 * @param links the catalog's links
 * @param collectionSupplier supplies this node's collections on demand
 * @param childCatalogSupplier supplies this node's child catalogs on demand
 */
public record StacCatalog(
        String id,
        String title,
        String latest,
        List<StacLink> links,
        Supplier<List<StacCollection>> collectionSupplier,
        Supplier<List<StacCatalog>> childCatalogSupplier) {

    public StacCatalog {
        Objects.requireNonNull(id, "id");
        links = links == null ? List.of() : List.copyOf(links);
        Objects.requireNonNull(collectionSupplier, "collectionSupplier");
        Objects.requireNonNull(childCatalogSupplier, "childCatalogSupplier");
    }

    /** A catalog declaring no latest child. */
    public StacCatalog(
            String id,
            String title,
            List<StacLink> links,
            Supplier<List<StacCollection>> collectionSupplier,
            Supplier<List<StacCatalog>> childCatalogSupplier) {
        this(id, title, null, links, collectionSupplier, childCatalogSupplier);
    }

    /** Pulls this node's collections on demand. */
    public List<StacCollection> collections() {
        return collectionSupplier.get();
    }

    /** Pulls this node's child catalogs on demand. */
    public List<StacCatalog> childCatalogs() {
        return childCatalogSupplier.get();
    }
}
