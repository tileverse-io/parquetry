/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.net.URI;

import io.tileverse.storage.Storage;

/**
 * Opens a STAC catalog for enumeration over a tileverse {@link Storage}. Reads catalog, collection, and item metadata
 * (ids, extents, asset hrefs) cheaply and lazily; it never reads a collection's feature data. A reader implementation
 * may walk a static JSON tree or a stac-geoparquet item-table; both produce the same {@link StacCatalog} model.
 */
public interface StacCatalogReader {

    /**
     * Opens the catalog rooted at {@code catalogRoot}, reading bytes through {@code storage}. {@code catalogRoot}
     * locates the entry document (for a JSON tree, the catalog's {@code catalog.json}); {@code storage} is rooted such
     * that the entry document and the documents it links to resolve as storage keys relative to that root.
     */
    StacCatalog open(URI catalogRoot, Storage storage);
}
