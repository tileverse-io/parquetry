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

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.StructuredTaskScope;

import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.Storage;

import tools.jackson.databind.JsonNode;

/**
 * A {@link StacCatalogReader} over a static STAC JSON tree (the Overture shape: a catalog document linking child
 * collections, each linking item documents that point at GeoParquet parts). Reads each document through a
 * tileverse-storage sequential get and parses it into the model. Navigation is lazy: a collection's items and a
 * catalog's child catalogs are read only when their model supplier is first called.
 */
public final class JsonStacReader implements StacCatalogReader {

    private static final String TITLE = "title";

    /** A modest fan-out bound: enough to hide per-document latency without hammering a public catalog host. */
    private static final int MAX_CONCURRENT_DOCUMENT_READS = 16;

    private final URI catalogDir;
    private final Storage storage;

    /** Builds a reader bound to a catalog root and storage at {@link #open}; the field-bearing instance is internal. */
    public JsonStacReader() {
        this.catalogDir = null;
        this.storage = null;
    }

    private JsonStacReader(URI catalogDir, Storage storage) {
        // The Storage is rooted at the catalog directory; every document key is the resolved document URI relative to
        // this directory (for example "building/collection.json").
        this.catalogDir = catalogDir;
        this.storage = storage;
    }

    @Override
    public StacCatalog open(URI catalogRoot, Storage storage) {
        Objects.requireNonNull(catalogRoot, "catalogRoot");
        Objects.requireNonNull(storage, "storage");
        JsonStacReader bound = new JsonStacReader(catalogRoot.resolve("."), storage);
        return bound.readCatalog(catalogRoot);
    }

    private StacCatalog readCatalog(URI documentUri) {
        return readCatalog(documentUri, parseDocument(documentUri));
    }

    private StacCatalog readCatalog(URI documentUri, JsonNode root) {
        String id = requiredText(root, "id");
        String title = optionalText(root, TITLE);
        String latest = optionalText(root, "latest");
        List<StacLink> links = readLinks(root);
        ChildDocuments children = new ChildDocuments(documentUri, links);
        return new StacCatalog(id, title, latest, links, children::collections, children::catalogs);
    }

    /**
     * One catalog's child documents, read once and partitioned into collections and child catalogs. Both partitions
     * come from the same single walk: each child document is fetched and parsed exactly once, whichever accessor is
     * called first, and a consumer asking for both pays one read per child.
     */
    private final class ChildDocuments {

        private final URI base;
        private final List<StacLink> links;
        private List<StacCollection> collections;
        private List<StacCatalog> catalogs;

        ChildDocuments(URI base, List<StacLink> links) {
            this.base = base;
            this.links = links;
        }

        synchronized List<StacCollection> collections() {
            readOnce();
            return collections;
        }

        synchronized List<StacCatalog> catalogs() {
            readOnce();
            return catalogs;
        }

        private void readOnce() {
            if (collections != null) {
                return;
            }
            List<StacCollection> foundCollections = new ArrayList<>();
            List<StacCatalog> foundCatalogs = new ArrayList<>();
            for (StacLink link : links) {
                if (isChildLink(link)) {
                    readChild(resolve(base, link.href()), foundCollections, foundCatalogs);
                }
            }
            collections = List.copyOf(foundCollections);
            catalogs = List.copyOf(foundCatalogs);
        }

        private void readChild(URI childUri, List<StacCollection> intoCollections, List<StacCatalog> intoCatalogs) {
            JsonNode child = parseDocument(childUri);
            if ("Collection".equals(optionalText(child, "type"))) {
                intoCollections.add(readCollection(childUri, child));
            } else {
                intoCatalogs.add(readCatalog(childUri, child));
            }
        }
    }

    private StacCollection readCollection(URI collectionUri, JsonNode root) {
        String id = requiredText(root, "id");
        String title = optionalText(root, TITLE);
        Optional<StacExtent> extent = StacJson.readExtent(root);
        List<StacLink> links = readLinks(root);
        return new StacCollection(
                id,
                title,
                extent,
                links,
                () -> readItems(collectionUri, links),
                () -> readFirstItem(collectionUri, links));
    }

    /** Reads the first linked item's document alone; a consumer probing one representative item pays one read. */
    private Optional<StacItem> readFirstItem(URI base, List<StacLink> links) {
        for (StacLink link : links) {
            if ("item".equals(link.rel())) {
                return Optional.of(readItem(resolve(base, link.href())));
            }
        }
        return Optional.empty();
    }

    private List<StacItem> readItems(URI base, List<StacLink> links) {
        List<URI> itemUris = new ArrayList<>();
        for (StacLink link : links) {
            if ("item".equals(link.rel())) {
                itemUris.add(resolve(base, link.href()));
            }
        }
        if (itemUris.isEmpty()) {
            return List.of();
        }
        return readItemsConcurrently(itemUris);
    }

    /**
     * Reads the item documents on virtual threads, at most {@link #MAX_CONCURRENT_DOCUMENT_READS} in flight, returning
     * the items in link order. A large collection is hundreds of documents (an Overture buildings release links 512);
     * reading them one round trip at a time is what made resolving such a collection take minutes.
     */
    private List<StacItem> readItemsConcurrently(List<URI> itemUris) {
        Semaphore inFlight = new Semaphore(MAX_CONCURRENT_DOCUMENT_READS);
        try (StructuredTaskScope<Object, Void> scope = StructuredTaskScope.open()) {
            List<StructuredTaskScope.Subtask<StacItem>> tasks = new ArrayList<>(itemUris.size());
            for (URI itemUri : itemUris) {
                tasks.add(scope.fork(() -> readItemBounded(itemUri, inFlight)));
            }
            scope.join();
            List<StacItem> items = new ArrayList<>(tasks.size());
            for (StructuredTaskScope.Subtask<StacItem> task : tasks) {
                items.add(task.get());
            }
            return items;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException("interrupted reading STAC item documents");
            interrupted.initCause(e);
            throw new UncheckedIOException(interrupted);
        } catch (StructuredTaskScope.FailedException e) {
            throw asUnchecked(e.getCause());
        }
    }

    private StacItem readItemBounded(URI itemUri, Semaphore inFlight) throws InterruptedException {
        inFlight.acquire();
        try {
            return readItem(itemUri);
        } finally {
            inFlight.release();
        }
    }

    /** Rethrows a per-item read failure with the type a sequential read would have thrown. */
    private static RuntimeException asUnchecked(Throwable cause) {
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("reading a STAC item document failed", cause);
    }

    private StacItem readItem(URI itemUri) {
        JsonNode root = parseDocument(itemUri);
        String id = requiredText(root, "id");
        double[] bbox = StacJson.readBbox(root.get("bbox"));
        Optional<String> datetime = readDatetime(root);
        List<StacAsset> assets = readAssets(root.get("assets"), itemUri);
        List<StacLink> links = readLinks(root);
        return new StacItem(id, bbox, datetime, assets, links);
    }

    private List<StacAsset> readAssets(JsonNode assetsNode, URI base) {
        List<StacAsset> assets = new ArrayList<>();
        if (assetsNode == null || !assetsNode.isObject()) {
            return assets;
        }
        for (String name : assetsNode.propertyNames()) {
            JsonNode asset = assetsNode.get(name);
            String href = requiredText(asset, "href");
            String type = optionalText(asset, "type");
            String title = optionalText(asset, TITLE);
            List<String> roles = readStringArray(asset.get("roles"));
            assets.add(new StacAsset(resolve(base, href).toString(), type, title, roles));
        }
        return assets;
    }

    private List<StacLink> readLinks(JsonNode root) {
        List<StacLink> links = new ArrayList<>();
        JsonNode linksNode = root.get("links");
        if (linksNode == null || !linksNode.isArray()) {
            return links;
        }
        for (JsonNode link : linksNode) {
            String rel = requiredText(link, "rel");
            String href = requiredText(link, "href");
            String type = optionalText(link, "type");
            String title = optionalText(link, TITLE);
            links.add(new StacLink(rel, href, type, title));
        }
        return links;
    }

    private Optional<String> readDatetime(JsonNode root) {
        JsonNode properties = root.get("properties");
        if (properties == null || !properties.isObject()) {
            return Optional.empty();
        }
        JsonNode datetime = properties.get("datetime");
        if (datetime == null || datetime.isNull() || !datetime.isString()) {
            return Optional.empty();
        }
        return Optional.of(datetime.stringValue());
    }

    private List<String> readStringArray(JsonNode array) {
        List<String> values = new ArrayList<>();
        if (array == null || !array.isArray()) {
            return values;
        }
        for (JsonNode value : array) {
            values.add(value.stringValue());
        }
        return values;
    }

    private static boolean isChildLink(StacLink link) {
        return "child".equals(link.rel());
    }

    private JsonNode parseDocument(URI documentUri) {
        String key = storageKey(documentUri);
        try (ReadHandle handle = storage.read(key);
                InputStream content = handle.content()) {
            return StacJson.parse(content);
        } catch (IOException failure) {
            throw new UncheckedIOException("reading STAC document " + key, failure);
        }
    }

    private static URI resolve(URI base, String href) {
        return base.resolve(href);
    }

    /**
     * The storage key for {@code documentUri}: its path relative to the catalog directory the Storage is rooted at. A
     * catalog at {@code .../overture-mini/catalog.json} yields keys such as {@code catalog.json},
     * {@code building/collection.json}, and {@code building/parts/west.parquet}.
     */
    private String storageKey(URI documentUri) {
        String relative = catalogDir.relativize(documentUri).getPath();
        return relative.startsWith("/") ? relative.substring(1) : relative;
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull() || !value.isString()) {
            throw new StacFormatException("missing or non-string field: " + field);
        }
        return value.stringValue();
    }

    private static String optionalText(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull() || !value.isString()) {
            return null;
        }
        return value.stringValue();
    }
}
