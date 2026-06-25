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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;

import tools.jackson.databind.JsonNode;

/**
 * A {@link StacCatalogReader} over a static STAC JSON tree (the Overture shape: a catalog document linking child
 * collections, each linking item documents that point at GeoParquet parts). Reads each document through a
 * tileverse-storage range get and parses it into the model. Navigation is lazy: a collection's items and a catalog's
 * child catalogs are read only when their model supplier is first called.
 */
public final class JsonStacReader implements StacCatalogReader {

    private static final String TITLE = "title";

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
        JsonNode root = parseDocument(documentUri);
        String id = requiredText(root, "id");
        String title = optionalText(root, TITLE);
        List<StacLink> links = readLinks(root);
        return new StacCatalog(
                id,
                title,
                links,
                () -> readCollections(documentUri, links),
                () -> readChildCatalogs(documentUri, links));
    }

    private List<StacCollection> readCollections(URI base, List<StacLink> links) {
        List<StacCollection> collections = new ArrayList<>();
        for (StacLink link : links) {
            if (isChildLink(link) && pointsAtCollection(base, link)) {
                collections.add(readCollection(resolve(base, link.href())));
            }
        }
        return collections;
    }

    private List<StacCatalog> readChildCatalogs(URI base, List<StacLink> links) {
        List<StacCatalog> children = new ArrayList<>();
        for (StacLink link : links) {
            if (isChildLink(link) && !pointsAtCollection(base, link)) {
                children.add(readCatalog(resolve(base, link.href())));
            }
        }
        return children;
    }

    private boolean pointsAtCollection(URI base, StacLink link) {
        JsonNode child = parseDocument(resolve(base, link.href()));
        String type = optionalText(child, "type");
        return "Collection".equals(type);
    }

    private StacCollection readCollection(URI collectionUri) {
        JsonNode root = parseDocument(collectionUri);
        String id = requiredText(root, "id");
        String title = optionalText(root, TITLE);
        Optional<StacExtent> extent = readExtent(root);
        List<StacLink> links = readLinks(root);
        return new StacCollection(id, title, extent, links, () -> readItems(collectionUri, links));
    }

    private List<StacItem> readItems(URI base, List<StacLink> links) {
        List<StacItem> items = new ArrayList<>();
        for (StacLink link : links) {
            if ("item".equals(link.rel())) {
                items.add(readItem(resolve(base, link.href())));
            }
        }
        return items;
    }

    private StacItem readItem(URI itemUri) {
        JsonNode root = parseDocument(itemUri);
        String id = requiredText(root, "id");
        double[] bbox = readBbox(root.get("bbox"));
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

    private Optional<StacExtent> readExtent(JsonNode root) {
        JsonNode extent = root.get("extent");
        if (extent == null || !extent.isObject()) {
            return Optional.empty();
        }
        Optional<double[]> bbox = readSpatialBbox(extent.get("spatial"));
        Optional<String[]> interval = readTemporalInterval(extent.get("temporal"));
        return Optional.of(new StacExtent(bbox, interval));
    }

    private Optional<double[]> readSpatialBbox(JsonNode spatial) {
        if (spatial == null || !spatial.isObject()) {
            return Optional.empty();
        }
        JsonNode bboxes = spatial.get("bbox");
        if (bboxes == null || !bboxes.isArray() || bboxes.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(readBbox(bboxes.get(0))).filter(Objects::nonNull);
    }

    private Optional<String[]> readTemporalInterval(JsonNode temporal) {
        if (temporal == null || !temporal.isObject()) {
            return Optional.empty();
        }
        JsonNode intervals = temporal.get("interval");
        if (intervals == null || !intervals.isArray() || intervals.isEmpty()) {
            return Optional.empty();
        }
        JsonNode first = intervals.get(0);
        if (first == null || !first.isArray()) {
            return Optional.empty();
        }
        String[] interval = new String[first.size()];
        for (int i = 0; i < first.size(); i++) {
            JsonNode bound = first.get(i);
            interval[i] = bound == null || bound.isNull() ? null : bound.stringValue();
        }
        return Optional.of(interval);
    }

    // A null return is the model's signal that the item declared no bbox; an empty array would be read as a real bound.
    @SuppressWarnings("java:S1168")
    private double[] readBbox(JsonNode bbox) {
        if (bbox == null || !bbox.isArray() || bbox.isEmpty()) {
            return null;
        }
        double[] values = new double[bbox.size()];
        for (int i = 0; i < bbox.size(); i++) {
            values[i] = bbox.get(i).doubleValue();
        }
        return values;
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
        return StacJson.parse(readFully(storageKey(documentUri)));
    }

    /** Reads the whole object at {@code key} as UTF-8 text through one range get. */
    private String readFully(String key) {
        try (RangeReader reader = storage.openRangeReader(key)) {
            OptionalLong size = reader.size();
            if (size.isEmpty()) {
                throw new StacFormatException("storage cannot report the size of " + key);
            }
            int length = Math.toIntExact(size.getAsLong());
            ByteBuffer buffer = reader.readRange(0, length);
            buffer.flip();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
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
