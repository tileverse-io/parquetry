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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

class JsonStacReaderTest {

    @Test
    void mapsCollectionItemsAssetsAndPreservesPmtilesLink(@TempDir Path tempDir) throws Exception {
        Path root = copyFixtureTree(tempDir);
        try (Storage storage = StorageFactory.open(root.toUri())) {
            StacCatalog catalog =
                    new JsonStacReader().open(root.resolve("catalog.json").toUri(), storage);

            assertThat(catalog.id()).isEqualTo("overture-mini");
            List<StacCollection> collections = catalog.collections();
            assertThat(collections).hasSize(1);

            StacCollection building = collections.get(0);
            assertThat(building.id()).isEqualTo("building");
            assertThat(building.extent()).isPresent();
            assertThat(building.links()).anyMatch(link -> link.rel().equals("pmtiles"));

            List<StacItem> items = building.items();
            assertThat(items).extracting(StacItem::id).containsExactly("item-west", "item-east");

            StacItem west = items.stream()
                    .filter(i -> i.id().equals("item-west"))
                    .findFirst()
                    .orElseThrow();
            assertThat(west.bbox()).containsExactly(0, 0, 10, 10);
            assertThat(west.assets())
                    .anyMatch(a ->
                            a.href().endsWith("west.parquet") && a.type().equals("application/vnd.apache.parquet"));
        }
    }

    @Test
    void readsEachChildDocumentOnceAndDefersItemDocuments(@TempDir Path tempDir) throws Exception {
        Path root = copyFixtureTree(tempDir);
        Map<String, Integer> reads = new ConcurrentHashMap<>();
        try (Storage storage = StorageFactory.open(root.toUri())) {
            StacCatalog catalog =
                    new JsonStacReader().open(root.resolve("catalog.json").toUri(), countingStorage(storage, reads));

            List<StacCollection> collections = catalog.collections();
            List<StacCatalog> children = catalog.childCatalogs();

            assertThat(collections).hasSize(1);
            assertThat(children).isEmpty();
            assertThat(reads.get("building/collection.json")).isEqualTo(1);
            assertThat(reads.keySet()).noneMatch(key -> key.contains("items/"));

            collections.get(0).items();
            assertThat(reads.get("building/items/item-west.json")).isEqualTo(1);
            assertThat(reads.get("building/items/item-east.json")).isEqualTo(1);
        }
    }

    /** Wraps {@code delegate} counting every document read by key. */
    private static Storage countingStorage(Storage delegate, Map<String, Integer> reads) {
        InvocationHandler countingReads = (proxy, method, args) -> {
            if ("read".equals(method.getName()) && args != null && args.length >= 1) {
                reads.merge((String) args[0], 1, Integer::sum);
            }
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException failure) {
                throw failure.getCause();
            }
        };
        return (Storage)
                Proxy.newProxyInstance(Storage.class.getClassLoader(), new Class<?>[] {Storage.class}, countingReads);
    }

    private static Path copyFixtureTree(Path tempDir) throws Exception {
        Path source = locateFixtureRoot();
        try (Stream<Path> walk = Files.walk(source)) {
            walk.forEach(path -> copyInto(source, path, tempDir));
        }
        return tempDir;
    }

    private static Path locateFixtureRoot() {
        java.net.URL url = JsonStacReaderTest.class.getClassLoader().getResource("stac/overture-mini/catalog.json");
        if (url == null) {
            throw new IllegalStateException("fixture stac/overture-mini/catalog.json not on the classpath");
        }
        return Path.of(URI.create(url.toString())).getParent();
    }

    private static void copyInto(Path source, Path path, Path destRoot) {
        try {
            Path relative = source.relativize(path);
            Path target = destRoot.resolve(relative.toString());
            if (Files.isDirectory(path)) {
                Files.createDirectories(target);
            } else {
                Files.createDirectories(target.getParent());
                Files.copy(path, target);
            }
        } catch (Exception copyFailure) {
            throw new IllegalStateException("copying fixture " + path, copyFailure);
        }
    }
}
