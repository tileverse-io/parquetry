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
package io.tileverse.parquetry.geotools.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.Parameter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.geotools.data.CatalogDataStore;
import io.tileverse.parquetry.testkit.TestCorpus;

class GeoParquetDataStoreFactoryTest {

    @Test
    void datastoreFinderOpensAGeoParquetFile(@TempDir Path dir) throws Exception {
        Path file = TestCorpus.extractFile("geoparquet/examples/example.parquet", dir);

        Map<String, Object> params = new HashMap<>();
        params.put("geoparquet", file.toUri().toString());

        GeoParquetDataStoreFactory factory = new GeoParquetDataStoreFactory();
        assertThat(factory.canProcess(params)).isTrue();

        DataStore store = DataStoreFinder.getDataStore(params);
        assertThat(store).isInstanceOf(CatalogDataStore.class);
        try {
            assertThat(store.getTypeNames()).hasSize(1);
        } finally {
            store.dispose();
        }
    }

    @Test
    void canProcessRequiresOwnUriKey() {
        GeoParquetDataStoreFactory factory = new GeoParquetDataStoreFactory();
        assertThat(factory.canProcess(Map.of("geoparquet", "file:///tmp/x.parquet")))
                .isTrue();
        assertThat(factory.canProcess(Map.of("uri", "file:///tmp/x.parquet"))).isFalse();
        assertThat(factory.canProcess(Map.of("geoparquet-stac", "file:///tmp/catalog.json")))
                .isFalse();
        assertThat(factory.canProcess(Map.of("iceberg", "file:///tmp/wh"))).isFalse();
    }

    @Test
    void exposesOptionalFidParameter() {
        GeoParquetDataStoreFactory factory = new GeoParquetDataStoreFactory();
        assertThat(factory.getParametersInfo())
                .anySatisfy(param -> assertThat(param.key).isEqualTo("fid"));
    }

    @ParameterizedTest
    @MethodSource("uriClassifications")
    void classifiesSingleFileVersusContainer(String uri, boolean singleFile) {
        assertThat(GeoParquetDataStoreFactory.isSingleFileUri(URI.create(uri)))
                .as(uri)
                .isEqualTo(singleFile);
    }

    /**
     * A single-object URI (plain {@code .parquet}, no glob) opens directly by key; a directory or glob is listed. The
     * distinction matters over HTTP and other list-less backends, where only the direct path resolves.
     */
    static Stream<Arguments> uriClassifications() {
        return Stream.of(
                arguments("file:///data/place.parquet", true),
                arguments("http://host.docker.internal:9191/2024-08-20/place.parquet", true),
                arguments("s3://bucket/dir/PLACE.PARQUET", true),
                arguments("http://host/place.parquet?X-Amz-Signature=sig", true),
                arguments("file:///data/ne/", false),
                arguments("file:///data/ne", false),
                arguments("file:///data/ne/*.parquet", false),
                arguments("file:///data/ne/**/*.parquet", false),
                arguments("s3://bucket/ne/", false));
    }

    @Test
    void exposesOptionalLayerGroupingParameter() {
        GeoParquetDataStoreFactory factory = new GeoParquetDataStoreFactory();
        assertThat(factory.getParametersInfo()).anySatisfy(param -> {
            assertThat(param.key).isEqualTo("layer-grouping");
            assertThat(param.required).isFalse();
            @SuppressWarnings("unchecked")
            List<String> options = (List<String>) param.metadata.get(Parameter.OPTIONS);
            assertThat(options).containsExactlyInAnyOrder("merged", "file");
            // GeoServer's store edit page sorts the options in place; an immutable list breaks the page
            assertThatCode(() -> options.sort(null)).doesNotThrowAnyException();
        });
    }

    @Test
    void rejectsUnknownLayerGroupingValue(@TempDir Path dir) {
        Map<String, Object> params = new HashMap<>();
        params.put("geoparquet", dir.toUri().toString());
        params.put("layer-grouping", "both");

        GeoParquetDataStoreFactory factory = new GeoParquetDataStoreFactory();
        assertThatThrownBy(() -> factory.createDataStore(params))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("layer-grouping")
                .hasMessageContaining("both");
    }

    @ParameterizedTest
    @MethodSource("fileModeContainers")
    void fileModeContainerDropsGlobTail(String uri, String container) {
        assertThat(GeoParquetDataStoreFactory.fileModeContainer(URI.create(uri)))
                .isEqualTo(URI.create(container));
    }

    /**
     * In {@code layer-grouping=file} mode the container is the directory itself; any glob tail is dropped, never
     * honored.
     */
    static Stream<Arguments> fileModeContainers() {
        return Stream.of(
                arguments("file:///data/ne/", "file:///data/ne/"),
                arguments("file:///data/ne", "file:///data/ne"),
                arguments("file:///data/ne/*.parquet", "file:///data/ne/"),
                arguments("file:///data/ne/**/*.parquet", "file:///data/ne/"),
                arguments("s3://bucket/ne/*.parquet", "s3://bucket/ne/"));
    }

    @Test
    void directoryWithFileLayersOpensOneLayerPerFile(@TempDir Path dir) throws Exception {
        Path storeDir = twoFileStoreDir(dir);

        Map<String, Object> params = new HashMap<>();
        params.put("geoparquet", storeDir.toUri().toString());
        params.put("layer-grouping", "file");

        DataStore store = new GeoParquetDataStoreFactory().createDataStore(params);
        try {
            assertThat(store.getTypeNames()).containsExactly("countries", "rivers");
            assertThat(((CatalogDataStore) store).catalog().capabilities().enumeratesDatasets())
                    .isTrue();
        } finally {
            store.dispose();
        }
    }

    @Test
    void directoryDefaultsToMergedLayer(@TempDir Path dir) throws Exception {
        Path storeDir = twoFileStoreDir(dir);

        Map<String, Object> params = new HashMap<>();
        params.put("geoparquet", storeDir.toUri().toString());

        DataStore store = new GeoParquetDataStoreFactory().createDataStore(params);
        try {
            assertThat(store.getTypeNames()).hasSize(1);
            assertThat(((CatalogDataStore) store).catalog().capabilities().enumeratesDatasets())
                    .isFalse();
        } finally {
            store.dispose();
        }
    }

    @Test
    void singleFileUriIgnoresLayersParam(@TempDir Path dir) throws Exception {
        Path file = TestCorpus.extractFile("geoparquet/examples/example.parquet", dir);

        Map<String, Object> params = new HashMap<>();
        params.put("geoparquet", file.toUri().toString());
        params.put("layer-grouping", "file");

        DataStore store = new GeoParquetDataStoreFactory().createDataStore(params);
        try {
            assertThat(store.getTypeNames()).hasSize(1);
            assertThat(((CatalogDataStore) store).catalog().capabilities().enumeratesDatasets())
                    .isFalse();
        } finally {
            store.dispose();
        }
    }

    /** A directory of two same-schema GeoParquet files named {@code countries.parquet} and {@code rivers.parquet}. */
    private static Path twoFileStoreDir(Path dir) throws Exception {
        Path example = TestCorpus.extractFile("geoparquet/examples/example.parquet", dir);
        Path storeDir = Files.createDirectory(dir.resolve("store"));
        Files.copy(example, storeDir.resolve("countries.parquet"));
        Files.copy(example, storeDir.resolve("rivers.parquet"));
        return storeDir;
    }
}
