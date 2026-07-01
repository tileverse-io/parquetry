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
package io.tileverse.parquetry.geotools;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.testkit.TestCorpus;

class GeoParquetDataStoreFactoryTest {

    @Test
    void datastoreFinderOpensAGeoParquetFile(@TempDir Path dir) throws Exception {
        Path file = TestCorpus.extractFile("geoparquet/examples/example.parquet", dir);

        Map<String, Object> params = new HashMap<>();
        params.put("filetype", "geoparquet");
        params.put("uri", file.toUri().toString());

        GeoParquetDataStoreFactory factory = new GeoParquetDataStoreFactory();
        assertThat(factory.canProcess(params)).isTrue();

        DataStore store = DataStoreFinder.getDataStore(params);
        assertThat(store).isInstanceOf(GeoParquetDataStore.class);
        try {
            assertThat(store.getTypeNames()).hasSize(1);
        } finally {
            store.dispose();
        }
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
}
