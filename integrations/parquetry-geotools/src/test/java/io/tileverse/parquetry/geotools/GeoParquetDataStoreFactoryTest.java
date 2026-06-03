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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
}
