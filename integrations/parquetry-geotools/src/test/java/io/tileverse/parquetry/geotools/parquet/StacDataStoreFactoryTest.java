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
package io.tileverse.parquetry.geotools.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.geotools.api.data.DataAccessFactory.Param;
import org.junit.jupiter.api.Test;

class StacDataStoreFactoryTest {

    @Test
    void parametersOmitTheForcedProviderButKeepBackendParams() {
        Param[] params = new StacDataStoreFactory().getParametersInfo();
        List<String> keys = Arrays.stream(params).map(param -> param.key).toList();

        assertThat(keys).contains("geoparquet-stac", "namespace");
        assertThat(keys).doesNotContain("storage.provider");
        assertThat(keys).anyMatch(key -> key.startsWith("storage.") && !key.equals("storage.provider"));
    }

    @Test
    void createDataStoreReportsAMalformedUriAsIoException() {
        StacDataStoreFactory factory = new StacDataStoreFactory();
        Map<String, Object> params = Map.of(StacDataStoreFactory.STAC_URI.key, "http://host/ bad catalog.json");

        assertThatThrownBy(() -> factory.createDataStore(params))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("geoparquet-stac");
    }
}
