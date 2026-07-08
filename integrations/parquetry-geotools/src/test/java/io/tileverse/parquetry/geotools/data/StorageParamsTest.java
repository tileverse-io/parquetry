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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.geotools.api.data.DataAccessFactory.Param;
import org.geotools.api.data.Parameter;
import org.junit.jupiter.api.Test;

class StorageParamsTest {

    private static Optional<Param> find(String key) {
        return StorageParams.PROVIDER_PARAMS.stream()
                .filter(p -> p.key.equals(key))
                .findFirst();
    }

    @Test
    void includesProviderSelectorWithOptions() {
        Param provider = find("storage.provider").orElseThrow();
        @SuppressWarnings("unchecked")
        List<Object> options = (List<Object>) provider.metadata.get(Parameter.OPTIONS);
        assertThat(options).contains("s3", "azure", "gcs", "http");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void optionListsAreSortableInPlace() {
        // GeoServer's ParamInfo sorts a param's OPTIONS list in place via Collections.sort; an
        // immutable list (Stream.toList) throws UnsupportedOperationException and breaks the store
        // edit page. Every OPTIONS list must support in-place sorting.
        for (Param param : StorageParams.PROVIDER_PARAMS) {
            Object options = param.metadata == null ? null : param.metadata.get(Parameter.OPTIONS);
            if (options instanceof List<?> list && !list.isEmpty()) {
                assertThatCode(() -> Collections.sort((List) options))
                        .as("OPTIONS for param '%s' must be sortable in place", param.key)
                        .doesNotThrowAnyException();
            }
        }
    }

    @Test
    void tooltipUsesTheFullDescription() {
        // GeoServer's store edit page shows the Param title as the field tooltip (the label comes from
        // GeoServerApplication.properties). The tooltip must be the full description, not the short title.
        Param endpoint = find("storage.s3.endpoint").orElseThrow();
        assertThat(endpoint.title.toString()).isEqualTo(endpoint.description.toString());
        assertThat(endpoint.title.toString()).contains("S3-compatible");
    }

    @Test
    void backendOptionsAreLists() {
        Object regionOptions = find("storage.s3.region").orElseThrow().metadata.get(Parameter.OPTIONS);
        assertThat(regionOptions).isInstanceOf(List.class);
        assertThat((List<?>) regionOptions).isNotEmpty();
    }

    @Test
    void parameterKeysAreDistinct() {
        List<String> keys =
                StorageParams.PROVIDER_PARAMS.stream().map(p -> p.key).toList();
        assertThat(keys).doesNotHaveDuplicates();
    }

    @Test
    void includesBackendParams() {
        assertThat(find("storage.s3.region")).isPresent();
        assertThat(find("storage.azure.sas-token")).isPresent();
        assertThat(find("storage.http.bearer-token")).isPresent();
        assertThat(find("storage.gcs.project-id")).isPresent();
    }

    @Test
    void marksSecretsAsPassword() {
        assertThat(find("storage.s3.aws-secret-access-key")
                        .orElseThrow()
                        .metadata
                        .get(Parameter.IS_PASSWORD))
                .isEqualTo(Boolean.TRUE);
        assertThat(find("storage.azure.account-key").orElseThrow().metadata.get(Parameter.IS_PASSWORD))
                .isEqualTo(Boolean.TRUE);
        assertThat(find("storage.http.password").orElseThrow().metadata.get(Parameter.IS_PASSWORD))
                .isEqualTo(Boolean.TRUE);
    }

    @Test
    void exposesOnlyCachingEnabled() {
        assertThat(find("storage.caching.enabled")).isPresent();
        assertThat(find("storage.caching.blockaligned")).isEmpty();
        assertThat(find("storage.caching.blocksize")).isEmpty();
    }

    @Test
    void allStorageParamsAreAdvancedAndOptional() {
        assertThat(StorageParams.PROVIDER_PARAMS)
                .allSatisfy(p -> assertThat(p.metadata.get(Parameter.LEVEL)).isEqualTo("advanced"))
                .allSatisfy(p -> assertThat(p.required).isFalse());
    }

    @Test
    void toPropertiesCopiesStorageKeysOnly() {
        Map<String, Object> params = Map.of("geoparquet", "s3://b/x.parquet", "storage.s3.region", "us-west-2");
        Properties props = StorageParams.toProperties(params);
        assertThat(props.getProperty("storage.s3.region")).isEqualTo("us-west-2");
        assertThat(props.stringPropertyNames()).containsExactly("storage.s3.region");
    }

    @Test
    void withStorageParamsNoProviderHidesOnlyTheProviderSelector() {
        Param core = new Param("core", String.class, "core", true);
        List<String> withProvider = keysOf(StorageParams.withStorageParams(core));
        List<String> withoutProvider = keysOf(StorageParams.withStorageParamsNoProvider(core));

        assertThat(withProvider).contains("storage.provider");
        assertThat(withoutProvider).doesNotContain("storage.provider");
        assertThat(withoutProvider).contains("core");
        assertThat(withoutProvider)
                .containsAll(withProvider.stream()
                        .filter(key -> !key.equals("storage.provider"))
                        .toList());
    }

    private static List<String> keysOf(Param[] params) {
        return Arrays.stream(params).map(param -> param.key).toList();
    }
}
