/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 */
package io.tileverse.parquetry.geoserver.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

class StorageParamVisibilityTest {

    @Test
    void groupOfExtractsTheBackendTokenFromTheKeyShape() {
        assertThat(StorageParamVisibility.groupOf("storage.s3.region")).isEqualTo("s3");
        assertThat(StorageParamVisibility.groupOf("storage.provider")).isEqualTo("provider");
        assertThat(StorageParamVisibility.groupOf("storage.caching.enabled")).isEqualTo("caching");
        assertThat(StorageParamVisibility.groupOf("namespace")).isEmpty();
    }

    @Test
    void alwaysVisibleCoversTheCoreParamsAndNothingElse() {
        for (String key : new String[] {"geoparquet", "namespace", "fid", "layer-grouping", "storage.provider"}) {
            assertThat(StorageParamVisibility.isAlwaysVisible(key)).as(key).isTrue();
        }
        assertThat(StorageParamVisibility.isAlwaysVisible("storage.s3.region")).isFalse();
        assertThat(StorageParamVisibility.isAlwaysVisible("storage.caching.enabled"))
                .isFalse();
    }

    @Test
    void showsOnlySelectedGroupsAndAlwaysCore() {
        Set<String> s3 = Set.of("s3");
        assertThat(StorageParamVisibility.isVisible("storage.s3.region", s3)).isTrue();
        assertThat(StorageParamVisibility.isVisible("storage.azure.account-key", s3))
                .isFalse();
        assertThat(StorageParamVisibility.isVisible("storage.azure.account-key", Set.of("azure")))
                .isTrue();
        assertThat(StorageParamVisibility.isVisible("namespace", Set.of())).isTrue();
        assertThat(StorageParamVisibility.isVisible("storage.provider", Set.of()))
                .isTrue();
        assertThat(StorageParamVisibility.isVisible("storage.caching.enabled", s3))
                .isTrue();
        assertThat(StorageParamVisibility.isVisible("storage.caching.enabled", Set.of()))
                .isFalse();
    }

    @Test
    void derivesSelectedGroupsFromPresentParameters() {
        Map<String, String> params = Map.of(
                "geoparquet-stac", "s3://x/i.parquet",
                "storage.s3.region", "us-east-1",
                "storage.caching.enabled", "true");
        assertThat(StorageParamVisibility.selectedGroupsFromParameters(params)).containsExactly("s3");
    }

    @Test
    void coreParamsAlwaysVisibleForTheSelectedProvider() {
        for (String key : new String[] {"geoparquet", "namespace", "fid", "layer-grouping", "storage.provider"}) {
            assertThat(visibleForProvider(key, "")).as(key).isTrue();
            assertThat(visibleForProvider(key, "s3")).as(key).isTrue();
        }
    }

    @Test
    void backendParamsVisibleOnlyForTheirProvider() {
        assertThat(visibleForProvider("storage.s3.region", "s3")).isTrue();
        assertThat(visibleForProvider("storage.s3.region", "azure")).isFalse();
        assertThat(visibleForProvider("storage.azure.sas-token", "azure")).isTrue();
        assertThat(visibleForProvider("storage.gcs.project-id", "gcs")).isTrue();
        assertThat(visibleForProvider("storage.http.bearer-token", "http")).isTrue();
    }

    @Test
    void azureDataLakeShowsTheSharedAzureParams() {
        assertThat(visibleForProvider("storage.azure.account-key", "azure-datalake"))
                .isTrue();
        assertThat(visibleForProvider("storage.s3.region", "azure-datalake")).isFalse();
    }

    @Test
    void cachingVisibleForCloudProvidersOnly() {
        assertThat(visibleForProvider("storage.caching.enabled", "s3")).isTrue();
        assertThat(visibleForProvider("storage.caching.enabled", "http")).isTrue();
        assertThat(visibleForProvider("storage.caching.enabled", "file")).isFalse();
        assertThat(visibleForProvider("storage.caching.enabled", "")).isFalse();
    }

    @Test
    void noBackendParamsWhenProviderBlankOrFile() {
        assertThat(visibleForProvider("storage.s3.region", "")).isFalse();
        assertThat(visibleForProvider("storage.s3.region", "file")).isFalse();
        assertThat(visibleForProvider("storage.azure.account-key", null)).isFalse();
    }

    /**
     * Resolves a single provider id to its backend groups and asks the group-based visibility rule, matching how a
     * single-backend store edit panel decides a field's visibility.
     */
    private static boolean visibleForProvider(String paramKey, String providerId) {
        return StorageParamVisibility.isVisible(paramKey, StorageParamVisibility.groupsForProvider(providerId));
    }
}
