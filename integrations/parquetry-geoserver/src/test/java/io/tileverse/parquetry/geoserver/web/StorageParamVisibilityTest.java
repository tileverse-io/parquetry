/*
 * Copyright (c) 2026 Multivers.io
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

import org.junit.jupiter.api.Test;

class StorageParamVisibilityTest {

    @Test
    void coreParamsAlwaysVisible() {
        for (String key : new String[] {"filetype", "uri", "namespace", "fid", "storage.provider"}) {
            assertThat(StorageParamVisibility.isVisible(key, "")).as(key).isTrue();
            assertThat(StorageParamVisibility.isVisible(key, "s3")).as(key).isTrue();
        }
    }

    @Test
    void backendParamsVisibleOnlyForTheirProvider() {
        assertThat(StorageParamVisibility.isVisible("storage.s3.region", "s3")).isTrue();
        assertThat(StorageParamVisibility.isVisible("storage.s3.region", "azure"))
                .isFalse();
        assertThat(StorageParamVisibility.isVisible("storage.azure.sas-token", "azure"))
                .isTrue();
        assertThat(StorageParamVisibility.isVisible("storage.gcs.project-id", "gcs"))
                .isTrue();
        assertThat(StorageParamVisibility.isVisible("storage.http.bearer-token", "http"))
                .isTrue();
    }

    @Test
    void cachingVisibleForCloudProvidersOnly() {
        assertThat(StorageParamVisibility.isVisible("storage.caching.enabled", "s3"))
                .isTrue();
        assertThat(StorageParamVisibility.isVisible("storage.caching.enabled", "http"))
                .isTrue();
        assertThat(StorageParamVisibility.isVisible("storage.caching.enabled", "file"))
                .isFalse();
        assertThat(StorageParamVisibility.isVisible("storage.caching.enabled", ""))
                .isFalse();
    }

    @Test
    void noBackendParamsWhenProviderBlankOrFile() {
        assertThat(StorageParamVisibility.isVisible("storage.s3.region", "")).isFalse();
        assertThat(StorageParamVisibility.isVisible("storage.s3.region", "file"))
                .isFalse();
        assertThat(StorageParamVisibility.isVisible("storage.azure.account-key", null))
                .isFalse();
    }
}
