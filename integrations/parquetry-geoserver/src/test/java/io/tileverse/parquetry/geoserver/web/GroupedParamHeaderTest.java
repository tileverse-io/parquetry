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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.geotools.api.data.DataAccessFactory.Param;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStoreFactory;

/**
 * Pins the pure decision behind the group headers: given the store's ordered connection-parameter keys, which keys
 * begin a titled backend group. The parameters arrive contiguous per group, in the order the factory reports them: the
 * core fields, the provider selector, then the backend groups sorted by group then key (azure, caching, gcs, http, s3).
 */
class GroupedParamHeaderTest {

    private static final List<String> ORDERED_KEYS = List.of(
            "geoparquet",
            "namespace",
            "fid",
            "layer-grouping",
            "storage.provider",
            "storage.azure.account-key",
            "storage.azure.endpoint",
            "storage.caching.enabled",
            "storage.gcs.endpoint",
            "storage.gcs.project-id",
            "storage.http.bearer-token",
            "storage.http.username",
            "storage.s3.endpoint",
            "storage.s3.region");

    @Test
    void marksTheFirstKeyOfEachTitledGroupInOrder() {
        Set<String> firstKeys = StorageParamVisibility.firstParamKeysPerGroup(ORDERED_KEYS);
        assertThat(firstKeys)
                .containsExactly(
                        "storage.azure.account-key",
                        "storage.caching.enabled",
                        "storage.gcs.endpoint",
                        "storage.http.bearer-token",
                        "storage.s3.endpoint");
    }

    @Test
    void coreFieldsAndTheProviderSelectorNeverBeginAGroup() {
        Set<String> firstKeys = StorageParamVisibility.firstParamKeysPerGroup(ORDERED_KEYS);
        assertThat(firstKeys).doesNotContain("geoparquet", "namespace", "fid", "layer-grouping", "storage.provider");
    }

    @Test
    void aTitledGroupYieldsOneHeaderEvenWhenAnUntitledFieldInterruptsIt() {
        List<String> interrupted =
                List.of("storage.s3.endpoint", "storage.provider", "storage.s3.region", "storage.azure.endpoint");
        Set<String> firstKeys = StorageParamVisibility.firstParamKeysPerGroup(interrupted);
        assertThat(firstKeys).containsExactly("storage.s3.endpoint", "storage.azure.endpoint");
    }

    @Test
    void titlesEveryBackendGroupTheGeoParquetFactoryReports() {
        Set<String> firstKeys = StorageParamVisibility.firstParamKeysPerGroup(orderedFactoryKeys());
        assertThat(firstKeys.stream().map(StorageParamVisibility::groupOf))
                .containsExactlyInAnyOrder("s3", "azure", "gcs", "http", "caching");
    }

    @Test
    void everyTitledGroupHasASectionLabel() throws IOException {
        Properties labels = sectionLabels();
        Set<String> firstKeys = StorageParamVisibility.firstParamKeysPerGroup(orderedFactoryKeys());
        for (String key : firstKeys) {
            String labelKey = "storage.group." + StorageParamVisibility.groupOf(key);
            assertThat(labels.getProperty(labelKey))
                    .as("missing section label %s", labelKey)
                    .isNotBlank();
        }
    }

    private static List<String> orderedFactoryKeys() {
        Param[] parameters = new GeoParquetDataStoreFactory().getParametersInfo();
        return Arrays.stream(parameters).map(parameter -> parameter.key).toList();
    }

    private static Properties sectionLabels() throws IOException {
        Properties labels = new Properties();
        try (InputStream in = GroupedParamHeaderTest.class.getResourceAsStream("/GeoServerApplication.properties")) {
            labels.load(in);
        }
        return labels;
    }
}
