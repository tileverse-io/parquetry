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

import java.io.InputStream;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import io.tileverse.storage.spi.StorageProvider;

class StorageProviderLabelsTest {

    @Test
    void everyRegisteredProviderHasALabel() throws Exception {
        Properties labels = new Properties();
        try (InputStream in = getClass().getResourceAsStream("/GeoServerApplication.properties")) {
            labels.load(in);
        }
        for (StorageProvider provider : StorageProvider.getProviders()) {
            String key = "storage.provider." + provider.getId();
            assertThat(labels.getProperty(key)).as("missing label %s", key).isNotBlank();
        }
    }

    @Test
    void azureDataLakeReusesTheAzureParameterGroup() {
        assertThat(StorageParamVisibility.groupsForProvider("azure-datalake")).containsExactly("azure");
        assertThat(StorageParamVisibility.groupsForProvider("s3")).containsExactly("s3");
        assertThat(StorageParamVisibility.selectableGroups()).containsExactly("azure", "gcs", "http", "s3");
    }
}
