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
package io.tileverse.parquetry.geoserver;

import static org.assertj.core.api.Assertions.assertThat;

import org.geoserver.platform.ModuleStatus;
import org.geoserver.platform.ModuleStatusImpl;
import org.geoserver.web.data.resource.DataStorePanelInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.tileverse.parquetry.geoserver.web.GeoParquetDataStoreEditPanel;
import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStoreFactory;

/**
 * Loads the plugin's {@code applicationContext.xml} the same way GeoServer does (Spring bean definitions at the jar
 * root) and asserts the store panel and module-status beans are wired to the GeoParquet factory.
 */
class PluginContextTest {

    private ClassPathXmlApplicationContext context;

    @BeforeEach
    void loadContext() {
        context = new ClassPathXmlApplicationContext("applicationContext.xml");
    }

    @AfterEach
    void closeContext() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void dataStorePanelBindsTheGeoParquetFactory() {
        DataStorePanelInfo panel = context.getBean("geoParquetDataStorePanel", DataStorePanelInfo.class);

        assertThat(panel.getFactoryClass()).isEqualTo(GeoParquetDataStoreFactory.class);
        assertThat(panel.getComponentClass()).isEqualTo(GeoParquetDataStoreEditPanel.class);
        assertThat(panel.getIcon()).isEqualTo("gs-icon-page-white-vector");
    }

    @Test
    void moduleStatusReportsTheCommunityPlugin() {
        ModuleStatusImpl status = context.getBean("parquetryModuleStatus", ModuleStatusImpl.class);

        assertThat(status.getModule()).isEqualTo("gs-parquetry");
        assertThat(status.isAvailable()).isTrue();
        assertThat(status.getCategory()).isEqualTo(ModuleStatus.Category.COMMUNITY);
    }

    @Test
    void factoryIsDiscoverableAndNamed() {
        GeoParquetDataStoreFactory factory = new GeoParquetDataStoreFactory();

        assertThat(factory.getDisplayName()).isEqualTo("GeoParquet");
        assertThat(factory.isAvailable()).isTrue();
    }
}
