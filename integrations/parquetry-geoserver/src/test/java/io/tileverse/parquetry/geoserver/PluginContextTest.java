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
package io.tileverse.parquetry.geoserver;

import static org.assertj.core.api.Assertions.assertThat;

import org.geoserver.platform.ModuleStatus;
import org.geoserver.platform.ModuleStatusImpl;
import org.geoserver.web.data.resource.DataStorePanelInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.tileverse.parquetry.geotools.GeoParquetDataStoreFactory;

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
