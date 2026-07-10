/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.catalog.CatalogCapabilities.SchemaSource;

class CatalogCapabilitiesTest {

    @Test
    void builderDefaultsAreSafeFalse() {
        CatalogCapabilities caps = CatalogCapabilities.builder().build();
        assertThat(caps.enumeratesDatasets()).isFalse();
        assertThat(caps.timeTravel()).isFalse();
        assertThat(caps.schemaSource()).isEqualTo(SchemaSource.MERGED_FILES);
    }

    @Test
    void requireTimeTravelThrowsWhenAbsent() {
        CatalogCapabilities caps = CatalogCapabilities.builder().build();
        assertThatThrownBy(caps::requireTimeTravel)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("timeTravel");
    }

    @Test
    void builderSetsFlags() {
        CatalogCapabilities caps = CatalogCapabilities.builder()
                .enumeratesDatasets(true)
                .timeTravel(true)
                .schemaSource(SchemaSource.TABLE_METADATA)
                .build();
        assertThat(caps.enumeratesDatasets()).isTrue();
        assertThat(caps.timeTravel()).isTrue();
        assertThat(caps.schemaSource()).isEqualTo(SchemaSource.TABLE_METADATA);
    }
}
