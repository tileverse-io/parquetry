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
package io.tileverse.parquetry.tileverse;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.jupiter.api.Test;

class ParquetStorageTest {

    @Test
    void appliesCacheDefaultsWhenCallerLeavesThemUnset() {
        Properties tuned = ParquetStorage.withParquetDefaults(new Properties());

        assertThat(tuned.getProperty("storage.caching.enabled")).isEqualTo("true");
        assertThat(tuned.getProperty("storage.caching.blockaligned")).isEqualTo("false");
    }

    @Test
    void callerValuesOverrideTheDefaults() {
        Properties caller = new Properties();
        caller.setProperty("storage.caching.enabled", "false");
        caller.setProperty("storage.caching.blockaligned", "true");

        Properties tuned = ParquetStorage.withParquetDefaults(caller);

        assertThat(tuned.getProperty("storage.caching.enabled")).isEqualTo("false");
        assertThat(tuned.getProperty("storage.caching.blockaligned")).isEqualTo("true");
    }

    @Test
    void preservesUnrelatedCallerProperties() {
        Properties caller = new Properties();
        caller.setProperty("storage.http.timeout-millis", "5000");

        Properties tuned = ParquetStorage.withParquetDefaults(caller);

        assertThat(tuned.getProperty("storage.http.timeout-millis")).isEqualTo("5000");
        assertThat(tuned.getProperty("storage.caching.blockaligned")).isEqualTo("false");
    }
}
