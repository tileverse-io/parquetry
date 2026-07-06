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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.runtime.ParquetRuntime;

class OpenOptionsTest {

    @Test
    void defaultsUseTheDefaultRuntimeAndNoDecryption() {
        OpenOptions opts = OpenOptions.DEFAULTS;
        assertThat(opts.runtime()).isSameAs(ParquetRuntime.defaultRuntime());
        assertThat(opts.decryptionKeyRetriever()).isEmpty();
    }

    @Test
    void builderOverridesRuntime() {
        ParquetRuntime rt = ParquetRuntime.builder().prefetchDepth(1).build();
        OpenOptions opts = OpenOptions.builder().runtime(rt).build();
        assertThat(opts.runtime()).isSameAs(rt);
        assertThat(opts.decryptionKeyRetriever()).isEmpty();
    }
}
