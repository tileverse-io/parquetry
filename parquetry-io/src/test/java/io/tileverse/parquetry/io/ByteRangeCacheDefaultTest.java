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
package io.tileverse.parquetry.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.cache.CachingRangeReader;

/**
 * The tileverse default relied on by {@link FileSource#open}: a Storage opened without {@code storage.caching.enabled}
 * hands out readers with no byte-range cache, and a deployment setting it gets one.
 */
class ByteRangeCacheDefaultTest {

    /**
     * An HTTP container, because the local-file provider registers no caching parameters at all and answers the same
     * whatever the property says. Opening it and asking for a reader touches no network: an HTTP reader fetches its
     * metadata on the first read, and these cases never issue one.
     */
    private static final URI CONTAINER = URI.create("http://localhost:1/parquet/");

    @Test
    void aDefaultOpenLeavesTheByteRangeCacheOff() throws IOException {
        try (Storage storage = StorageFactory.open(CONTAINER, new Properties());
                RangeReader reader = storage.openRangeReader("data.parquet")) {
            assertThat(reader).isNotInstanceOf(CachingRangeReader.class);
        }
    }

    @Test
    void aDeploymentThatAsksForTheByteRangeCacheGetsIt() throws IOException {
        Properties deployment = new Properties();
        deployment.setProperty("storage.caching.enabled", "true");

        try (Storage storage = StorageFactory.open(CONTAINER, deployment);
                RangeReader reader = storage.openRangeReader("data.parquet")) {
            assertThat(reader).isInstanceOf(CachingRangeReader.class);
        }
    }
}
