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
package io.tileverse.parquetry.stac;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;

class ContainerStoragesTest {

    @Test
    void cachesOneStoragePerContainerAndReadsAcrossContainers(@TempDir Path tempDir) throws Exception {
        Path containerA = Files.createDirectories(tempDir.resolve("a"));
        Path containerB = Files.createDirectories(tempDir.resolve("b"));
        Files.writeString(containerA.resolve("hello.txt"), "hello");
        Files.writeString(containerB.resolve("world.txt"), "world");

        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            Storage a1 = storages.storageFor(containerA.toUri());
            Storage a2 = storages.storageFor(containerA.toUri());
            Storage b1 = storages.storageFor(containerB.toUri());

            assertThat(a1).isSameAs(a2);
            assertThat(b1).isNotSameAs(a1);
            assertThat(a1.exists("hello.txt")).isTrue();
            assertThat(b1.exists("world.txt")).isTrue();
        }
    }

    @Test
    void dropsForcedProviderLettingEachContainerAutoDetect(@TempDir Path tempDir) throws Exception {
        Files.writeString(tempDir.resolve("hello.txt"), "hello");
        Properties props = new Properties();
        props.setProperty("storage.provider", "s3"); // wrong for a file:// container; must be ignored

        try (ContainerStorages storages = new ContainerStorages(props)) {
            Storage storage = storages.storageFor(tempDir.toUri());
            assertThat(storage.exists("hello.txt")).isTrue();
        }
    }

    @Test
    void dropsLegacyForcedProviderAlias(@TempDir Path tempDir) throws Exception {
        Files.writeString(tempDir.resolve("hello.txt"), "hello");
        Properties props = new Properties();
        // The legacy alias of storage.provider; wrong for a file:// container, must be ignored.
        props.setProperty("io.tileverse.rangereader.provider", "s3");

        try (ContainerStorages storages = new ContainerStorages(props)) {
            Storage storage = storages.storageFor(tempDir.toUri());
            assertThat(storage.exists("hello.txt")).isTrue();
        }
    }
}
