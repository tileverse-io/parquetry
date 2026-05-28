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
package io.tileverse.parquetry.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;

class UriResolverTest {

    @Test
    void opensABarePathRelativeToCwd(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("hello.bin");
        Files.write(file, new byte[] {1, 2, 3, 4});
        try (UriResolver.OpenFile open = UriResolver.open(file.toString())) {
            RangeReader reader = open.reader();
            assertThat(reader.size().getAsLong()).isEqualTo(4L);
        }
    }

    @Test
    void opensAFileUri(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("hello.bin");
        Files.write(file, new byte[] {9, 9});
        try (UriResolver.OpenFile open = UriResolver.open(file.toUri().toString())) {
            assertThat(open.reader().size().getAsLong()).isEqualTo(2L);
        }
    }

    @Test
    void opensABarePathContainingSpaces(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("my data.bin");
        Files.write(file, new byte[] {7, 7, 7});
        try (UriResolver.OpenFile open = UriResolver.open(file.toString())) {
            assertThat(open.reader().size().getAsLong()).isEqualTo(3L);
        }
    }
}
