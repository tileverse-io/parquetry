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
package io.tileverse.parquetry.testkit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestCorpusTest {

    @TempDir
    Path tempDir;

    @Test
    void extractsADirectorySubtreeFromTheClasspath() throws Exception {
        Path root = TestCorpus.extractDirectory("sample-corpus", tempDir);

        assertThat(Files.readString(root.resolve("hello.txt"))).isEqualTo("hello");
        assertThat(Files.readString(root.resolve("nested/world.txt"))).isEqualTo("world");
    }

    @Test
    void extractsASingleFileFromTheClasspath() throws Exception {
        Path file = TestCorpus.extractFile("sample-corpus/hello.txt", tempDir);

        assertThat(file).isRegularFile();
        assertThat(Files.readString(file)).isEqualTo("hello");
    }

    @Test
    void missingResourceFailsClearly() {
        assertThatThrownBy(() -> TestCorpus.extractFile("does/not/exist.parquet", tempDir))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
