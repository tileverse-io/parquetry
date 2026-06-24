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
package io.tileverse.parquetry.tileverse;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;
import io.tileverse.parquetry.io.LocalFileSource;

class ParquetFileSourcesTest {

    @TempDir
    Path tempDir;

    @Test
    void fileSchemeUriRoutesToNativeLocalSource() throws Exception {
        Files.createFile(tempDir.resolve("a.parquet"));
        Files.createFile(tempDir.resolve("b.parquet"));
        Files.createFile(tempDir.resolve("ignore.txt"));

        FileSource source = ParquetFileSources.open(tempDir.toUri(), "*.parquet", new Properties());

        assertThat(source).isInstanceOf(LocalFileSource.class);
        assertThat(source.list().map(FileEntry::relativePath)).containsExactlyInAnyOrder("a.parquet", "b.parquet");
    }

    @Test
    void singleFileBraceGlobMatchesViaNativeLocalSource() throws Exception {
        Files.createFile(tempDir.resolve("only.parquet"));
        Files.createFile(tempDir.resolve("other.parquet"));

        FileSource source = ParquetFileSources.open(tempDir.toUri(), "{only.parquet}", new Properties());

        assertThat(source).isInstanceOf(LocalFileSource.class);
        assertThat(source.list().map(FileEntry::relativePath)).containsExactly("only.parquet");
    }

    @Test
    void openObjectOnLocalFileRoutesToNativeLocalSource() throws Exception {
        Files.createFile(tempDir.resolve("only.parquet"));
        Files.createFile(tempDir.resolve("other.parquet"));

        FileSource source =
                ParquetFileSources.openObject(tempDir.resolve("only.parquet").toUri(), new Properties());

        assertThat(source).isInstanceOf(LocalFileSource.class);
        assertThat(source.list().map(FileEntry::relativePath)).containsExactly("only.parquet");
    }
}
