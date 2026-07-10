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
package io.tileverse.parquetry.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.cli.UriResolver.Target;

class UriResolverTest {

    @Test
    void opensABarePathRelativeToCwd(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("hello.bin");
        Files.write(file, new byte[] {1, 2, 3, 4});
        try (UriResolver.OpenFile open = UriResolver.open(file.toString())) {
            assertThat(open.source().size()).isEqualTo(4L);
        }
    }

    @Test
    void opensAFileUri(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("hello.bin");
        Files.write(file, new byte[] {9, 9});
        try (UriResolver.OpenFile open = UriResolver.open(file.toUri().toString())) {
            assertThat(open.source().size()).isEqualTo(2L);
        }
    }

    @Test
    void opensABarePathContainingSpaces(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("my data.bin");
        Files.write(file, new byte[] {7, 7, 7});
        try (UriResolver.OpenFile open = UriResolver.open(file.toString())) {
            assertThat(open.source().size()).isEqualTo(3L);
        }
    }

    @Test
    void barePathDerivesParentContainerAndFilenameKey(@TempDir Path dir) {
        Path file = dir.resolve("out.parquet");
        Target target = UriResolver.deriveTarget(file.toString(), "fallback.parquet");
        assertThat(target.container()).isEqualTo(dir.toUri());
        assertThat(target.key()).isEqualTo("out.parquet");
    }

    @Test
    void fileUriDerivesParentContainerAndFilenameKey(@TempDir Path dir) {
        Path file = dir.resolve("out.parquet");
        Target target = UriResolver.deriveTarget(file.toUri().toString(), "fallback.parquet");
        assertThat(target.container()).isEqualTo(dir.toUri());
        assertThat(target.key()).isEqualTo("out.parquet");
    }

    @Test
    void fileUriWithSpacesDecodesFilenameKey(@TempDir Path dir) {
        Path file = dir.resolve("my data.parquet");
        Target target = UriResolver.deriveTarget(file.toUri().toString(), "fallback.parquet");
        assertThat(target.container()).isEqualTo(dir.toUri());
        assertThat(target.key()).isEqualTo("my data.parquet");
    }

    @Test
    void trailingSlashFileUriUsesSourceFileNameAsKey(@TempDir Path dir) {
        String dirUri = dir.toUri().toString();
        assertThat(dirUri).endsWith("/");
        Target target = UriResolver.deriveTarget(dirUri, "source.parquet");
        assertThat(target.container()).isEqualTo(dir.toUri());
        assertThat(target.key()).isEqualTo("source.parquet");
    }

    @Test
    void existingLocalDirectoryWithoutTrailingSlashUsesSourceFileNameAsKey(@TempDir Path dir) {
        Target target = UriResolver.deriveTarget(dir.toString(), "source.parquet");
        assertThat(target.container()).isEqualTo(dir.toUri());
        assertThat(target.key()).isEqualTo("source.parquet");
    }

    @Test
    void s3ObjectUriDerivesPrefixContainerAndSegmentKey() {
        Target target = UriResolver.deriveTarget("s3://bucket/a/b.parquet", "fallback.parquet");
        assertThat(target.container()).isEqualTo(URI.create("s3://bucket/a/"));
        assertThat(target.key()).isEqualTo("b.parquet");
    }

    @Test
    void s3PrefixUriUsesSourceFileNameAsKey() {
        Target target = UriResolver.deriveTarget("s3://bucket/prefix/", "source.parquet");
        assertThat(target.container()).isEqualTo(URI.create("s3://bucket/prefix/"));
        assertThat(target.key()).isEqualTo("source.parquet");
    }

    @Test
    void opaqueUriWithoutPathIsRejected() {
        assertThatThrownBy(() -> UriResolver.deriveTarget("s3:bucket/key", "source.parquet"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no path segment");
    }

    @Test
    void writesToExplicitLocalFile(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.parquet");
        byte[] payload = {1, 2, 3, 4, 5};
        try (UriResolver.OpenSink sink = UriResolver.openForWrite(file.toString(), "ignored.parquet", false, null)) {
            sink.out().write(payload);
            sink.commit();
        }
        assertThat(file).exists();
        assertThat(Files.readAllBytes(file)).isEqualTo(payload);
    }

    @Test
    void writesToDirectoryDestinationUsingSourceFileName(@TempDir Path dir) throws Exception {
        byte[] payload = {6, 7, 8};
        try (UriResolver.OpenSink sink = UriResolver.openForWrite(dir.toString() + "/", "src.parquet", false, null)) {
            sink.out().write(payload);
            sink.commit();
        }
        Path written = dir.resolve("src.parquet");
        assertThat(written).exists();
        assertThat(Files.readAllBytes(written)).isEqualTo(payload);
    }

    @Test
    void refusesToOverwriteExistingFileByDefault(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.parquet");
        Files.write(file, new byte[] {0});
        String target = file.toString();
        assertThatThrownBy(() -> UriResolver.openForWrite(target, "ignored.parquet", false, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("destination exists")
                .hasMessageContaining(target);
    }

    @Test
    void overwritesExistingFileWhenForced(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.parquet");
        Files.write(file, new byte[] {0, 0, 0, 0, 0, 0});
        byte[] payload = {9, 9};
        try (UriResolver.OpenSink sink = UriResolver.openForWrite(file.toString(), "ignored.parquet", true, null)) {
            sink.out().write(payload);
            sink.commit();
        }
        assertThat(Files.readAllBytes(file)).isEqualTo(payload);
    }

    @Test
    void abortLeavesNoVisibleDestinationFile(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.parquet");
        byte[] payload = {1, 2, 3, 4, 5};
        try (UriResolver.OpenSink sink = UriResolver.openForWrite(file.toString(), "ignored.parquet", false, null)) {
            sink.out().write(payload);
            sink.abort();
        }
        assertThat(file)
                .as("an aborted write never renames its temp file into place")
                .doesNotExist();
    }

    @Test
    void closeWithoutCommitAbortsAndLeavesNoVisibleDestinationFile(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.parquet");
        byte[] payload = {1, 2, 3, 4, 5};
        try (UriResolver.OpenSink sink = UriResolver.openForWrite(file.toString(), "ignored.parquet", false, null)) {
            sink.out().write(payload);
            // No commit(): an unwinding try-with-resources must default to abort, not a silent commit.
        }
        assertThat(file)
                .as("a sink closed without commit() must abort, leaving no visible destination")
                .doesNotExist();
    }
}
