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
package io.tileverse.parquetry.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/** A JDK-only {@link FileSource} over a local directory (with a glob) or a single file. */
public final class LocalFileSource implements FileSource {

    private final Path root;
    private final String glob;
    private final boolean singleFile;

    private LocalFileSource(Path root, String glob, boolean singleFile) {
        this.root = root.toAbsolutePath().normalize();
        this.glob = glob;
        this.singleFile = singleFile;
    }

    /** A source over every file under {@code directory} matching {@code glob} (e.g. {@code "*.parquet"}). */
    public static LocalFileSource directory(Path directory, String glob) {
        Objects.requireNonNull(directory, "directory");
        Objects.requireNonNull(glob, "glob");
        return new LocalFileSource(directory, glob, false);
    }

    /** A source over exactly one file; {@link #root()} is the file's parent directory. */
    public static LocalFileSource file(Path file) {
        Objects.requireNonNull(file, "file");
        Path abs = file.toAbsolutePath().normalize();
        return new LocalFileSource(abs.getParent(), abs.getFileName().toString(), true);
    }

    @Override
    public URI root() {
        return root.toUri();
    }

    @Override
    public Stream<FileEntry> list() {
        PathMatcher matcher = root.getFileSystem().getPathMatcher("glob:" + glob);
        try (Stream<Path> walk = Files.walk(root)) {
            List<FileEntry> matches = walk.filter(Files::isRegularFile)
                    .filter(path ->
                            singleFile ? path.equals(root.resolve(glob)) : matcher.matches(root.relativize(path)))
                    .map(this::toFileEntry)
                    .toList();
            return matches.stream();
        } catch (IOException e) {
            throw new UncheckedIOException("Listing files under " + root, e);
        }
    }

    private FileEntry toFileEntry(Path path) {
        String relative = root.relativize(path).toString().replace('\\', '/');
        long size = sizeOf(path);
        return new LocalFileEntry(path, relative, size);
    }

    private static long sizeOf(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new UncheckedIOException("Sizing " + path, e);
        }
    }

    @Override
    public void close() {
        // nothing to release: list() opens and closes its own directory walk
    }

    private record LocalFileEntry(Path path, String relativePath, long sizeBytes) implements FileEntry {
        @Override
        public ByteRangeSource open() {
            return ByteRangeSource.ofFile(path);
        }
    }
}
