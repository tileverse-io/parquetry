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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StoragePattern;

/**
 * A {@link FileSource} over a local directory (with a glob) or a single file.
 *
 * <p>The glob follows tileverse's {@link StoragePattern} syntax (DuckDB-style, with brace alternation) and is matched
 * against the {@code /}-separated path of every regular file relative to the directory, at any depth; a backslash in
 * the glob is an ordinary character on every platform. A pattern without glob metacharacters names exactly one relative
 * path, where a storage listing would list that prefix unfiltered.
 */
public final class LocalFileSource implements FileSource {

    private final Path root;
    private final Listing listing;

    private LocalFileSource(Path root, Listing listing) {
        this.root = root.toAbsolutePath().normalize();
        this.listing = listing;
    }

    /**
     * A source over every file under {@code directory} matching {@code glob} (e.g. {@code "*.parquet"}).
     *
     * @throws IllegalArgumentException if {@code glob} is empty, starts with {@code /}, has a {@code .} or {@code ..}
     *     segment, contains a NUL character, or does not compile
     */
    public static LocalFileSource directory(Path directory, String glob) {
        Objects.requireNonNull(directory, "directory");
        Objects.requireNonNull(glob, "glob");
        return new LocalFileSource(directory, new Listing.GlobMatches(compile(glob)));
    }

    /**
     * A source over exactly one file; {@link #root()} is the file's parent directory. Listing looks at that file alone,
     * never at its siblings, and lists nothing when the file or its directory is missing.
     */
    public static LocalFileSource file(Path file) {
        Objects.requireNonNull(file, "file");
        Path abs = file.toAbsolutePath().normalize();
        return new LocalFileSource(
                abs.getParent(), new Listing.NamedFile(abs.getFileName().toString()));
    }

    /**
     * The predicate over relative paths for {@code glob}, validated and compiled as tileverse does. A pattern without
     * glob metacharacters has no matcher there, because a storage backend lists that prefix unfiltered; here it names
     * one relative path exactly.
     */
    private static Predicate<String> compile(String glob) {
        if (glob.isEmpty()) {
            throw new IllegalArgumentException("glob must not be empty");
        }
        Storage.requireSafePattern(glob);
        StoragePattern pattern = StoragePattern.parse(glob);
        return pattern.matcher().orElse(glob::equals);
    }

    @Override
    public URI root() {
        return root.toUri();
    }

    @Override
    public Stream<FileEntry> list() {
        return switch (listing) {
            case Listing.GlobMatches(Predicate<String> matcher) -> listMatches(matcher);
            case Listing.NamedFile(String fileName) -> listNamedFile(root.resolve(fileName));
        };
    }

    private Stream<FileEntry> listNamedFile(Path file) {
        try {
            BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class);
            if (!attributes.isRegularFile()) {
                return Stream.empty();
            }
            return Stream.of(new LocalFileEntry(file, relativePathOf(file), attributes.size()));
        } catch (NoSuchFileException _) {
            return Stream.empty();
        } catch (IOException e) {
            throw new UncheckedIOException("Inspecting " + file, e);
        }
    }

    private Stream<FileEntry> listMatches(Predicate<String> matcher) {
        try (Stream<Path> walk = Files.walk(root)) {
            List<FileEntry> matches = walk.filter(Files::isRegularFile)
                    .filter(path -> matcher.test(relativePathOf(path)))
                    .map(this::toFileEntry)
                    .toList();
            return matches.stream();
        } catch (IOException e) {
            throw new UncheckedIOException("Listing files under " + root, e);
        }
    }

    private FileEntry toFileEntry(Path path) {
        String relative = relativePathOf(path);
        long size = sizeOf(path);
        return new LocalFileEntry(path, relative, size);
    }

    private String relativePathOf(Path path) {
        return root.relativize(path).toString().replace('\\', '/');
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

    /** What {@link #list()} reports under {@link #root}: every regular file matching a glob, or one named file. */
    private sealed interface Listing {

        record GlobMatches(Predicate<String> matcher) implements Listing {}

        record NamedFile(String fileName) implements Listing {}
    }

    private record LocalFileEntry(Path path, String relativePath, long sizeBytes) implements FileEntry {
        @Override
        public ByteRangeSource open() {
            return ByteRangeSource.ofFile(path);
        }
    }
}
