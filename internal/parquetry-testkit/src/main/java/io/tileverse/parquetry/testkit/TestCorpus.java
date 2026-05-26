/*
 * Copyright (c) 2026 Tileverse.io
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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Extracts bundled test corpora from the classpath into a directory on disk, letting tests that need real files (a
 * {@code RangeReader} over a {@code Path}, a CSV sidecar) read them regardless of where the consuming module lives in
 * the build tree.
 *
 * <p>The corpora travel as resources inside this module's jar; a consumer depends on {@code parquetry-testkit} at test
 * scope and passes a JUnit {@code @TempDir}. Extraction works whether the resources are an exploded directory (reactor
 * build) or packed inside the jar.
 *
 * <p>Intentionally free of any {@code parquetry} dependency: the modules that consume it at test scope (including
 * {@code parquetry-core}) would otherwise form a build cycle.
 */
public final class TestCorpus {

    private TestCorpus() {}

    /**
     * Copies the classpath resource directory {@code resourcePath} and its whole subtree into {@code targetDir},
     * preserving the structure, and returns {@code targetDir}.
     *
     * @param resourcePath slash-separated classpath directory, e.g. {@code "geoparquet/test_data"}
     * @param targetDir an existing, writable directory (typically a JUnit {@code @TempDir})
     */
    public static Path extractDirectory(String resourcePath, Path targetDir) {
        URL url = requireResource(resourcePath);
        try {
            URI uri = url.toURI();
            if ("jar".equals(uri.getScheme())) {
                copyFromJar(uri, resourcePath, targetDir);
            } else {
                copyTree(Paths.get(uri), targetDir);
            }
            return targetDir;
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Malformed URI for classpath resource " + resourcePath, e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to extract classpath directory " + resourcePath, e);
        }
    }

    /**
     * Copies the single classpath resource {@code resourcePath} into {@code targetDir}, returning the written file.
     *
     * @param resourcePath slash-separated classpath file, e.g. {@code "parquet-testing/data/alltypes_plain.parquet"}
     * @param targetDir an existing, writable directory (typically a JUnit {@code @TempDir})
     */
    public static Path extractFile(String resourcePath, Path targetDir) {
        Path destination = targetDir.resolve(fileNameOf(resourcePath));
        try (InputStream in = requireStream(resourcePath)) {
            Files.createDirectories(targetDir);
            Files.copy(in, destination, StandardCopyOption.REPLACE_EXISTING);
            return destination;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to extract classpath resource " + resourcePath, e);
        }
    }

    private static void copyFromJar(URI jarUri, String resourcePath, Path targetDir) throws IOException {
        try {
            FileSystem fileSystem = FileSystems.newFileSystem(jarUri, Map.of());
            try {
                copyTree(fileSystem.getPath("/" + resourcePath), targetDir);
            } finally {
                fileSystem.close();
            }
        } catch (FileSystemAlreadyExistsException alreadyOpen) {
            // Another reader already mounted this jar; reuse its file system and leave it open for them.
            FileSystem fileSystem = FileSystems.getFileSystem(jarUri);
            copyTree(fileSystem.getPath("/" + resourcePath), targetDir);
        }
    }

    private static void copyTree(Path source, Path targetDir) throws IOException {
        Files.createDirectories(targetDir);
        try (Stream<Path> entries = Files.walk(source)) {
            for (Path entry : (Iterable<Path>) entries::iterator) {
                Path destination = targetDir.resolve(source.relativize(entry).toString());
                if (Files.isDirectory(entry)) {
                    Files.createDirectories(destination);
                } else {
                    Files.createDirectories(destination.getParent());
                    Files.copy(entry, destination, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private static URL requireResource(String resourcePath) {
        URL url = classLoader().getResource(resourcePath);
        if (url == null) {
            throw new IllegalArgumentException("Classpath resource not found: " + resourcePath);
        }
        return url;
    }

    private static InputStream requireStream(String resourcePath) {
        InputStream in = classLoader().getResourceAsStream(resourcePath);
        if (in == null) {
            throw new IllegalArgumentException("Classpath resource not found: " + resourcePath);
        }
        return in;
    }

    private static ClassLoader classLoader() {
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        return (contextLoader != null) ? contextLoader : TestCorpus.class.getClassLoader();
    }

    private static String fileNameOf(String resourcePath) {
        int lastSlash = resourcePath.lastIndexOf('/');
        return (lastSlash < 0) ? resourcePath : resourcePath.substring(lastSlash + 1);
    }
}
