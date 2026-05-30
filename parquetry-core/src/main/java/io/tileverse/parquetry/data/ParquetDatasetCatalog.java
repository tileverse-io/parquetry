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
package io.tileverse.parquetry.data;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;

/**
 * Reads a logical dataset spread across the files of a {@link FileSource} as one {@link ParquetDataset}. This first
 * increment resolves a base location to a single named dataset whose files share a schema by equality. The catalog owns
 * the byte sources it opens and the {@link FileSource}; {@link #close()} releases both.
 *
 * <p>Each file's {@link FileEntry#relativePath() relative path} is the basis for hive-partition parsing and partition
 * columns (a later increment).
 *
 * <p>{@link #open(FileSource, CatalogOptions) open} eagerly opens every file and reads every footer, holding one byte
 * source per file open for the catalog's lifetime. Open-handle count and footer reads scale with the number of files; a
 * lazy, bounded version is a later increment.
 */
public final class ParquetDatasetCatalog implements AutoCloseable {

    private final FileSource source;
    private final List<ByteRangeSource> openSources;
    private final String name;
    private final ParquetDataset dataset;

    private ParquetDatasetCatalog(
            FileSource source, List<ByteRangeSource> openSources, String name, ParquetDataset dataset) {
        this.source = source;
        this.openSources = openSources;
        this.name = name;
        this.dataset = dataset;
    }

    /**
     * Opens a catalog over {@code source}. Every file is opened immediately and all files must share a schema by
     * equality.
     *
     * @throws IllegalArgumentException if {@code source} lists no files
     * @throws io.tileverse.parquetry.format.ParquetFormatException if any footer fails to conform to the spec
     * @throws java.io.UncheckedIOException if any source fails to deliver bytes
     */
    public static ParquetDatasetCatalog open(FileSource source, CatalogOptions options) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(options, "options");

        List<FileEntry> files = listSorted(source);
        if (files.isEmpty()) {
            throw new IllegalArgumentException("no files found at " + source.root());
        }

        List<ByteRangeSource> opened = new ArrayList<>(files.size());
        try {
            for (FileEntry file : files) {
                opened.add(file.open());
            }
            ParquetDataset dataset = ParquetDataset.open(new PreOpenedFileset(opened));
            String name = options.datasetName().orElseGet(() -> deriveName(files, source.root()));
            return new ParquetDatasetCatalog(source, opened, name, dataset);
        } catch (RuntimeException failure) {
            RuntimeException cleanupFailure = closeAll(opened, source);
            if (cleanupFailure != null) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    /** Returns the names of the datasets available through this catalog (currently always a single-element list). */
    public List<String> datasetNames() {
        return List.of(name);
    }

    /**
     * Returns the dataset identified by {@code datasetName}.
     *
     * @throws IllegalArgumentException if no dataset with that name exists in this catalog
     */
    public ParquetDataset dataset(String datasetName) {
        if (!name.equals(datasetName)) {
            throw new IllegalArgumentException("no dataset named '" + datasetName + "' (have '" + name + "')");
        }
        return dataset;
    }

    /** Closes all open byte sources and the underlying {@link FileSource}, even when some closes fail. */
    @Override
    public void close() {
        RuntimeException failure = closeAll(openSources, source);
        if (failure != null) {
            throw failure;
        }
    }

    private static List<FileEntry> listSorted(FileSource source) {
        try (Stream<FileEntry> files = source.list()) {
            return files.sorted(Comparator.comparing(FileEntry::relativePath)).toList();
        }
    }

    /**
     * Derives a dataset name from the file list or root URI.
     *
     * <p>When the source holds exactly one file, the name is derived from that file's relative path (extension
     * stripped) rather than the root URI. This is necessary because
     * {@link io.tileverse.parquetry.io.LocalFileSource#file} sets {@code root} to the parent directory, whose name is
     * opaque (e.g. a JUnit temp dir), not the file itself.
     *
     * <p>For multi-file sources, the last component of the root URI is used as the dataset name.
     */
    private static String deriveName(List<FileEntry> files, URI root) {
        if (files.size() == 1) {
            return stripExtension(files.get(0).relativePath());
        }
        String path = root.getPath();
        if (path == null || path.isEmpty()) {
            return "dataset";
        }
        String trimmed = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        String last = trimmed.substring(trimmed.lastIndexOf('/') + 1);
        return last.isEmpty() ? "dataset" : last;
    }

    private static String stripExtension(String filename) {
        String base = filename.substring(filename.lastIndexOf('/') + 1);
        int dot = base.lastIndexOf('.');
        return dot > 0 ? base.substring(0, dot) : base;
    }

    /**
     * Closes every byte source and then the file source, attempting all of them even when one fails. Returns the first
     * failure with any later failures attached as suppressed, or {@code null} when every close succeeded.
     */
    private static RuntimeException closeAll(List<ByteRangeSource> sources, FileSource fileSource) {
        RuntimeException failure = null;
        for (ByteRangeSource src : sources) {
            failure = closeChaining(src, failure);
        }
        return closeChaining(fileSource, failure);
    }

    private static RuntimeException closeChaining(AutoCloseable closeable, RuntimeException accumulated) {
        try {
            closeable.close();
            return accumulated;
        } catch (RuntimeException e) {
            return chain(accumulated, e);
        } catch (Exception e) {
            return chain(accumulated, new IllegalStateException("closing " + closeable, e));
        }
    }

    private static RuntimeException chain(RuntimeException accumulated, RuntimeException next) {
        if (accumulated == null) {
            return next;
        }
        accumulated.addSuppressed(next);
        return accumulated;
    }

    /** A {@link FilesetReader} over byte sources the catalog already opened (and owns). */
    private record PreOpenedFileset(List<ByteRangeSource> sources) implements FilesetReader {
        @Override
        public ByteRangeSource openFile(int index) {
            return sources.get(index);
        }

        @Override
        public int fileCount() {
            return sources.size();
        }
    }
}
