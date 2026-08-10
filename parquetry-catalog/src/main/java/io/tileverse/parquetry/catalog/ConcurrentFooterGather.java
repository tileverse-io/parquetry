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
package io.tileverse.parquetry.catalog;

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;

import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * The concurrent fan-out behind the catalog's footer gather: opens each file's byte source, decodes its footer, and
 * extracts the per-file planning metadata (schema, GeoParquet {@code geo} entry, footer statistics), overlapping up to
 * {@code maxConcurrentFiles} files on virtual threads. Against remote storage those per-file round-trips dominate a
 * sequential loop over many files.
 *
 * <p>A multi-file gather extracts the metadata inside each fork and lets the decoded footer go: peak footer heap is
 * bounded by the fan-out width, not the file count, and the returned files hold only the byte source and the compact
 * metadata. A one-file gather also hands back the parsed source, letting the one-file dataset keep its already-decoded
 * footer instead of paying a second fetch and decode.
 *
 * <p>A per-file failure is captured, not raced: an open failure is thrown after every fork completes, lowest index
 * first, with every opened sibling source closed beforehand; a footer-decode failure is handed back in its file's
 * entry, letting the caller's index-order walk decide which failure a broken dataset reports. The interrupted and error
 * exits close every opened source the same way. Lives in its own class, not on {@link FilesetCatalog}, keeping the
 * catalog's class file free of preview API references for consumers compiled without preview features.
 */
final class ConcurrentFooterGather {

    private ConcurrentFooterGather() {}

    /**
     * One gathered file: its open byte source (owned by the caller from here on) and the planning metadata extracted
     * from its footer. {@code geoJson} is the raw {@code "geo"} key-value entry, or null when the file has none.
     * {@code parsed} is present only for a one-file gather, whose dataset reuses the already-decoded footer. A file
     * whose footer failed to decode has {@code footerFailure} set and no metadata: the caller throws it when its own
     * index-order walk reaches the file, keeping the reported failure the one a sequential loop over the listing would
     * have hit first.
     */
    record GatheredFile(
            ByteRangeSource source,
            ParquetSchema schema,
            String geoJson,
            FileStats fileStats,
            Optional<ParquetSource> parsed,
            RuntimeException footerFailure) {

        private static GatheredFile extracted(ByteRangeSource source, ParquetSource parsed, boolean keepParsed) {
            return new GatheredFile(
                    source,
                    parsed.schema(),
                    parsed.keyValueMetadata().get("geo"),
                    parsed.fileStats(),
                    keepParsed ? Optional.of(parsed) : Optional.empty(),
                    null);
        }

        private static GatheredFile failed(ByteRangeSource source, RuntimeException footerFailure) {
            return new GatheredFile(source, null, null, null, Optional.empty(), footerFailure);
        }
    }

    /** One fork's outcome: the gathered file (metadata or deferred footer failure), or the open failure. */
    private record Outcome(GatheredFile gathered, RuntimeException openFailure) {}

    /**
     * Gathers every file's footer metadata, returning one entry per file in {@code files}' index order. An open failure
     * closes every byte source that did open and throws the lowest-index one - the file a sequential loop over the
     * listing would have reported. A footer-decode failure is returned in its file's entry instead of thrown, keeping
     * the caller's index-order walk (and its own per-file checks) in charge of which failure a broken dataset reports.
     * On success the caller owns the sources, including those of footer-failed entries.
     */
    static List<GatheredFile> gather(List<FileEntry> files) {
        if (files.size() == 1) {
            return List.of(gatherSingle(files.get(0)));
        }
        Semaphore openSlots = new Semaphore(OpenOptions.DEFAULTS.runtime().maxConcurrentFiles());
        Queue<ByteRangeSource> opened = new ConcurrentLinkedQueue<>();
        try (StructuredTaskScope<Outcome, Void> scope = StructuredTaskScope.open()) {
            List<Subtask<Outcome>> outcomes = new ArrayList<>(files.size());
            for (FileEntry file : files) {
                outcomes.add(scope.fork(() -> gatherOne(file, openSlots, opened)));
            }
            scope.join();
            return gatheredOrThrow(outcomes, opened);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException("Interrupted while opening dataset files");
            interrupted.initCause(e);
            UncheckedIOException failure = new UncheckedIOException(interrupted);
            closeAll(opened, failure);
            throw failure;
        } catch (StructuredTaskScope.FailedException e) {
            throw closeAllAndRethrow(opened, e.getCause());
        }
    }

    /** The sequential one-file gather; the returned file keeps its parsed source for the dataset to reuse. */
    private static GatheredFile gatherSingle(FileEntry file) {
        ByteRangeSource source = file.open();
        try {
            return GatheredFile.extracted(source, ParquetSource.open(source), true);
        } catch (RuntimeException | Error failure) {
            closeSuppressing(source, failure);
            throw failure;
        }
    }

    /**
     * Opens and extracts one file on its fan-out thread, capturing any {@link RuntimeException} as this file's outcome.
     * The opened byte source is registered in {@code opened} before the footer decode: the registry is what the
     * exceptional exits close, covering this source even when its own decode is the failure.
     */
    private static Outcome gatherOne(FileEntry file, Semaphore openSlots, Queue<ByteRangeSource> opened)
            throws InterruptedException {
        openSlots.acquire();
        try {
            ByteRangeSource source;
            try {
                source = file.open();
            } catch (RuntimeException failure) {
                return new Outcome(null, failure);
            }
            opened.add(source);
            try {
                return new Outcome(GatheredFile.extracted(source, ParquetSource.open(source), false), null);
            } catch (RuntimeException failure) {
                return new Outcome(GatheredFile.failed(source, failure), null);
            }
        } finally {
            openSlots.release();
        }
    }

    /**
     * Returns one gathered entry per file in index order, or closes every opened source and throws the lowest-index
     * open failure - the file a sequential loop over the listing would have reported.
     */
    private static List<GatheredFile> gatheredOrThrow(List<Subtask<Outcome>> outcomes, Queue<ByteRangeSource> opened) {
        RuntimeException openFailure = null;
        List<GatheredFile> gathered = new ArrayList<>(outcomes.size());
        for (Subtask<Outcome> outcome : outcomes) {
            Outcome result = outcome.get();
            if (openFailure == null && result.openFailure() != null) {
                openFailure = result.openFailure();
            }
            if (result.gathered() != null) {
                gathered.add(result.gathered());
            }
        }
        if (openFailure == null) {
            return gathered;
        }
        closeAll(opened, openFailure);
        throw openFailure;
    }

    /** Closes every registered source, attaching close failures to {@code failure} as suppressed. */
    private static void closeAll(Queue<ByteRangeSource> opened, Throwable failure) {
        for (ByteRangeSource source : opened) {
            closeSuppressing(source, failure);
        }
    }

    private static void closeSuppressing(ByteRangeSource source, Throwable failure) {
        try {
            source.close();
        } catch (RuntimeException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    /**
     * Closes every registered source, then rethrows a fork failure that escaped the per-file capture with the type the
     * sequential path would have thrown. The capture returns every {@link RuntimeException} as an outcome: what reaches
     * the scope is an {@link Error} or the {@link InterruptedException} of a cancelled sibling, with anything else
     * falling back to {@link IllegalStateException}. Closing runs before the rethrow on every branch, keeping the error
     * exit leak-free.
     */
    private static RuntimeException closeAllAndRethrow(Queue<ByteRangeSource> opened, Throwable cause) {
        if (cause instanceof RuntimeException runtime) {
            closeAll(opened, runtime);
            return runtime;
        }
        if (cause instanceof Error error) {
            closeAll(opened, error);
            throw error;
        }
        IllegalStateException fallback = new IllegalStateException("Opening a dataset file failed", cause);
        closeAll(opened, fallback);
        return fallback;
    }
}
