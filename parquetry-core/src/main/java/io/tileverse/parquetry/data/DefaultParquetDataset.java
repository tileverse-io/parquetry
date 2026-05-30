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

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.stream.Stream;

import io.tileverse.parquetry.batch.BatchMaterializer;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.filter.ExplainPlan;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Default {@link ParquetDataset} implementation: a collection of 1..N {@link ParquetReader} instances over files that
 * share the same schema. The read overloads concatenate per-reader streams; {@code rowGroups()} aggregates all row
 * groups and re-assigns sequential indices across readers. {@code count} over more than one file fans the per-file
 * counts out across virtual threads, bounded by the shared fetch and decode budgets in {@link ReadOptions}.
 * {@code explain} remains single-reader only and throws {@link UnsupportedOperationException} when more than one reader
 * is present.
 *
 * <p>Schema check happens in the constructor: every reader must agree on {@link ParquetSchema} by equality.
 */
final class DefaultParquetDataset implements ParquetDataset {

    private final List<ParquetReader> readers;
    private final ParquetSchema schema;
    private final List<RowGroupSummary> rowGroups;

    DefaultParquetDataset(List<ParquetReader> readers) {
        if (readers == null || readers.isEmpty()) {
            throw new IllegalArgumentException("readers must contain at least one ParquetReader");
        }
        ParquetSchema first = readers.get(0).schema();
        for (int i = 1; i < readers.size(); i++) {
            if (!first.equals(readers.get(i).schema())) {
                throw new IllegalArgumentException("all files in a dataset must share one schema; the file at index "
                        + i + " has a schema that differs from the file at index 0");
            }
        }
        this.readers = List.copyOf(readers);
        this.schema = first;
        this.rowGroups = buildRowGroups(this.readers);
    }

    private static List<RowGroupSummary> buildRowGroups(List<ParquetReader> readers) {
        List<RowGroupSummary> aggregated = new ArrayList<>();
        int index = 0;
        for (ParquetReader reader : readers) {
            for (RowGroupSummary group : reader.rowGroups()) {
                aggregated.add(new RowGroupSummary(
                        index++, group.rowCount(), group.totalByteSize(), group.totalCompressedSize()));
            }
        }
        return List.copyOf(aggregated);
    }

    @Override
    public ParquetSchema schema() {
        return schema;
    }

    @Override
    public Map<String, String> keyValueMetadata() {
        return readers.get(0).keyValueMetadata();
    }

    @Override
    public List<RowGroupSummary> rowGroups() {
        return rowGroups;
    }

    @Override
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        return readers.stream().flatMap(reader -> reader.read(predicate, projection, options));
    }

    @Override
    public <T> Stream<T> read(
            Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options) {
        return readers.stream().flatMap(reader -> reader.read(predicate, projection, materializer, options));
    }

    @Override
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        return readers.stream().flatMap(reader -> reader.readBatches(predicate, projection, options));
    }

    @Override
    public <T> Stream<T> readBatches(
            Predicate predicate, Projection projection, BatchMaterializer<T> materializer, ReadOptions options) {
        return readers.stream().flatMap(reader -> reader.readBatches(predicate, projection, materializer, options));
    }

    @Override
    public ExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options) {
        ensureSingleReader();
        return readers.get(0).explain(predicate, projection, options);
    }

    @Override
    public long count(Predicate predicate, ReadOptions options) {
        if (readers.size() == 1) {
            return readers.get(0).count(predicate, options);
        }
        return countConcurrently(predicate, options);
    }

    /**
     * Counts each file on its own virtual thread, overlapping the independent footer reads and residual decodes. The
     * shared fetch budget and decode pool in {@code options} bound total I/O and CPU. The fan-out therefore overlaps
     * latency without oversubscribing those resources, and a failure in any file cancels the rest.
     */
    private long countConcurrently(Predicate predicate, ReadOptions options) {
        try (StructuredTaskScope<Long, Void> scope = StructuredTaskScope.open()) {
            List<Subtask<Long>> counts = new ArrayList<>(readers.size());
            for (ParquetReader reader : readers) {
                counts.add(scope.fork(() -> reader.count(predicate, options)));
            }
            scope.join();
            return sum(counts);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException("Interrupted while counting dataset rows");
            interrupted.initCause(e);
            throw new UncheckedIOException(interrupted);
        } catch (StructuredTaskScope.FailedException e) {
            throw asUnchecked(e.getCause());
        }
    }

    private static long sum(List<Subtask<Long>> counts) {
        long total = 0L;
        for (Subtask<Long> count : counts) {
            total += count.get();
        }
        return total;
    }

    /**
     * Rethrows a per-file count failure with the type the sequential path would have thrown.
     * {@link ParquetReader#count} declares no checked exceptions. The cause is therefore always a
     * {@link RuntimeException} or an {@link Error}.
     */
    private static RuntimeException asUnchecked(Throwable cause) {
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("Counting a dataset file failed", cause);
    }

    private void ensureSingleReader() {
        if (readers.size() > 1) {
            throw new UnsupportedOperationException("explain over multi-file datasets is not implemented yet");
        }
    }
}
