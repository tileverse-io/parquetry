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
package io.tileverse.parquetry.dataset;

import java.util.Optional;
import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * The queryable facade for one dataset reached through a {@link io.tileverse.parquetry.catalog.DatasetCatalog}: a
 * named, schema-bearing, optionally-versioned collection of Parquet files read as one. The read/count/explain methods
 * mirror the single-file engine; the catalog adds the name, capabilities, snapshot, and (later) partition awareness.
 */
public interface ParquetDataset extends ParquetReader {

    String name();

    /** The unified, field-id-aware schema of the dataset. */
    ParquetSchema schema();

    /** The pinned snapshot for a versioned dataset; empty for pure-parquet. */
    Optional<CatalogSnapshot> snapshot();

    DatasetCapabilities capabilities();

    @MustBeClosed
    Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options);

    @MustBeClosed
    <T> Stream<T> read(Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options);

    /**
     * Streams matching rows as columnar batches. The predicate is applied exactly (when record-level filtering is
     * enabled, the default; with it disabled only metadata pruning applies): each emitted batch holds only the rows
     * that satisfy {@code predicate}, narrowed to {@code projection}.
     */
    @MustBeClosed
    Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options);

    long count(Predicate predicate, ReadOptions options);

    /** Explains how a query prunes files and how each kept file is read, without executing the read. */
    DatasetExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options);

    /** Like {@link #explain} but executes the read of each kept file and annotates it with its execution stats. */
    DatasetExplainPlan explainAnalyze(Predicate predicate, Projection projection, ReadOptions options);
}
