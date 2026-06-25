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

import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * The read operations shared by the two dataset facades in this package: the physical multi-file reader
 * ({@link ParquetDataset}) and the catalog dataset ({@link Dataset}). Both expose the same predicate-filtered record
 * reads, the same columnar batch read, and the same metadata-only count. Naming that common contract lets a consumer
 * read either one without knowing which it holds.
 *
 * <p>The explain methods stay out of this contract because the two facades return different plan types, as do the
 * physical reader's file-level methods (row groups, file statistics, key/value metadata, and the query-shaped
 * overloads).
 */
public interface ParquetReader {

    /** The schema of the records this reader produces. */
    ParquetSchema schema();

    /** Reads the rows matching {@code predicate}, projected to {@code projection}, as canonical records. */
    @MustBeClosed
    Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options);

    /** Reads the matching rows, materializing each to {@code T} through {@code materializer}. */
    @MustBeClosed
    <T> Stream<T> read(Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options);

    /**
     * Reads the matching rows as columnar batches. Pushdown-only: row-group and page elimination from the predicate,
     * where a surviving page still holds rows that do not match. A consumer needing exact per-row filtering over the
     * batches applies it on top.
     */
    @MustBeClosed
    Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options);

    /** Counts the matching rows without assembling any record. */
    long count(Predicate predicate, ReadOptions options);
}
