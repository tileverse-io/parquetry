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
package io.tileverse.parquetry.probes;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.BooleanVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.Int96Vector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.LevelListVector;
import io.tileverse.parquetry.columnar.LevelMapVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.ShreddedVariantVector;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * parquetry's columnar arm: reads the file as vectorized {@link ParquetRecordBatch}es and touches every leaf value with
 * the typed leaf accessors, recursing through {@link ListVector}, {@link MapVector}, {@link StructVector}, and
 * {@link VariantVector} down to the primitive and binary leaves. No records are assembled. The dataset is opened once
 * and reused across scans (the long-lived server model); each batch is closed after it is touched.
 */
final class ParquetryColumnarEngine implements ColumnarEngine {

    private final Path file;
    private final ProbeQueryObserver analyzeObserver;
    private final ReadOptions readOptions;
    private ByteRangeSource source;
    private ParquetFileReader reader;
    private long sink;

    ParquetryColumnarEngine(Path file) {
        this(file, false);
    }

    ParquetryColumnarEngine(Path file, boolean analyze) {
        this.file = file;
        this.analyzeObserver = analyze ? new ProbeQueryObserver() : null;
        this.readOptions = analyze
                ? ReadOptions.DEFAULTS.toBuilder()
                        .queryObserver(analyzeObserver)
                        .build()
                : ReadOptions.DEFAULTS;
    }

    @Override
    public String name() {
        return "parquetry";
    }

    /** The columns to decode from {@code parquetry.probe.columns}, or every column when unset. */
    private static Projection projection() {
        Set<ColumnPath> kept = ProbeColumns.requested().stream()
                .map(name -> ColumnPath.of(name.split("\\.")))
                .collect(Collectors.toSet());
        return kept.isEmpty() ? Projection.ALL : Projection.ofPhysical(kept);
    }

    @Override
    public long scan() {
        try (Stream<ParquetRecordBatch> batches =
                reader().readBatches(Predicate.ALWAYS_TRUE, projection(), readOptions)) {
            long rows = 0L;
            for (ParquetRecordBatch batch : (Iterable<ParquetRecordBatch>) batches::iterator) {
                try (batch) {
                    touchBatch(batch);
                    rows += batch.rowCount();
                }
            }
            return rows;
        }
    }

    @Override
    public long checksum() {
        return sink;
    }

    @Override
    public Optional<QueryStats> analyzeStats() {
        return analyzeObserver == null ? Optional.empty() : analyzeObserver.snapshot();
    }

    @Override
    public void close() {
        if (source == null) {
            return;
        }
        source.close();
        source = null;
        reader = null;
    }

    /** Opened once and reused across scans; synchronized so concurrent scans share a single open reader. */
    private synchronized ParquetFileReader reader() {
        if (reader == null) {
            source = ByteRangeSource.ofFile(file);
            reader = ParquetFileReader.open(source);
        }
        return reader;
    }

    private void touchBatch(ParquetRecordBatch batch) {
        for (ColumnVector column : batch.columns().values()) {
            touchVector(column);
        }
    }

    /** Folds every value of {@code vector} into the sink, descending nested vectors to their leaves. */
    private void touchVector(ColumnVector vector) {
        switch (vector) {
            case IntVector ints -> touchInts(ints);
            case LongVector longs -> touchLongs(longs);
            case FloatVector floats -> touchFloats(floats);
            case DoubleVector doubles -> touchDoubles(doubles);
            case BooleanVector booleans -> touchBooleans(booleans);
            case BinaryVector binaries -> touchBinaries(binaries);
            case FixedLenBinaryVector fixed -> touchFixedLenBinaries(fixed);
            case Int96Vector int96s -> touchInt96s(int96s);
            case ListVector list -> touchVector(list.child());
            case MapVector map -> {
                touchVector(map.keys());
                touchVector(map.values());
            }
            case StructVector struct -> {
                for (ColumnVector child : struct.children().values()) {
                    touchVector(child);
                }
            }
            case VariantVector variantVector -> {
                touchBinaries(variantVector.metadataColumn());
                touchBinaries(variantVector.valueColumn());
            }
            case ShreddedVariantVector _ ->
                throw new UnsupportedOperationException(
                        "the eager probe path does not produce shredded variant vectors");
            case LevelListVector _, LevelMapVector _ ->
                throw new IllegalStateException(
                        "the eager probe path never produces level-backed vectors; they belong to the streaming scan");
        }
    }

    private void touchInts(IntVector vector) {
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            sink += vector.valueAt(row);
        }
    }

    private void touchLongs(LongVector vector) {
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            sink += vector.valueAt(row);
        }
    }

    private void touchFloats(FloatVector vector) {
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            sink += (long) vector.valueAt(row);
        }
    }

    private void touchDoubles(DoubleVector vector) {
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            sink += (long) vector.valueAt(row);
        }
    }

    private void touchBooleans(BooleanVector vector) {
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            sink += vector.valueAt(row) ? 1L : 0L;
        }
    }

    private void touchBinaries(BinaryVector vector) {
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            sink += segmentByteSize(vector.get(row));
        }
    }

    private void touchFixedLenBinaries(FixedLenBinaryVector vector) {
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            sink += segmentByteSize(vector.get(row));
        }
    }

    private void touchInt96s(Int96Vector vector) {
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            sink += segmentByteSize(vector.get(row));
        }
    }

    private static long segmentByteSize(MemorySegment segment) {
        return segment == null ? 0L : segment.byteSize();
    }
}
