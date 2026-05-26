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
package io.tileverse.parquetry.benchmarks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Shared fixtures for the read-path benchmarks: a two-column {@code id INT64 + value DOUBLE} table, generators for its
 * key column, and a writer/opener pair. Keeping the file shape in one place lets each benchmark focus on the dimension
 * it measures (pruning, filter tiers, ...) instead of repeating schema and lifecycle boilerplate.
 */
final class SyntheticParquet {

    static final ColumnPath ID = ColumnPath.of("id");
    static final ColumnPath VALUE = ColumnPath.of("value");

    private SyntheticParquet() {}

    /** Identity-ordered keys {@code 0..count-1}; per-page and per-row-group bounds are then contiguous. */
    static long[] sortedIds(int count) {
        long[] ids = new long[count];
        for (int i = 0; i < count; i++) {
            ids[i] = i;
        }
        return ids;
    }

    /** A deterministic permutation of {@code 0..count-1}; bounds of any contiguous slice span the whole domain. */
    static long[] shuffledIds(int count, long seed) {
        long[] ids = sortedIds(count);
        Random random = new Random(seed);
        for (int i = ids.length - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            long swap = ids[i];
            ids[i] = ids[j];
            ids[j] = swap;
        }
        return ids;
    }

    /** Writes one file of {@code (id, value=id*0.25)} rows in the given id order, under the supplied write options. */
    static void writeIdValueFile(Path file, WriteOptions options, long[] ids) throws IOException {
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), idValueSchema(), options)) {
            IdValueRow row = new IdValueRow();
            for (long id : ids) {
                row.id = id;
                row.value = id * 0.25;
                writer.write(row);
            }
        }
    }

    /** Opens a dataset over {@code file}; the returned handle owns the storage and reader it closes. */
    static OpenDataset open(Path file) {
        Storage storage = StorageFactory.open(file.getParent().toUri());
        RangeReader reader = storage.openRangeReader(file.getFileName().toString());
        return new OpenDataset(storage, reader, ParquetDataset.open(reader));
    }

    static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                    // best effort cleanup of a trial's scratch directory
                }
            });
        }
    }

    /**
     * Returns the {@link ColumnPath} for the i-th output column in the wide-table schema ({@code v0}, {@code v1}, ...).
     */
    static ColumnPath valueColumn(int i) {
        return ColumnPath.of("v" + i);
    }

    /**
     * Returns a projection over all {@code valueColumns} output columns ({@code v0..v{N-1}}) in the wide schema,
     * deliberately excluding the {@code id} predicate column. This lets the read-path benchmark measure output-column
     * decode work in isolation.
     */
    static Projection wideValueProjection(int valueColumns) {
        Set<ColumnPath> cols = new HashSet<>();
        for (int i = 0; i < valueColumns; i++) {
            cols.add(valueColumn(i));
        }
        return Projection.of(cols);
    }

    /**
     * Writes one file with an {@code id INT64} key column and {@code valueColumns} DOUBLE output columns named
     * {@code v0..v{N-1}}. Each output column value is deterministic: {@code id * (col + 1) * 0.5}. The rows are written
     * in {@code ids} order; pass sorted ids to cluster matches into a few pages and make column-index pruning
     * effective.
     */
    static void writeWideFile(Path file, WriteOptions options, long[] ids, int valueColumns) throws IOException {
        ParquetSchema schema = wideSchema(valueColumns);
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            WideRow row = new WideRow(valueColumns);
            for (long id : ids) {
                row.id = id;
                writer.write(row);
            }
        }
    }

    private static ParquetSchema wideSchema(int valueColumns) {
        List<SchemaNode> leaves = new ArrayList<>(1 + valueColumns);
        leaves.add(required("id", PrimitiveKind.INT64));
        for (int i = 0; i < valueColumns; i++) {
            leaves.add(required("v" + i, PrimitiveKind.DOUBLE));
        }
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, leaves, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema idValueSchema() {
        List<SchemaNode> leaves = List.of(required("id", PrimitiveKind.INT64), required("value", PrimitiveKind.DOUBLE));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, leaves, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive required(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    /** A dataset plus the storage and reader behind it; close it to release all three. */
    static final class OpenDataset implements AutoCloseable {

        private final Storage storage;
        private final RangeReader reader;
        private final ParquetDataset dataset;

        private OpenDataset(Storage storage, RangeReader reader, ParquetDataset dataset) {
            this.storage = storage;
            this.reader = reader;
            this.dataset = dataset;
        }

        ParquetDataset dataset() {
            return dataset;
        }

        @Override
        public void close() throws IOException {
            reader.close();
            storage.close();
        }
    }

    /** Reused across the write loop to avoid allocating a map per row. */
    private static final class IdValueRow implements WriteRow {
        private long id;
        private double value;

        @Override
        public Object value(ColumnPath path) {
            if (path.equals(ID)) {
                return id;
            }
            return value;
        }
    }

    /**
     * Reused across the wide-table write loop. Each output column value is {@code id * (col + 1) * 0.5}, making the
     * values deterministic without any extra per-row allocation.
     */
    private static final class WideRow implements WriteRow {
        private final int valueColumns;
        private long id;

        private WideRow(int valueColumns) {
            this.valueColumns = valueColumns;
        }

        @Override
        public Object value(ColumnPath path) {
            if (path.equals(ID)) {
                return id;
            }
            // paths are "v0", "v1", ... - parse the column index from the dot-joined name
            String name = path.dot();
            int col = Integer.parseInt(name.substring(1));
            return id * (col + 1) * 0.5;
        }
    }
}
