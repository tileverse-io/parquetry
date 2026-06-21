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
import java.util.stream.LongStream;
import java.util.stream.Stream;

import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
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
        return LongStream.range(0, count).toArray();
    }

    /** A deterministic permutation of {@code 0..count-1}; bounds of any contiguous slice span the whole domain. */
    static long[] shuffledIds(int count, long seed) {
        long[] ids = sortedIds(count);
        @SuppressWarnings("java:S2245") // not a security sentitive path, PRNG is ok
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
            ParquetRecordBatchBuilder appender = writer.appender();
            for (long id : ids) {
                appender.setLong(0, id).setDouble(1, id * 0.25);
                appender.endRow();
            }
            appender.flush();
        }
    }

    /** Opens a reader over {@code file}; the returned handle owns the byte source it closes. */
    static OpenDataset open(Path file) {
        ByteRangeSource source = ByteRangeSource.ofFile(file);
        return new OpenDataset(source, ParquetReader.open(source));
    }

    static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException _) {
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
            ParquetRecordBatchBuilder appender = writer.appender();
            for (long id : ids) {
                appender.setLong(0, id);
                for (int col = 0; col < valueColumns; col++) {
                    appender.setDouble(1 + col, id * (col + 1) * 0.5);
                }
                appender.endRow();
            }
            appender.flush();
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

    /** A reader plus the byte source behind it; close it to release both. */
    static final class OpenDataset implements AutoCloseable {

        private final ByteRangeSource source;
        private final ParquetReader reader;

        private OpenDataset(ByteRangeSource source, ParquetReader reader) {
            this.source = source;
            this.reader = reader;
        }

        ParquetReader reader() {
            return reader;
        }

        @Override
        public void close() {
            source.close();
        }
    }
}
