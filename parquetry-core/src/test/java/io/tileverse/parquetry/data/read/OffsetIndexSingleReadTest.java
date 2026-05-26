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
package io.tileverse.parquetry.data.read;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.filter.ExplainPlan;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowGroupOutcome;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies that the predicate column's OffsetIndex is read exactly once per {@code read()} call when the column-index
 * tier narrows the row group to a partial outcome. The column-index tier and the decode-mask builder each need the
 * OffsetIndex; they should share a single cached read via {@link RowGroupChunks} rather than issuing two separate range
 * requests.
 */
class OffsetIndexSingleReadTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath VALUE = ColumnPath.of("value");

    @TempDir
    Path tempDir;

    @Test
    void predicateColumnOffsetIndexIsReadOnceWhenRowGroupIsNarrowed() throws Exception {
        Path file = writeIdValueFile();

        // Locate the id column's OffsetIndex byte offset from the footer.
        long idOffsetIndexOffset = readIdOffsetIndexOffset(file);

        // Confirm the predicate yields a PARTIAL row-group outcome; a FULL outcome means the
        // column-index tier did not activate and the single-read property cannot be demonstrated.
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader plainReader =
                        storage.openRangeReader(file.getFileName().toString())) {
            ExplainPlan plan =
                    ParquetDataset.open(plainReader).explain(col("id").eq(5L), Projection.ALL, ReadOptions.DEFAULTS);
            assertThat(plan.rowGroups().get(0).outcome())
                    .as("predicate must narrow to PARTIAL so the decode-mask builder runs")
                    .isEqualTo(RowGroupOutcome.PARTIAL);
        }

        // Wrap a fresh reader in the counting decorator and drive a full filtered read.
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader base = storage.openRangeReader(file.getFileName().toString())) {
            CountingRangeReader counting = new CountingRangeReader(base, idOffsetIndexOffset);
            Predicate predicate = col("id").eq(5L);
            long rowsMatched;
            try (Stream<ParquetRecord> rows =
                    ParquetDataset.open(counting).read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                rowsMatched = rows.count();
            }

            assertThat(rowsMatched)
                    .as("predicate matched at least one row (sanity: the filter path ran)")
                    .isGreaterThan(0L);

            assertThat(counting.readsAtWatchedOffset())
                    .as("the predicate column's offset index is read once: the column-index tier and the"
                            + " decode-mask builder share one read")
                    .isEqualTo(1);
        }
    }

    // -------------------------------------------------------------------------
    // Footer inspection
    // -------------------------------------------------------------------------

    /**
     * Opens the file on a plain reader, reads the footer, and returns the OffsetIndex byte offset for the {@code id}
     * column chunk in row group 0.
     */
    private long readIdOffsetIndexOffset(Path file) throws IOException {
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            FileMetaData footer = ParquetFormat.readFooter(reader);
            RowGroup rowGroup = footer.rowGroups().get(0);
            ColumnChunk idChunk = findIdChunk(rowGroup);
            return idChunk.offsetIndexOffset()
                    .orElseThrow(() -> new IllegalStateException("writer did not emit an OffsetIndex for id"));
        }
    }

    private static ColumnChunk findIdChunk(RowGroup rowGroup) {
        for (ColumnChunk chunk : rowGroup.columns()) {
            List<String> path = chunk.metaData()
                    .orElseThrow(() -> new IllegalStateException("ColumnChunk missing inline ColumnMetaData"))
                    .pathInSchema();
            if (path.size() == 1 && "id".equals(path.get(0))) {
                return chunk;
            }
        }
        throw new IllegalStateException("id column chunk not found in row group");
    }

    // -------------------------------------------------------------------------
    // File writing
    // -------------------------------------------------------------------------

    /**
     * Writes a single row group with 50 rows, sorted on {@code id} 0..49, using a small page limit so multiple pages
     * are created. Column-index and offset-index structures are written by default.
     */
    private Path writeIdValueFile() throws Exception {
        Path file = tempDir.resolve("id-value.parquet");
        ParquetSchema schema = idValueSchema();
        WriteOptions options =
                WriteOptions.builder().tempDir(tempDir).pageValueLimit(5).build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (int i = 0; i < 50; i++) {
                final long id = i;
                final double value = id * 0.5;
                Map<ColumnPath, Object> rowValues = Map.of(ID, id, VALUE, value);
                writer.write((WriteRow) rowValues::get);
            }
        }
        return file;
    }

    private static ParquetSchema idValueSchema() {
        List<SchemaNode> leaves = List.of(requiredInt64("id"), requiredDouble("value"));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, leaves, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredDouble(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
    }

    // -------------------------------------------------------------------------
    // Counting decorator
    // -------------------------------------------------------------------------

    /**
     * A {@link RangeReader} decorator that counts how many times {@link #readRange(long, int)} (the 2-arg,
     * buffer-allocating overload) is called with an offset equal to the watched byte offset. That overload is what
     * {@link io.tileverse.parquetry.format.ParquetFormat} uses when loading OffsetIndex and ColumnIndex structures;
     * column-data fetches use the 3-arg overload and never hit those offsets.
     */
    private static final class CountingRangeReader implements RangeReader {

        private final RangeReader delegate;
        private final long watchedOffset;
        private final AtomicInteger hits = new AtomicInteger();

        CountingRangeReader(RangeReader delegate, long watchedOffset) {
            this.delegate = delegate;
            this.watchedOffset = watchedOffset;
        }

        /** Returns how many times the 2-arg {@code readRange} was called at the watched offset. */
        int readsAtWatchedOffset() {
            return hits.get();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) {
            if (offset == watchedOffset) {
                hits.incrementAndGet();
            }
            return delegate.readRange(offset, length);
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            return delegate.readRange(offset, length, target);
        }

        @Override
        public OptionalLong size() {
            return delegate.size();
        }

        @Override
        public String getSourceIdentifier() {
            return delegate.getSourceIdentifier();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
