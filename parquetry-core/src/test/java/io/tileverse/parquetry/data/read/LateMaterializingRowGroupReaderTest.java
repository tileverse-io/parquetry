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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RecordLevelEvaluator;
import io.tileverse.parquetry.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.record.BatchRowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

import io.tileverse.io.ByteBufferPool;

/** Drives the two-phase late-materializing reader over a real single-row-group, multi-page chunk. */
class LateMaterializingRowGroupReaderTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath V = ColumnPath.of("v");
    private static final ColumnPath NAME = ColumnPath.of("name");

    private static final int ROW_COUNT = 40;

    @TempDir
    Path tempDir;

    @Test
    void decodesOnlyMatchingRowsForOutputOnlyProjection() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        // id >= 10 AND id < 20 -> ten matching rows in the middle of the row group.
        Predicate predicate = Pred.and(Pred.col(ID).gtEq(10L), Pred.col(ID).lt(20L));
        ParquetSchema outputSchema = fileSchema.project(Set.of(V, NAME));

        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ReaderFixture fixture = openFixture(reader, fileSchema, predicate);
            try (RowGroupFetch fetch = fixture.fetch()) {
                LateMaterializingRowGroupReader lateReader = new LateMaterializingRowGroupReader(
                        fetch.columns(),
                        fileSchema,
                        outputSchema,
                        Predicate.columns(predicate),
                        predicate,
                        OptionalInt.empty(),
                        Optional.empty(),
                        fixture.offsetIndexes(),
                        fixture.numRows());

                List<MaterializedRow> actual = drain(lateReader.decodeAll(), outputSchema);
                List<MaterializedRow> expected = bruteForce(predicate, outputSchema);

                assertThat(actual)
                        .as("late materialization yields exactly the predicate-matching rows, in order")
                        .containsExactlyElementsOf(expected);
                assertThat(actual).as("ten rows match id in [10, 20)").hasSize(10);
            }
        }
    }

    @Test
    void includesPredicateColumnWhenAlsoProjected() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        Predicate predicate = Pred.col(ID).eq(25L);
        ParquetSchema outputSchema = fileSchema.project(Set.of(ID, V));

        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ReaderFixture fixture = openFixture(reader, fileSchema, predicate);
            try (RowGroupFetch fetch = fixture.fetch()) {
                LateMaterializingRowGroupReader lateReader = new LateMaterializingRowGroupReader(
                        fetch.columns(),
                        fileSchema,
                        outputSchema,
                        Predicate.columns(predicate),
                        predicate,
                        OptionalInt.empty(),
                        Optional.empty(),
                        fixture.offsetIndexes(),
                        fixture.numRows());

                List<MaterializedRow> actual = drain(lateReader.decodeAll(), outputSchema);
                List<MaterializedRow> expected = bruteForce(predicate, outputSchema);

                assertThat(actual)
                        .as("predicate column appears in the output with its matching value")
                        .containsExactlyElementsOf(expected);
                assertThat(actual).hasSize(1);
                assertThat(actual.get(0).id())
                        .as("the projected id carries the matched value")
                        .isEqualTo(25L);
            }
        }
    }

    @Test
    void emptyWhenNoRowMatches() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        Predicate predicate = Pred.col(ID).eq((long) ROW_COUNT + 100L);
        ParquetSchema outputSchema = fileSchema.project(Set.of(V, NAME));

        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ReaderFixture fixture = openFixture(reader, fileSchema, predicate);
            try (RowGroupFetch fetch = fixture.fetch()) {
                LateMaterializingRowGroupReader lateReader = new LateMaterializingRowGroupReader(
                        fetch.columns(),
                        fileSchema,
                        outputSchema,
                        Predicate.columns(predicate),
                        predicate,
                        OptionalInt.empty(),
                        Optional.empty(),
                        fixture.offsetIndexes(),
                        fixture.numRows());

                List<ParquetRecordBatch> batches = lateReader.decodeAll();

                assertThat(batches)
                        .as("no matching row yields zero output batches")
                        .isEmpty();
            }
        }
    }

    // --- fixture assembly ---

    private ReaderFixture openFixture(RangeReader reader, ParquetSchema fileSchema, Predicate predicate) {
        FileMetaData footer = ParquetFormat.readFooter(reader);
        assertThat(footer.rowGroups()).as("test file holds a single row group").hasSize(1);
        RowGroup rowGroup = footer.rowGroups().get(0);

        IndexSectionLoader loader = indexLoader(reader);
        RowGroupChunks chunks = RowGroupChunks.of(rowGroup, fileSchema, loader);

        Set<ColumnPath> scanLeaves = scanLeaves(fileSchema, predicate);
        ParquetSchema scanSchema = fileSchema.project(scanLeaves);

        Map<ColumnPath, OffsetIndex> offsetIndexes = LinkedHashMap.newLinkedHashMap(scanLeaves.size());
        for (ColumnPath leaf : scanSchema.leafColumns()) {
            offsetIndexes.put(leaf, chunks.offsetIndex(leaf).orElseThrow());
        }

        RowGroupFetcher fetcher =
                new RowGroupFetcher(reader, fileSchema, scanSchema, ByteBufferPool.getDefault(), 1 << 20, 8 << 20);
        RowGroupSurvivor survivor = RowGroupSurvivor.full(chunks);
        return new ReaderFixture(fetcher, survivor, offsetIndexes, chunks.numRows());
    }

    private static Set<ColumnPath> scanLeaves(ParquetSchema fileSchema, Predicate predicate) {
        Set<ColumnPath> scan = new java.util.LinkedHashSet<>(fileSchema.leafColumns());
        scan.addAll(Predicate.columns(predicate));
        return scan;
    }

    private static IndexSectionLoader indexLoader(RangeReader reader) {
        return new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                return ParquetFormat.readOffsetIndex(reader, offset, length);
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                return ParquetFormat.readColumnIndex(reader, offset, length);
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                throw new UnsupportedOperationException("bloom filters not used in this test");
            }
        };
    }

    private record ReaderFixture(
            RowGroupFetcher fetcher,
            RowGroupSurvivor survivor,
            Map<ColumnPath, OffsetIndex> offsetIndexes,
            long numRows) {

        RowGroupFetch fetch() throws Exception {
            return fetcher.fetch(survivor, fetcher.planFor(survivor), BudgetReservation.NONE);
        }
    }

    // --- materialization helpers ---

    private static List<MaterializedRow> drain(List<ParquetRecordBatch> batches, ParquetSchema outputSchema) {
        List<MaterializedRow> rows = new ArrayList<>();
        try {
            for (ParquetRecordBatch batch : batches) {
                for (int row = 0; row < batch.rowCount(); row++) {
                    rows.add(readRow(new BatchRowAccessor(batch, row), outputSchema));
                }
            }
        } finally {
            for (ParquetRecordBatch batch : batches) {
                batch.close();
            }
        }
        return rows;
    }

    private static MaterializedRow readRow(BatchRowAccessor accessor, ParquetSchema outputSchema) {
        List<ColumnPath> projected = outputSchema.leafColumns();
        Long id = projected.contains(ID) ? (Long) accessor.get(ID) : null;
        Double v = projected.contains(V) ? (Double) accessor.get(V) : null;
        String name = projected.contains(NAME) ? asString(accessor.get(NAME)) : null;
        return new MaterializedRow(id, v, name);
    }

    private static String asString(Object value) {
        if (value == null) {
            return null;
        }
        MemorySegment segment = (MemorySegment) value;
        return new String(segment.toArray(JAVA_BYTE), StandardCharsets.UTF_8);
    }

    /**
     * Decodes every row of the file and keeps only those satisfying the predicate, used as the correctness baseline.
     */
    private List<MaterializedRow> bruteForce(Predicate predicate, ParquetSchema outputSchema) throws Exception {
        List<MaterializedRow> matching = new ArrayList<>();
        for (long id = 0; id < ROW_COUNT; id++) {
            WriteValues values = valuesFor(id);
            boolean passes = RecordLevelEvaluator.test(predicate, columnPath -> recordValue(columnPath, values));
            if (passes) {
                Long projectedId = outputSchema.leafColumns().contains(ID) ? values.id() : null;
                Double projectedV = outputSchema.leafColumns().contains(V) ? values.v() : null;
                String projectedName = outputSchema.leafColumns().contains(NAME) ? values.name() : null;
                matching.add(new MaterializedRow(projectedId, projectedV, projectedName));
            }
        }
        return matching;
    }

    private static Object recordValue(ColumnPath path, WriteValues values) {
        if (path.equals(ID)) {
            return values.id();
        }
        if (path.equals(V)) {
            return values.v();
        }
        if (path.equals(NAME)) {
            return MemorySegment.ofArray(values.name().getBytes(StandardCharsets.UTF_8));
        }
        return null;
    }

    private record MaterializedRow(Long id, Double v, String name) {}

    // --- file writing ---

    private Path writeFortyRowsAcrossManyPages() throws Exception {
        Path file = tempDir.resolve("late-materialization.parquet");
        ParquetSchema schema = fileSchema();
        WriteOptions options =
                WriteOptions.builder().tempDir(tempDir).pageValueLimit(4).build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (long id = 0; id < ROW_COUNT; id++) {
                writer.write(row(id));
            }
        }
        return file;
    }

    private static WriteRow row(long id) {
        WriteValues values = valuesFor(id);
        Map<ColumnPath, Object> cells = Map.of(
                ID,
                values.id(),
                V,
                values.v(),
                NAME,
                MemorySegment.ofArray(values.name().getBytes(StandardCharsets.UTF_8)));
        return cells::get;
    }

    private static WriteValues valuesFor(long id) {
        return new WriteValues(id, id * 1.5, "row-" + id);
    }

    private record WriteValues(long id, double v, String name) {}

    // --- schema ---

    private static ParquetSchema fileSchema() {
        return flatSchema(
                requiredLeaf("id", PrimitiveKind.INT64),
                requiredLeaf("v", PrimitiveKind.DOUBLE),
                requiredLeaf("name", PrimitiveKind.BYTE_ARRAY));
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children =
                Stream.of(leaves).map(leaf -> (SchemaNode) leaf).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredLeaf(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }
}
