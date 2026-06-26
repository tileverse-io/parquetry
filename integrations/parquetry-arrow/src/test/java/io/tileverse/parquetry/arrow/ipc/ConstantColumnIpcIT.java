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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.OutputColumn;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * A constant output column appended to a read must export through Arrow IPC as a real materialized vector: the written
 * stream gains a {@code year} field and every row holds the constant. This is the path-only Hive value flowing from the
 * dataset engine ({@link ParquetSource#readBatches(Query, ReadOptions)}) through {@link ArrowIpcWriter} to a canonical
 * Arrow reader.
 */
class ConstantColumnIpcIT {

    private static final String CORPUS_RESOURCE = "parquet-testing/data/alltypes_plain.parquet";
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final long YEAR_CONSTANT = 2024L;

    @Test
    void constantColumnExportsAsMaterializedVectorWithConstantPerRow(@TempDir Path tempDir) throws Exception {
        Path parquetFile = TestCorpus.extractFile(CORPUS_RESOURCE, tempDir);
        byte[] ipc = writeIpcWithConstantYear(parquetFile);

        assertEveryRowHoldsTheConstant(ipc);
    }

    private static byte[] writeIpcWithConstantYear(Path parquetFile) {
        Query query = Query.builder(Predicate.ALWAYS_TRUE, Projection.of(Set.of(ID)))
                .output(List.of(
                        new OutputColumn.Physical(ID, ID),
                        new OutputColumn.Constant(YEAR, new Value.LongVal(YEAR_CONSTANT))))
                .build();
        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            ParquetSource parquetSource = ParquetSource.open(source);
            try (Stream<ParquetRecordBatch> batches = parquetSource.readBatches(query, ReadOptions.DEFAULTS)) {
                List<ParquetRecordBatch> materialized = batches.toList();
                ParquetSchema schemaWithConstant = materialized.get(0).projectedSchema();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowIpcWriter.write(schemaWithConstant, Optional.empty(), materialized.stream(), out);
                return out.toByteArray();
            }
        }
    }

    private static void assertEveryRowHoldsTheConstant(byte[] ipc) throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            long rowsSeen = 0;
            boolean sawYearField = false;
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                assertThat(root.getSchema().findField("year")).isNotNull();
                sawYearField = true;
                BigIntVector year = (BigIntVector) root.getVector("year");
                for (int row = 0; row < root.getRowCount(); row++) {
                    assertThat(year.isNull(row)).isFalse();
                    assertThat(year.get(row)).isEqualTo(YEAR_CONSTANT);
                }
                rowsSeen += root.getRowCount();
            }
            assertThat(sawYearField).isTrue();
            assertThat(rowsSeen).isPositive();
        }
    }
}
