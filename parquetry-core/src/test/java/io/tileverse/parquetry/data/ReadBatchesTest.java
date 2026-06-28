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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.VectorArrays;

/**
 * Coverage for {@link ParquetFileReader#readBatches()} and the {@link ReadOptions#batchSize()} cap. Fixtures are
 * written via {@code parquet-avro} with small row groups so multiple batches are emitted per file.
 */
class ReadBatchesTest {

    @Test
    void readsBatchesFromFixtureFile(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("read-batches.parquet");
        int rowCount = 1_000;
        writeFixture(file, generateRows(rowCount));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);

            long totalRows = 0L;
            int batchCount = 0;
            try (Stream<ParquetRecordBatch> batches =
                    dataset.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                List<ParquetRecordBatch> seen = batches.toList();
                for (ParquetRecordBatch batch : seen) {
                    totalRows += batch.rowCount();
                    batchCount++;
                    batch.close();
                }
            }

            assertThat(totalRows).as("sum of batch row counts").isEqualTo(rowCount);
            assertThat(batchCount)
                    .as("expected at least one batch for %d rows", rowCount)
                    .isPositive();
        }
    }

    @Test
    void batchSizeCapHonored(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("read-batches-capped.parquet");
        int rowCount = 1_000;
        writeFixture(file, generateRows(rowCount));

        ReadOptions options = ReadOptions.builder().batchSize(100).build();

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);

            long totalRows = 0L;
            int maxObservedBatchSize = 0;
            try (Stream<ParquetRecordBatch> batches =
                    dataset.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                List<ParquetRecordBatch> seen = batches.toList();
                for (ParquetRecordBatch batch : seen) {
                    int n = batch.rowCount();
                    maxObservedBatchSize = Math.max(maxObservedBatchSize, n);
                    totalRows += n;
                    assertThat(n)
                            .as("each batch must respect the configured batchSize cap")
                            .isLessThanOrEqualTo(100);
                    batch.close();
                }
            }

            assertThat(totalRows).isEqualTo(rowCount);
            assertThat(maxObservedBatchSize).isPositive();
        }
    }

    @Test
    void batchSizeCapStreamsCorrectValues(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("read-batches-values.parquet");
        int rowCount = 1_000;
        writeFixture(file, generateRows(rowCount));

        ReadOptions options = ReadOptions.builder().batchSize(100).build();
        ColumnPath yearPath = ColumnPath.of("year");

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);

            List<Integer> seenYears = new ArrayList<>(rowCount);
            try (Stream<ParquetRecordBatch> batches =
                    dataset.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                List<ParquetRecordBatch> emitted = batches.toList();
                for (ParquetRecordBatch batch : emitted) {
                    IntVector yearVec = (IntVector) batch.columns().get(yearPath);
                    int[] values = VectorArrays.toArray(yearVec);
                    for (int i = 0; i < batch.rowCount(); i++) {
                        seenYears.add(values[i]);
                    }
                    batch.close();
                }
            }

            assertThat(seenYears).hasSize(rowCount);
            for (int i = 0; i < rowCount; i++) {
                assertThat(seenYears.get(i)).as("year @ logical row %d", i).isEqualTo(2020 + (i % 5));
            }
        }
    }

    @Test
    void batchSizeRejectsNonPositive() {
        ReadOptions.Builder builder = ReadOptions.builder();
        assertThat(builder.batchSize(1)).isSameAs(builder);
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> builder.batchSize(0));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> builder.batchSize(-5));
    }

    private static List<RowDto> generateRows(int count) {
        List<RowDto> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rows.add(new RowDto(2020 + (i % 5), (i % 2 == 0) ? "AR" : "BR", i * 1.5));
        }
        return rows;
    }

    private static void writeFixture(Path out, List<RowDto> rows) throws IOException {
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("""
                {"type":"record","name":"Row","fields":[
                  {"name":"year","type":"int"},
                  {"name":"country","type":"string"},
                  {"name":"value","type":"double"}
                ]}""");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(WriterVersion.PARQUET_2_0)
                .withRowGroupSize(8_192L)
                .build()) {
            for (RowDto r : rows) {
                GenericData.Record avroRecord = new GenericData.Record(schema);
                avroRecord.put("year", r.year());
                avroRecord.put("country", r.country());
                avroRecord.put("value", r.value());
                writer.write(avroRecord);
            }
        }
    }

    private record RowDto(int year, String country, double value) {}
}
