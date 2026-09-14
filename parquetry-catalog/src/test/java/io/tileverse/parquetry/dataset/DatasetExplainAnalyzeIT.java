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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.QueryStats;

/**
 * Verifies that {@link ParquetSource#explainAnalyze} delegates to the single underlying reader and annotates the plan
 * with real execution stats. The fixture is a written file and the drain reads actual page bytes, which is why this
 * lives in the integration tier.
 */
class DatasetExplainAnalyzeIT {

    private static final Predicate KEEP_HIGH_IDS = Pred.col("id").gtEq(500L);

    @Test
    void explainAnalyzeAnnotatesThePlanWithRealExecutionStats(@TempDir Path tmp) throws Exception {
        Path file = writeFile(tmp.resolve("dataset-explain-analyze.parquet"), 1_000);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetSource parquetSource = ParquetSource.open(source);
            ReadOptions options = ReadOptions.DEFAULTS;

            long expectedRows = parquetSource.count(KEEP_HIGH_IDS, options);
            assertThat(expectedRows).as("ids 500..999 satisfy id >= 500").isEqualTo(500L);
            ExplainPlan plan = parquetSource.explainAnalyze(KEEP_HIGH_IDS, Projection.ALL, options);

            assertThat(plan.execution()).isPresent();
            QueryStats stats = plan.execution().orElseThrow();
            assertThat(stats.rowsMatched()).isEqualTo(expectedRows);
        }
    }

    private static Path writeFile(Path out, int rows) throws IOException {
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("""
                {"type":"record","name":"Row","fields":[
                  {"name":"id","type":"long"}
                ]}""");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(WriterVersion.PARQUET_2_0)
                .withRowGroupSize(8_192L)
                .build()) {
            for (int i = 0; i < rows; i++) {
                GenericData.Record avroRecord = new GenericData.Record(schema);
                avroRecord.put("id", (long) i);
                writer.write(avroRecord);
            }
        }
        return out;
    }
}
