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
package io.tileverse.parquetry.schema;

import java.nio.file.Path;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;

/**
 * Generates Parquet fixture files for schema-level tests using the reference parquet-avro writer.
 *
 * <p>Mirrors a subset of the fixture generators in parquetry-format's test utilities.
 */
final class TestFixtures {

    private TestFixtures() {}

    /** Writes a zero-row Parquet file with a single {@code int} column named {@code id}. */
    static Path emptyIntColumn(Path tmpDir) throws Exception {
        Schema schema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"m\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}");
        Path out = tmpDir.resolve("empty.parquet");
        try (var _ = AvroParquetWriter.<GenericData.Record>builder(new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {
            // zero records intentionally; opening + closing the writer produces a valid empty Parquet footer
        }
        return out;
    }

    /** 5 columns (int, long, float, double, boolean), Snappy, 100 rows. */
    static Path flatIntSnappy(Path tmpDir) throws Exception {
        Schema schema = new Schema.Parser().parse("""
                {"type":"record","name":"FlatInt","fields":[
                  {"name":"col_int","type":"int"},
                  {"name":"col_long","type":"long"},
                  {"name":"col_float","type":"float"},
                  {"name":"col_double","type":"double"},
                  {"name":"col_bool","type":"boolean"}
                ]}""");
        Path out = tmpDir.resolve("flat-int-snappy.parquet");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (int i = 0; i < 100; i++) {
                GenericData.Record rec = new GenericData.Record(schema);
                rec.put("col_int", i);
                rec.put("col_long", (long) i * 1000);
                rec.put("col_float", (float) i * 0.5f);
                rec.put("col_double", (double) i * 0.25);
                rec.put("col_bool", i % 2 == 0);
                writer.write(rec);
            }
        }
        return out;
    }

    /**
     * Record with one {@code array<int>} field, 10 rows, Snappy.
     *
     * <p>Avro encodes array as Parquet list encoding: one leaf (int element).
     */
    static Path nestedList(Path tmpDir) throws Exception {
        Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
        Schema schema = Schema.createRecord("NestedList", null, null, false);
        schema.setFields(java.util.List.of(new Schema.Field("col_list", arraySchema, null, null)));
        Path out = tmpDir.resolve("nested-list.parquet");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (int i = 0; i < 10; i++) {
                GenericData.Record rec = new GenericData.Record(schema);
                rec.put("col_list", java.util.List.of(i, i + 1));
                writer.write(rec);
            }
        }
        return out;
    }

    /**
     * Record with one {@code map<string,int>} field, 10 rows, Snappy.
     *
     * <p>Parquet map encoding produces two leaves: key (BYTE_ARRAY) and value (INT32).
     */
    static Path nestedMap(Path tmpDir) throws Exception {
        Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
        Schema schema = Schema.createRecord("NestedMap", null, null, false);
        schema.setFields(java.util.List.of(new Schema.Field("col_map", mapSchema, null, null)));
        Path out = tmpDir.resolve("nested-map.parquet");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (int i = 0; i < 10; i++) {
                GenericData.Record rec = new GenericData.Record(schema);
                java.util.Map<String, Integer> map = new java.util.HashMap<>();
                map.put("key_a", i);
                rec.put("col_map", map);
                writer.write(rec);
            }
        }
        return out;
    }
}
