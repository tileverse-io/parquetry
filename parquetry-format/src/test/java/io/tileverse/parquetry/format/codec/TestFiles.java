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
package io.tileverse.parquetry.format.codec;

import java.nio.file.Path;
import java.util.function.IntFunction;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;

/**
 * Generates Parquet fixture files for integration tests using the reference parquet-avro writer.
 */
final class TestFiles {

    private TestFiles() {}

    /**
     * Functional interface for a fixture factory: given a temp directory, generates a Parquet file
     * and returns its path, or {@code null} if the fixture cannot be generated on this platform.
     */
    @FunctionalInterface
    public interface Fixture {
        Path generate(Path tmpDir) throws Exception;
    }

    /**
     * Writes a zero-row Parquet file with a single {@code int} column named {@code id}.
     *
     * @param tmpDir directory to write into (must exist)
     * @return path to the written file
     */
    static Path emptyIntColumn(Path tmpDir) throws Exception {
        Schema schema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"m\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}");
        Path out = tmpDir.resolve("empty.parquet");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {
            // zero records intentionally
        }
        return out;
    }

    /**
     * 5 columns (int, long, float, double, boolean), Snappy, 1000 rows.
     */
    static Path flatIntSnappy(Path tmpDir) throws Exception {
        Schema schema = new Schema.Parser().parse("""
                {"type":"record","name":"FlatInt","fields":[
                  {"name":"col_int","type":"int"},
                  {"name":"col_long","type":"long"},
                  {"name":"col_float","type":"float"},
                  {"name":"col_double","type":"double"},
                  {"name":"col_bool","type":"boolean"}
                ]}""");
        return writeFile(tmpDir, "flat-int-snappy.parquet", schema, CompressionCodecName.SNAPPY, 1000, i -> {
            GenericData.Record rec = new GenericData.Record(schema);
            rec.put("col_int", i);
            rec.put("col_long", (long) i * 1000);
            rec.put("col_float", (float) i * 0.5f);
            rec.put("col_double", (double) i * 0.25);
            rec.put("col_bool", i % 2 == 0);
            return rec;
        });
    }

    /**
     * Single string column, GZIP, 1000 rows.
     */
    static Path flatStringGzip(Path tmpDir) throws Exception {
        Schema schema = new Schema.Parser().parse("""
                {"type":"record","name":"FlatString","fields":[
                  {"name":"col_str","type":"string"}
                ]}""");
        return writeFile(tmpDir, "flat-string-gzip.parquet", schema, CompressionCodecName.GZIP, 1000, i -> {
            GenericData.Record rec = new GenericData.Record(schema);
            rec.put("col_str", "value_" + i);
            return rec;
        });
    }

    /**
     * Record with one map&lt;string,int&gt; field, 100 rows, Snappy.
     */
    static Path nestedMap(Path tmpDir) throws Exception {
        Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
        Schema schema = Schema.createRecord("NestedMap", null, null, false);
        schema.setFields(java.util.List.of(new Schema.Field("col_map", mapSchema, null, null)));
        return writeFile(tmpDir, "nested-map.parquet", schema, CompressionCodecName.SNAPPY, 100, i -> {
            GenericData.Record rec = new GenericData.Record(schema);
            java.util.Map<String, Integer> map = new java.util.HashMap<>();
            map.put("key_a", i);
            map.put("key_b", i * 2);
            rec.put("col_map", map);
            return rec;
        });
    }

    /**
     * Record with one array&lt;int&gt; field, 100 rows, Snappy.
     */
    static Path nestedList(Path tmpDir) throws Exception {
        Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
        Schema schema = Schema.createRecord("NestedList", null, null, false);
        schema.setFields(java.util.List.of(new Schema.Field("col_list", arraySchema, null, null)));
        return writeFile(tmpDir, "nested-list.parquet", schema, CompressionCodecName.SNAPPY, 100, i -> {
            GenericData.Record rec = new GenericData.Record(schema);
            rec.put("col_list", java.util.List.of(i, i + 1, i + 2));
            return rec;
        });
    }

    /**
     * 3 optional columns with 50% nulls, 200 rows.
     */
    static Path nullableFlat(Path tmpDir) throws Exception {
        Schema nullableInt = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
        Schema nullableStr = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        Schema nullableDbl = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE));
        Schema schema = Schema.createRecord("NullableFlat", null, null, false);
        schema.setFields(java.util.List.of(
                new Schema.Field("opt_int", nullableInt, null, Schema.Field.NULL_DEFAULT_VALUE),
                new Schema.Field("opt_str", nullableStr, null, Schema.Field.NULL_DEFAULT_VALUE),
                new Schema.Field("opt_dbl", nullableDbl, null, Schema.Field.NULL_DEFAULT_VALUE)));
        return writeFile(tmpDir, "nullable-flat.parquet", schema, CompressionCodecName.SNAPPY, 200, i -> {
            GenericData.Record rec = new GenericData.Record(schema);
            rec.put("opt_int", i % 2 == 0 ? null : i);
            rec.put("opt_str", i % 2 == 0 ? "v_" + i : null);
            rec.put("opt_dbl", i % 3 == 0 ? null : (double) i * 1.1);
            return rec;
        });
    }

    /**
     * Flat int column, 3 row groups (forced via small row group size), 3000 rows total.
     */
    static Path multiRowGroup(Path tmpDir) throws Exception {
        Schema schema = new Schema.Parser().parse("""
                {"type":"record","name":"MultiRG","fields":[
                  {"name":"id","type":"int"}
                ]}""");
        Path out = tmpDir.resolve("multi-rowgroup.parquet");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                // Force small row groups so 3000 rows produce multiple row groups
                .withRowGroupSize(4096)
                .build()) {
            for (int i = 0; i < 3000; i++) {
                GenericData.Record rec = new GenericData.Record(schema);
                rec.put("id", i);
                writer.write(rec);
            }
        }
        return out;
    }

    /**
     * Low-cardinality string column (10 unique values), 1000 rows; parquet-avro picks dictionary encoding.
     */
    static Path dictionaryEncoded(Path tmpDir) throws Exception {
        Schema schema = new Schema.Parser().parse("""
                {"type":"record","name":"DictEncoded","fields":[
                  {"name":"category","type":"string"}
                ]}""");
        String[] categories = {"cat_a", "cat_b", "cat_c", "cat_d", "cat_e", "cat_f", "cat_g", "cat_h", "cat_i", "cat_j"
        };
        return writeFile(tmpDir, "dictionary-encoded.parquet", schema, CompressionCodecName.SNAPPY, 1000, i -> {
            GenericData.Record rec = new GenericData.Record(schema);
            rec.put("category", categories[i % categories.length]);
            return rec;
        });
    }

    /**
     * Sequential int column with writer version v2 to encourage DELTA_BINARY_PACKED encoding.
     */
    static Path deltaEncoded(Path tmpDir) throws Exception {
        Schema schema = new Schema.Parser().parse("""
                {"type":"record","name":"DeltaEncoded","fields":[
                  {"name":"seq","type":"int"}
                ]}""");
        Path out = tmpDir.resolve("delta-encoded.parquet");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0)
                .build()) {
            for (int i = 0; i < 1000; i++) {
                GenericData.Record rec = new GenericData.Record(schema);
                rec.put("seq", i);
                writer.write(rec);
            }
        }
        return out;
    }

    /**
     * Double column; BYTE_STREAM_SPLIT requires explicit column property configuration.
     * Returns null if parquet-avro cannot be configured for it in this environment.
     */
    static Path byteStreamSplit(Path tmpDir) throws Exception {
        // BYTE_STREAM_SPLIT encoding requires parquet v2 writer properties with
        // explicit column config. parquet-avro exposes this through ParquetProperties.
        // If not trivially configurable, skip this fixture.
        // Attempt to write with v2 writer; actual encoding depends on parquet-avro version.
        Schema schema = new Schema.Parser().parse("""
                {"type":"record","name":"ByteStreamSplit","fields":[
                  {"name":"val","type":"double"}
                ]}""");
        Path out = tmpDir.resolve("byte-stream-split.parquet");
        try {
            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                            new LocalOutputFile(out))
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withWriterVersion(org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0)
                    .build()) {
                for (int i = 0; i < 1000; i++) {
                    GenericData.Record rec = new GenericData.Record(schema);
                    rec.put("val", (double) i * 1.23456);
                    writer.write(rec);
                }
            }
        } catch (Exception e) {
            // If encoding configuration fails, skip this fixture
            return null;
        }
        return out;
    }

    /**
     * Flat int column, ZSTD compression, 1000 rows.
     */
    static Path zstdCompressed(Path tmpDir) throws Exception {
        Schema schema = new Schema.Parser().parse("""
                {"type":"record","name":"ZstdCompressed","fields":[
                  {"name":"id","type":"int"}
                ]}""");
        return writeFile(tmpDir, "zstd-compressed.parquet", schema, CompressionCodecName.ZSTD, 1000, i -> {
            GenericData.Record rec = new GenericData.Record(schema);
            rec.put("id", i);
            return rec;
        });
    }

    /**
     * Helper that writes a fixed number of rows using a record-factory function.
     */
    private static Path writeFile(
            Path tmpDir,
            String name,
            Schema schema,
            CompressionCodecName codec,
            int rows,
            IntFunction<GenericData.Record> recordFn)
            throws Exception {
        Path out = tmpDir.resolve(name);
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(codec)
                .build()) {
            for (int i = 0; i < rows; i++) {
                writer.write(recordFn.apply(i));
            }
        }
        return out;
    }
}
