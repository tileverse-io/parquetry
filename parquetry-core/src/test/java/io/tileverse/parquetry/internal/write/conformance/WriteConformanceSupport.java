/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.internal.write.conformance;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalInputFile;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Shared read-back helpers for the write-conformance and round-trip ITs. Each IT writes a Parquet file with parquetry,
 * then reads it back through parquet-java (Avro records or footer), through parquetry's own reader, or wraps a
 * top-level node in a {@code message schema { ... }} root. These helpers are the byte-identical copies each IT
 * previously declared on its own; the assertions stay with the ITs.
 */
final class WriteConformanceSupport {

    private WriteConformanceSupport() {}

    /** Reads {@code file} back through parquet-java's Avro reader into one {@link GenericRecord} per row. */
    static List<GenericRecord> readWithAvro(Path file) throws IOException {
        List<GenericRecord> out = new ArrayList<>();
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(
                        new LocalInputFile(file))
                .build()) {
            GenericData.Record row;
            while ((row = reader.read()) != null) {
                out.add(row);
            }
        }
        return out;
    }

    /** Opens {@code file} with parquet-java and returns its parsed footer metadata. */
    static ParquetMetadata readFooterViaParquetJava(Path file) throws IOException {
        try (org.apache.parquet.hadoop.ParquetFileReader reader =
                org.apache.parquet.hadoop.ParquetFileReader.open(new LocalInputFile(file))) {
            return reader.getFooter();
        }
    }

    /**
     * Wraps a top-level node in a {@code message schema { ... }} root and returns the resulting {@link ParquetSchema}.
     */
    static ParquetSchema rootOf(SchemaNode topLevel) {
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(topLevel), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /** Reads every row of {@code file} back through parquetry's reader, detaching each record from the page buffers. */
    static List<ParquetRecord> readAll(Path file) {
        List<ParquetRecord> records = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file);
                Stream<ParquetRecord> stream = io.tileverse.parquetry.data.ParquetFileReader.open(source)
                        .read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            stream.map(ParquetRecord::detach).forEach(records::add);
        }
        return records;
    }
}
