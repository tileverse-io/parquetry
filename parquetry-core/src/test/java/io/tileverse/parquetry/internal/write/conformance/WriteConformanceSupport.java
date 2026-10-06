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
package io.tileverse.parquetry.internal.write.conformance;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.io.LocalInputFile;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Shared helpers for the write-conformance and round-trip ITs. Each IT writes a Parquet file with parquetry, then reads
 * it back through parquet-java (records, footer, column index), through parquetry's own reader, or wraps a top-level
 * node in a {@code message schema { ... }} root. The assertions stay with the ITs.
 *
 * <p>parquet-java and parquetry both name a file reader, a statistics type and a column index. The ITs import the
 * parquet-java ones, and reach the parquetry ones through this class.
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
        return WriteFixtures.schemaOf(topLevel);
    }

    /** The row groups of {@code file} as read by parquet-java. */
    static List<BlockMetaData> rowGroupsViaParquetJava(Path file) throws IOException {
        return readFooterViaParquetJava(file).getBlocks();
    }

    /** The sole row group of {@code file} as read by parquet-java. */
    static BlockMetaData soleRowGroupOf(Path file) throws IOException {
        List<BlockMetaData> rowGroups = rowGroupsViaParquetJava(file);
        if (rowGroups.size() != 1) {
            throw new IllegalStateException(file.getFileName() + " has " + rowGroups.size() + " row groups");
        }
        return rowGroups.getFirst();
    }

    /** The chunk of {@code column} in {@code rowGroup}. */
    static ColumnChunkMetaData chunkOf(BlockMetaData rowGroup, String column) {
        return rowGroup.getColumns().stream()
                .filter(chunk -> chunk.getPath().toDotString().equals(column))
                .findFirst()
                .orElseThrow();
    }

    /**
     * The column index of {@code column} in a row group of {@code file} as read by parquet-java, {@code null} for a
     * chunk without one.
     */
    static ColumnIndex columnIndexViaParquetJava(Path file, int rowGroup, String column) throws IOException {
        try (org.apache.parquet.hadoop.ParquetFileReader reader =
                org.apache.parquet.hadoop.ParquetFileReader.open(new LocalInputFile(file))) {
            BlockMetaData block = reader.getFooter().getBlocks().get(rowGroup);
            return reader.readColumnIndex(chunkOf(block, column));
        }
    }

    /** A copy of the remaining bytes of {@code buffer}; the buffer keeps its position. */
    static byte[] bytesOf(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    /** The remaining bytes of each buffer of {@code buffers}. */
    static List<byte[]> bytesOf(List<ByteBuffer> buffers) {
        return buffers.stream().map(WriteConformanceSupport::bytesOf).toList();
    }

    /** The FLOAT in the little-endian bytes of {@code bound}; the buffer keeps its position. */
    static float floatOf(ByteBuffer bound) {
        ByteBuffer littleEndian = bound.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        return littleEndian.getFloat();
    }

    /** The FLOAT in the little-endian bytes of {@code bound}. */
    static float floatOf(byte[] bound) {
        return floatOf(ByteBuffer.wrap(bound));
    }

    /** A parquet-java reader of the rows of {@code file} as example groups. */
    static ParquetReader.Builder<Group> groupReader(Path file) {
        return new ParquetReader.Builder<>(new LocalInputFile(file)) {
            @Override
            protected ReadSupport<Group> getReadSupport() {
                return new GroupReadSupport();
            }
        };
    }

    /**
     * The values of the INT32 column {@code idColumn} in the rows read by parquet-java through {@code filter}, with its
     * metadata pruning tiers on or off.
     */
    static List<Integer> idsReadByParquetJava(
            Path file, String idColumn, FilterPredicate filter, boolean metadataPruning) throws IOException {
        List<Integer> ids = new ArrayList<>();
        ParquetReader.Builder<Group> builder = groupReader(file)
                .withFilter(FilterCompat.get(filter))
                .useStatsFilter(metadataPruning)
                .useDictionaryFilter(metadataPruning)
                .useBloomFilter(metadataPruning)
                .useColumnIndexFilter(metadataPruning)
                .useRecordFilter(true);
        try (ParquetReader<Group> reader = builder.build()) {
            Group group = reader.read();
            while (group != null) {
                ids.add(group.getInteger(idColumn, 0));
                group = reader.read();
            }
        }
        return ids;
    }

    /** The count of the rows read by parquetry through {@code predicate}. */
    static long rowsReadByParquetry(Path file, Predicate predicate, ReadOptions options) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file);
                Stream<ParquetRecord> rows = ParquetFileReader.open(source).read(predicate, Projection.ALL, options)) {
            return rows.count();
        }
    }

    /**
     * The bytes of the deprecated {@code min} statistics field of {@code column} in a row group of {@code file}; empty
     * for a chunk without that field.
     */
    static byte[] deprecatedMin(Path file, int rowGroup, String column) {
        return wireStatistics(file, rowGroup, column).min().toArray(JAVA_BYTE);
    }

    /** The {@code max} counterpart of {@link #deprecatedMin}. */
    static byte[] deprecatedMax(Path file, int rowGroup, String column) {
        return wireStatistics(file, rowGroup, column).max().toArray(JAVA_BYTE);
    }

    /**
     * The bytes of the {@code min_value} statistics field of {@code column} in a row group of {@code file}, as held by
     * the footer; empty for a chunk without that field.
     */
    static byte[] footerMin(Path file, int rowGroup, String column) {
        return wireStatistics(file, rowGroup, column).minValue().toArray(JAVA_BYTE);
    }

    /** The {@code max_value} counterpart of {@link #footerMin}. */
    static byte[] footerMax(Path file, int rowGroup, String column) {
        return wireStatistics(file, rowGroup, column).maxValue().toArray(JAVA_BYTE);
    }

    private static Statistics wireStatistics(Path file, int rowGroup, String column) {
        FileMetaData footer;
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            footer = ParquetFormat.readFooter(source);
        }
        return chunkMetaData(footer, rowGroup, column).statistics().orElseThrow();
    }

    private static ColumnMetaData chunkMetaData(FileMetaData footer, int rowGroup, String column) {
        List<String> path = List.of(column);
        for (ColumnChunk chunk : footer.rowGroups().get(rowGroup).columns()) {
            ColumnMetaData meta = chunk.metaData().orElseThrow();
            if (meta.pathInSchema().equals(path)) {
                return meta;
            }
        }
        throw new NoSuchElementException("no chunk of " + column + " in row group " + rowGroup);
    }

    /** The schema of {@code file} as read by parquetry. */
    static ParquetSchema schemaViaParquetry(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFileReader.open(source).schema();
        }
    }

    /** Reads every row of {@code file} back through parquetry's reader, detaching each record from the page buffers. */
    static List<ParquetRecord> readAll(Path file) {
        List<ParquetRecord> records = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file);
                Stream<ParquetRecord> stream = ParquetFileReader.open(source)
                        .read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            stream.map(ParquetRecord::detach).forEach(records::add);
        }
        return records;
    }
}
