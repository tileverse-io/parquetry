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
package io.tileverse.parquetry.data;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies that {@link ExplainPlan#estimatedBytesRead()} reflects the total compressed size of the projected leaf
 * column chunks across the row groups that survive pruning.
 *
 * <p>The fixture writes ids {@code 0..8} four rows per row group into two INT64 columns, hence three row groups holding
 * {@code [0,1,2,3]}, {@code [4,5,6,7]}, and {@code [8]}. The eliminating predicate {@code id >= 5} drops the first row
 * group from its statistics, which lets the test prove that an eliminated group contributes nothing to the estimate.
 */
class EstimatedBytesIT {

    private static final Predicate MATCH_ALL = col("id").gtEq(0L);
    private static final Predicate ELIMINATES_FIRST_GROUP = col("id").gtEq(5L);

    @Test
    void fullScanEstimateIsPositiveAndBoundedByFileSize(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            ExplainPlan plan = reader.explain(MATCH_ALL, Projection.ALL, ReadOptions.DEFAULTS);

            assertThat(plan.estimatedBytesRead())
                    .as("a full scan estimates a positive number of fetched bytes")
                    .isPositive();
            assertThat(plan.estimatedBytesRead())
                    .as("the estimate never exceeds the file size on disk")
                    .isLessThanOrEqualTo(Files.size(file));
        }
    }

    @Test
    void singleColumnProjectionEstimatesFewerBytesThanFullProjection(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            Projection idOnly = Projection.ofPhysical(Set.of(ColumnPath.of("id")));

            long allColumns = reader.explain(MATCH_ALL, Projection.ALL, ReadOptions.DEFAULTS)
                    .estimatedBytesRead();
            long oneColumn =
                    reader.explain(MATCH_ALL, idOnly, ReadOptions.DEFAULTS).estimatedBytesRead();

            assertThat(oneColumn)
                    .as("projecting a single column fetches fewer bytes than projecting all columns")
                    .isLessThan(allColumns);
        }
    }

    @Test
    void eliminatingPredicateEstimatesFewerBytesThanFullScan(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            long noElimination = reader.explain(MATCH_ALL, Projection.ALL, ReadOptions.DEFAULTS)
                    .estimatedBytesRead();
            long oneGroupEliminated = reader.explain(ELIMINATES_FIRST_GROUP, Projection.ALL, ReadOptions.DEFAULTS)
                    .estimatedBytesRead();

            assertThat(oneGroupEliminated)
                    .as("an eliminated row group contributes no bytes to the estimate")
                    .isLessThan(noElimination);
        }
    }

    /**
     * A chunk whose recorded length is beyond the addressable range can never be fetched, and the footer records it as
     * a placeholder rather than a byte count. Counting that placeholder would pull the estimate below the bytes
     * actually fetched by a read, hence the column contributes nothing and the estimate stays an honest lower bound.
     */
    @Test
    void aChunkLengthBeyondTheAddressableRangeCountsAsNoBytes(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        long dropped = firstChunkCompressedSize(file);
        Path unfetchable = withFirstChunkLengthBeyondRange(file, tmp);

        assertThat(fullScanEstimateOf(unfetchable))
                .as("the unfetchable column drops out of the estimate rather than subtracting from it")
                .isEqualTo(fullScanEstimateOf(file) - dropped);
    }

    private static long fullScanEstimateOf(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFileReader.open(source)
                    .explain(MATCH_ALL, Projection.ALL, ReadOptions.DEFAULTS)
                    .estimatedBytesRead();
        }
    }

    private static long firstChunkCompressedSize(Path file) {
        return firstChunkMetaData(footerOf(file)).totalCompressedSize();
    }

    /** The same file, its first column chunk claiming a length that no fetch can address. */
    private static Path withFirstChunkLengthBeyondRange(Path file, Path tmp) throws IOException {
        FileMetaData footer = footerOf(file);
        ColumnMetaData first = firstChunkMetaData(footer);
        ColumnMetaData unfetchable = ColumnMetaData.builder()
                .type(first.type())
                .encodings(first.encodings())
                .pathInSchema(first.pathInSchema())
                .codec(first.codec())
                .numValues(first.numValues())
                .totalUncompressedSize(first.totalUncompressedSize())
                .totalCompressedSize(Integer.MAX_VALUE + 1L)
                .dataPageOffset(first.dataPageOffset())
                .build();
        return writeWithFooter(file, tmp, withFirstChunkReplaced(footer, unfetchable));
    }

    private static ColumnMetaData firstChunkMetaData(FileMetaData footer) {
        return footer.rowGroups().getFirst().columns().getFirst().metaData().orElseThrow();
    }

    private static FileMetaData withFirstChunkReplaced(FileMetaData footer, ColumnMetaData replacement) {
        RowGroup first = footer.rowGroups().getFirst();
        List<ColumnChunk> columns = new ArrayList<>(first.columns());
        columns.set(0, ColumnChunk.builder().metaData(Optional.of(replacement)).build());
        List<RowGroup> rowGroups = new ArrayList<>(footer.rowGroups());
        rowGroups.set(
                0,
                RowGroup.builder()
                        .columns(columns)
                        .numRows(first.numRows())
                        .totalByteSize(first.totalByteSize())
                        .totalCompressedSize(first.totalCompressedSize())
                        .build());
        return FileMetaData.builder()
                .version(footer.version())
                .schema(footer.schema())
                .numRows(footer.numRows())
                .rowGroups(rowGroups)
                .keyValueMetadata(footer.keyValueMetadata())
                .createdBy(footer.createdBy())
                .build();
    }

    private static FileMetaData footerOf(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFormat.readFooter(source);
        }
    }

    /** Copies {@code file} up to its footer and re-frames it around {@code footer}: thrift, length, tail magic. */
    private static Path writeWithFooter(Path file, Path tmp, FileMetaData footer) throws IOException {
        byte[] original = Files.readAllBytes(file);
        int originalFooterLength = ByteBuffer.wrap(original, original.length - 8, 4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
        int dataEnd = original.length - 8 - originalFooterLength;
        byte[] encoded = ParquetFormat.toBytes(footer);
        byte[] trailer = ByteBuffer.allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(encoded.length)
                .put("PAR1".getBytes(StandardCharsets.US_ASCII))
                .array();

        Path patched = tmp.resolve("unfetchable-chunk.parquet");
        try (OutputStream out = Files.newOutputStream(patched)) {
            out.write(original, 0, dataEnd);
            out.write(encoded);
            out.write(trailer);
        }
        return patched;
    }

    private static Path writeThreeRowGroups(Path tmp) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt64("id"), requiredInt64("payload"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tmp)
                .rowGroupSize(RowGroupSize.rows(4L))
                .build();
        Path file = tmp.resolve("estimated-bytes.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (int i = 0; i < 9; i++) {
                WriteFixtures.appendRow(
                        appender,
                        schema,
                        Map.of(
                                ColumnPath.of("id"), (long) i,
                                ColumnPath.of("payload"), (long) (i * 1000)));
            }
        }
        return file;
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }
}
