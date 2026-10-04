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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.FooterRewrite;

/**
 * Reads files with only the deprecated min and max statistics, as written by parquet-mr, Hive and Impala before
 * {@code min_value} and {@code max_value} existed. Those bounds follow signed byte order for a string column, and
 * pruning must not drop a row group from them.
 */
class LegacyStatisticsReadTest {

    private static final ColumnPath NAME = ColumnPath.of("name");

    /** A name with a UTF-8 leading byte of 0xC3, negative as a signed byte: below "Bern" in signed order only. */
    private static final String OLTEN = "\u00D6lten";

    private static final String PARQUET_MR_1_9 =
            "parquet-mr version 1.9.0 (build 38262e2c80015d0935dad20f8e18f2d6f9fbd03c)";

    @TempDir
    Path tempDir;

    @Test
    void signedLegacyStringBoundsDropNoMatchingRowGroup() throws IOException {
        Path file = withLegacyBounds(writeNames(List.of(OLTEN, "Bern")), OLTEN, "Bern");
        Predicate bern = new Predicate.Eq(NAME, new Value.StringVal("Bern"));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.count(bern, ReadOptions.DEFAULTS)).isEqualTo(1L);
            assertThat(statsDecision(reader, bern)).isInstanceOf(PruningDecision.NotApplied.class);
        }
    }

    @Test
    void equalLegacyStringBoundsStillPrune() throws IOException {
        Path file = withLegacyBounds(writeNames(List.of("Bern", "Bern")), "Bern", "Bern");
        Predicate zug = new Predicate.Eq(NAME, new Value.StringVal("Zug"));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.count(zug, ReadOptions.DEFAULTS)).isZero();
            assertThat(statsDecision(reader, zug)).isInstanceOf(PruningDecision.Eliminated.class);
        }
    }

    private static PruningDecision statsDecision(ParquetFileReader reader, Predicate predicate) {
        ExplainPlan plan = reader.explain(predicate, Projection.ALL, ReadOptions.DEFAULTS);
        return plan.rowGroups().getFirst().tiers().getFirst();
    }

    private Path writeNames(List<String> names) throws IOException {
        Path file = tempDir.resolve("names.parquet");
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                "name",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        ParquetSchema schema = new ParquetSchema(
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1));
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(names.size());
        for (String name : names) {
            rows.add(Map.of(NAME, name));
        }
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    /** A copy of {@code file} with the name statistics recorded the parquet-mr 1.9 way: deprecated fields only. */
    private Path withLegacyBounds(Path file, String legacyMin, String legacyMax) throws IOException {
        Path rewritten = tempDir.resolve("legacy.parquet");
        return FooterRewrite.rewrite(file, rewritten, legacyStatistics(legacyMin, legacyMax));
    }

    private static UnaryOperator<FileMetaData> legacyStatistics(String legacyMin, String legacyMax) {
        Statistics legacy = new Statistics(
                utf8(legacyMax),
                utf8(legacyMin),
                OptionalLong.of(0L),
                OptionalLong.empty(),
                MemorySegment.NULL,
                MemorySegment.NULL,
                false,
                false);
        return footer -> new FileMetaData(
                footer.version(),
                footer.schema(),
                footer.numRows(),
                rowGroupsWith(footer.rowGroups(), legacy),
                footer.keyValueMetadata(),
                Optional.of(PARQUET_MR_1_9),
                Optional.empty(),
                footer.encryptionAlgorithm(),
                footer.footerSigningKeyMetadata());
    }

    private static List<RowGroup> rowGroupsWith(List<RowGroup> rowGroups, Statistics legacy) {
        List<RowGroup> edited = new ArrayList<>(rowGroups.size());
        for (RowGroup rowGroup : rowGroups) {
            List<ColumnChunk> chunks = new ArrayList<>(rowGroup.columns().size());
            for (ColumnChunk chunk : rowGroup.columns()) {
                chunks.add(chunkWith(chunk, legacy));
            }
            edited.add(new RowGroup(
                    chunks,
                    rowGroup.totalByteSize(),
                    rowGroup.numRows(),
                    rowGroup.sortingColumns(),
                    rowGroup.fileOffset(),
                    rowGroup.totalCompressedSize(),
                    rowGroup.ordinal()));
        }
        return edited;
    }

    private static ColumnChunk chunkWith(ColumnChunk chunk, Statistics legacy) {
        ColumnMetaData meta = chunk.metaData().orElseThrow();
        ColumnMetaData edited = new ColumnMetaData(
                meta.type(),
                meta.encodings(),
                meta.pathInSchema(),
                meta.codec(),
                meta.numValues(),
                meta.totalUncompressedSize(),
                meta.totalCompressedSize(),
                meta.keyValueMetadata(),
                meta.dataPageOffset(),
                meta.indexPageOffset(),
                meta.dictionaryPageOffset(),
                Optional.of(legacy),
                meta.encodingStats(),
                meta.bloomFilterOffset(),
                meta.bloomFilterLength(),
                meta.sizeStatistics(),
                meta.geospatialStatistics());
        return new ColumnChunk(
                chunk.filePath(),
                chunk.fileOffset(),
                Optional.of(edited),
                chunk.offsetIndexOffset(),
                chunk.offsetIndexLength(),
                chunk.columnIndexOffset(),
                chunk.columnIndexLength(),
                chunk.cryptoMetadata(),
                chunk.encryptedColumnMetadata());
    }

    private static MemorySegment utf8(String value) {
        return MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8));
    }
}
