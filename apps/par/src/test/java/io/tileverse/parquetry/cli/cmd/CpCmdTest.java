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
package io.tileverse.parquetry.cli.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.cli.CliExitCode;
import io.tileverse.parquetry.cli.Par;
import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

import picocli.CommandLine;

class CpCmdTest {

    @Test
    void preservesOpaqueKeyValueMetadata(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Path dst = dir.resolve("copy.parquet");
        Fixtures.writeCities(src, Map.of("comment", "round-trips"));
        int code = Par.newCommandLine().execute("cp", src.toString(), dst.toString());
        assertThat(code).isZero();

        StringWriter out = new StringWriter();
        CommandLine meta = Par.newCommandLine();
        meta.setOut(new PrintWriter(out));
        int metaCode = meta.execute("meta", dst.toString(), "-o", "json");
        assertThat(metaCode).isZero();
        assertThat(out.toString()).contains("comment", "round-trips");
    }

    @Test
    void copiesWithProjectionAndFilter(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Path dst = dir.resolve("big.parquet");
        Fixtures.writeCities(src);
        int code = Par.newCommandLine()
                .execute("cp", src.toString(), dst.toString(), "--columns", "name,pop", "--filter", "pop > 1000000");
        assertThat(code).isZero();
        assertThat(Files.exists(dst)).isTrue();

        try (Storage storage = StorageFactory.open(dir.toUri());
                RangeReader reader = storage.openRangeReader("big.parquet")) {
            ParquetSource out = ParquetSource.open(ByteRangeSources.from(reader));
            assertThat(out.schema().leafColumns()).extracting(p -> p.dot()).containsExactly("name", "pop");
            try (Stream<ParquetRecord> rows = out.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                assertThat(rows.count()).isEqualTo(3L);
            }
        }
    }

    @Test
    void copiesIntoDirectoryDestinationUsingSourceFilename(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Path destDir = Files.createDirectory(dir.resolve("out"));
        Fixtures.writeCities(src);

        int code = Par.newCommandLine().execute("cp", src.toString(), destDir.toString());
        assertThat(code).isZero();

        Path written = destDir.resolve("cities.parquet");
        assertThat(written).exists();
        assertThat(rowCount(written)).isEqualTo(4L);
    }

    @Test
    void copiesToFileUriDestination(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Path dst = dir.resolve("copy.parquet");
        Fixtures.writeCities(src);

        int code =
                Par.newCommandLine().execute("cp", src.toString(), dst.toUri().toString());
        assertThat(code).isZero();
        assertThat(dst).exists();
        assertThat(rowCount(dst)).isEqualTo(4L);
    }

    @Test
    void refusesToOverwriteSource(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Fixtures.writeCities(src);
        int code = Par.newCommandLine().execute("cp", src.toString(), src.toString());
        assertThat(code).isEqualTo(CliExitCode.GENERIC);
    }

    @Test
    void refusesToWriteOntoSourceViaDirectoryDestination(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Fixtures.writeCities(src);
        // dst is the directory holding the source; the resolved object is dir/cities.parquet, the source itself.
        int code = Par.newCommandLine().execute("cp", src.toString(), dir.toString() + "/");
        assertThat(code).isEqualTo(CliExitCode.GENERIC);
    }

    @Test
    void refusesToWriteOntoSourceViaFileUri(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Fixtures.writeCities(src);
        int code =
                Par.newCommandLine().execute("cp", src.toString(), src.toUri().toString());
        assertThat(code).isEqualTo(CliExitCode.GENERIC);
    }

    @Test
    void refusesToClobberWithoutForce(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Path dst = dir.resolve("dst.parquet");
        Fixtures.writeCities(src);
        Files.writeString(dst, "exists");
        int code = Par.newCommandLine().execute("cp", src.toString(), dst.toString());
        assertThat(code).isEqualTo(CliExitCode.GENERIC);
    }

    @Test
    void overwritesExistingDestinationWithForce(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Path dst = dir.resolve("dst.parquet");
        Fixtures.writeCities(src);
        Files.writeString(dst, "stale-content");

        int code = Par.newCommandLine().execute("cp", src.toString(), dst.toString(), "-f");
        assertThat(code).isZero();
        assertThat(rowCount(dst)).isEqualTo(4L);
    }

    @Test
    void rejectsDirectoryPrefixSource(@TempDir Path dir) {
        Path dst = dir.resolve("out.parquet");
        int code = Par.newCommandLine().execute("cp", "s3://bucket/prefix/", dst.toString());
        assertThat(code).isEqualTo(CliExitCode.GENERIC);
    }

    @Test
    void rowGroupRowsFlagSealsRowGroupsAtTheRequestedCount(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Path dst = dir.resolve("split.parquet");
        Fixtures.writeCities(src);

        int code = Par.newCommandLine().execute("cp", src.toString(), dst.toString(), "--row-group-rows", "2");
        assertThat(code).isZero();
        assertThat(rowCount(dst)).isEqualTo(4L);
        assertThat(rowGroupCount(dst)).isEqualTo(2);
    }

    @Test
    void rowGroupBytesFlagAcceptsASizeSuffix(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Path dst = dir.resolve("byte-sized.parquet");
        Fixtures.writeCities(src);

        int code = Par.newCommandLine().execute("cp", src.toString(), dst.toString(), "--row-group-bytes", "256MB");
        assertThat(code).isZero();
        assertThat(rowCount(dst)).isEqualTo(4L);
    }

    @Test
    void rejectsBothRowGroupSizingFlagsAtOnce(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Path dst = dir.resolve("dst.parquet");
        Fixtures.writeCities(src);

        int code = Par.newCommandLine()
                .execute("cp", src.toString(), dst.toString(), "--row-group-rows", "2", "--row-group-bytes", "1MB");
        assertThat(code).isEqualTo(CliExitCode.USAGE);
    }

    @Test
    void rejectsAnInvalidRowGroupBytesValue(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("cities.parquet");
        Path dst = dir.resolve("dst.parquet");
        Fixtures.writeCities(src);

        int code =
                Par.newCommandLine().execute("cp", src.toString(), dst.toString(), "--row-group-bytes", "not-a-size");
        assertThat(code).isEqualTo(CliExitCode.USAGE);
    }

    /**
     * Copies a file that has a REQUIRED struct group column (two FLOAT leaves) and verifies the leaf values round-trip
     * through the copy faithfully.
     */
    @Test
    void copiesRequiredStructColumn(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("structs.parquet");
        Path dst = dir.resolve("copy.parquet");
        writeStructFixture(src);

        int code = Par.newCommandLine().execute("cp", src.toString(), dst.toString());
        assertThat(code).isZero();

        ColumnPath xmin = ColumnPath.of("bbox", "xmin");
        ColumnPath xmax = ColumnPath.of("bbox", "xmax");
        try (Storage storage = StorageFactory.open(dst.getParent().toUri());
                RangeReader reader = storage.openRangeReader(dst.getFileName().toString())) {
            ParquetSource out = ParquetSource.open(ByteRangeSources.from(reader));
            try (Stream<ParquetRecord> rows = out.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                List<ParquetRecord> records = rows.map(ParquetRecord::detach).toList();
                assertThat(records).hasSize(2);
                assertThat(records.get(0).getFloat(xmin)).isEqualTo(1.0f);
                assertThat(records.get(0).getFloat(xmax)).isEqualTo(2.0f);
                assertThat(records.get(1).getFloat(xmin)).isEqualTo(3.0f);
                assertThat(records.get(1).getFloat(xmax)).isEqualTo(4.0f);
            }
        }
    }

    /**
     * Copies a file that has an OPTIONAL struct group column where one row has a null struct, and verifies the null
     * struct round-trips as null (not as a present struct with null children).
     */
    @Test
    void copiesOptionalStructWithNullRow(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("optional_structs.parquet");
        Path dst = dir.resolve("copy.parquet");
        writeOptionalStructFixture(src);

        int code = Par.newCommandLine().execute("cp", src.toString(), dst.toString());
        assertThat(code).isZero();

        ColumnPath structPath = ColumnPath.of("bbox");
        ColumnPath xmin = ColumnPath.of("bbox", "xmin");
        ColumnPath xmax = ColumnPath.of("bbox", "xmax");
        try (Storage storage = StorageFactory.open(dst.getParent().toUri());
                RangeReader reader = storage.openRangeReader(dst.getFileName().toString())) {
            ParquetSource out = ParquetSource.open(ByteRangeSources.from(reader));
            try (Stream<ParquetRecord> rows = out.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                List<ParquetRecord> records = rows.map(ParquetRecord::detach).toList();
                assertThat(records).hasSize(2);
                // Row 0: struct is present.
                assertThat(records.get(0).isNull(structPath)).isFalse();
                assertThat(records.get(0).getFloat(xmin)).isEqualTo(1.0f);
                assertThat(records.get(0).getFloat(xmax)).isEqualTo(2.0f);
                // Row 1: struct is null.
                assertThat(records.get(1).isNull(structPath)).isTrue();
                assertThat(records.get(1).isNull(xmin)).isTrue();
                assertThat(records.get(1).isNull(xmax)).isTrue();
            }
        }
    }

    /** Writes a two-row Parquet file with a REQUIRED struct column {@code bbox} containing two FLOAT leaves. */
    private static void writeStructFixture(Path file) throws Exception {
        SchemaNode.Primitive xminLeaf = new SchemaNode.Primitive(
                "xmin", Repetition.REQUIRED, PrimitiveKind.FLOAT, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive xmaxLeaf = new SchemaNode.Primitive(
                "xmax", Repetition.REQUIRED, PrimitiveKind.FLOAT, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group bboxGroup =
                new SchemaNode.Group("bbox", Repetition.REQUIRED, List.of(xminLeaf, xmaxLeaf), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(bboxGroup), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);
        WriteOptions options = WriteOptions.builder().tempDir(file.getParent()).build();
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.setFloat(ColumnPath.of("bbox", "xmin"), 1.0f);
            appender.setFloat(ColumnPath.of("bbox", "xmax"), 2.0f);
            appender.endRow();
            appender.setFloat(ColumnPath.of("bbox", "xmin"), 3.0f);
            appender.setFloat(ColumnPath.of("bbox", "xmax"), 4.0f);
            appender.endRow();
            appender.flush();
        }
    }

    /** Writes a two-row Parquet file with an OPTIONAL struct column {@code bbox}: row 0 present, row 1 null. */
    private static void writeOptionalStructFixture(Path file) throws Exception {
        SchemaNode.Primitive xminLeaf = new SchemaNode.Primitive(
                "xmin", Repetition.OPTIONAL, PrimitiveKind.FLOAT, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive xmaxLeaf = new SchemaNode.Primitive(
                "xmax", Repetition.OPTIONAL, PrimitiveKind.FLOAT, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group bboxGroup =
                new SchemaNode.Group("bbox", Repetition.OPTIONAL, List.of(xminLeaf, xmaxLeaf), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(bboxGroup), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);
        WriteOptions options = WriteOptions.builder().tempDir(file.getParent()).build();
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.setFloat(ColumnPath.of("bbox", "xmin"), 1.0f);
            appender.setFloat(ColumnPath.of("bbox", "xmax"), 2.0f);
            appender.endRow();
            appender.setNull(ColumnPath.of("bbox"));
            appender.endRow();
            appender.flush();
        }
    }

    private static long rowCount(Path file) throws Exception {
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetSource source = ParquetSource.open(ByteRangeSources.from(reader));
            try (Stream<ParquetRecord> rows =
                    source.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return rows.count();
            }
        }
    }

    private static int rowGroupCount(Path file) throws Exception {
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetSource source = ParquetSource.open(ByteRangeSources.from(reader));
            return source.rowGroups().size();
        }
    }
}
