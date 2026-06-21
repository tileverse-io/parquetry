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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.cli.CliExitCode;
import io.tileverse.parquetry.cli.Par;
import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
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
            ParquetDataset out = ParquetDataset.open(ByteRangeSources.from(reader));
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

    private static long rowCount(Path file) throws Exception {
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(ByteRangeSources.from(reader));
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return rows.count();
            }
        }
    }

    private static int rowGroupCount(Path file) throws Exception {
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(ByteRangeSources.from(reader));
            return dataset.rowGroups().size();
        }
    }
}
