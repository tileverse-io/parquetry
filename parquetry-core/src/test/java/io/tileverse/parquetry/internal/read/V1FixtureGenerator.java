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
package io.tileverse.parquetry.internal.read;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;

/**
 * One-shot utility that emits the curated Parquet 1.x fixtures consumed by
 * {@link io.tileverse.parquetry.internal.read.DataPageV1ConformanceTest}.
 *
 * <p>This class is deliberately not a JUnit test: it has no {@code @Test} methods, only a {@code main} entrypoint. The
 * {@code surefire} and {@code failsafe} plugins discover tests by class-name patterns ({@code *Test}, {@code *IT},
 * {@code Test*}) plus the presence of {@code @Test}-annotated methods, so this class is inert during {@code mvn
 * verify}. Run it by hand whenever the fixture corpus needs to be regenerated:
 *
 * <pre>{@code
 * mvn -pl parquetry-core test-compile
 * mvn -pl parquetry-core exec:java \
 *   -Dexec.classpathScope=test \
 *   -Dexec.mainClass=io.tileverse.parquetry.read.V1FixtureGenerator
 * }</pre>
 *
 * <p>The fixtures are committed under {@code parquetry-core/src/test/resources/v1-fixtures/} so the conformance test
 * stays deterministic and is unaffected by future parquet-avro version bumps.
 *
 * <h2>Fixture inventory</h2>
 *
 * <ul>
 *   <li>{@code plain-int-string.parquet} - {@code (int year, string country)} uncompressed, plain-encoded values
 *   <li>{@code dict-string.parquet} - {@code (int year, string region)} uncompressed, dictionary-encoded values
 *   <li>{@code gzip-int-string.parquet} - same schema as {@code dict-string} but GZIP-compressed
 *   <li>{@code snappy-int-string.parquet} - same schema as {@code dict-string} but SNAPPY-compressed
 *   <li>{@code nullable-string.parquet} - {@code (int id, [nullable] string optional_label)} with mixed nulls
 * </ul>
 *
 * <p>Row counts and content are produced from {@link FixtureContent} so the conformance test recomputes the same
 * expected records without having to read the fixtures via a second oracle.
 */
public final class V1FixtureGenerator {

    private V1FixtureGenerator() {}

    // main entry point - args are required by the JVM launcher contract but unused here.
    @SuppressWarnings("java:S1172")
    public static void main(String[] args) throws IOException {
        Path outDir = resolveFixtureDirectory();
        System.out.println("Writing V1 fixtures into " + outDir);

        writePlainIntString(outDir.resolve(FixtureContent.PLAIN_INT_STRING));
        writeDictString(outDir.resolve(FixtureContent.DICT_STRING));
        writeGzipIntString(outDir.resolve(FixtureContent.GZIP_INT_STRING));
        writeSnappyIntString(outDir.resolve(FixtureContent.SNAPPY_INT_STRING));
        writeNullableString(outDir.resolve(FixtureContent.NULLABLE_STRING));
    }

    private static void writePlainIntString(Path out) throws IOException {
        // Many distinct strings + small row count keeps dictionary off and exercises the PLAIN binary path.
        Schema schema = parseSchema(FixtureContent.SCHEMA_YEAR_COUNTRY);
        writeIntStringFixture(out, schema, CompressionCodecName.UNCOMPRESSED, FixtureContent.PLAIN_ROW_COUNT, i -> {
            GenericData.Record r = new GenericData.Record(schema);
            r.put("year", FixtureContent.year(i));
            r.put("country", FixtureContent.distinctCountry(i));
            return r;
        });
    }

    private static void writeDictString(Path out) throws IOException {
        Schema schema = parseSchema(FixtureContent.SCHEMA_YEAR_REGION);
        writeIntStringFixture(out, schema, CompressionCodecName.UNCOMPRESSED, FixtureContent.DICT_ROW_COUNT, i -> {
            GenericData.Record r = new GenericData.Record(schema);
            r.put("year", FixtureContent.year(i));
            r.put("region", FixtureContent.region(i));
            return r;
        });
    }

    private static void writeGzipIntString(Path out) throws IOException {
        Schema schema = parseSchema(FixtureContent.SCHEMA_YEAR_REGION);
        writeIntStringFixture(out, schema, CompressionCodecName.GZIP, FixtureContent.GZIP_ROW_COUNT, i -> {
            GenericData.Record r = new GenericData.Record(schema);
            r.put("year", FixtureContent.year(i));
            r.put("region", FixtureContent.region(i));
            return r;
        });
    }

    private static void writeSnappyIntString(Path out) throws IOException {
        Schema schema = parseSchema(FixtureContent.SCHEMA_YEAR_REGION);
        writeIntStringFixture(out, schema, CompressionCodecName.SNAPPY, FixtureContent.SNAPPY_ROW_COUNT, i -> {
            GenericData.Record r = new GenericData.Record(schema);
            r.put("year", FixtureContent.year(i));
            r.put("region", FixtureContent.region(i));
            return r;
        });
    }

    private static void writeNullableString(Path out) throws IOException {
        Schema schema = parseSchema(FixtureContent.SCHEMA_ID_OPTIONAL_LABEL);
        writeIntStringFixture(out, schema, CompressionCodecName.UNCOMPRESSED, FixtureContent.NULLABLE_ROW_COUNT, i -> {
            GenericData.Record r = new GenericData.Record(schema);
            r.put("id", i);
            r.put("optional_label", FixtureContent.optionalLabel(i));
            return r;
        });
    }

    private static void writeIntStringFixture(
            Path out, Schema schema, CompressionCodecName codec, int rowCount, RowFactory rowFactory)
            throws IOException {
        Files.createDirectories(out.getParent());
        Files.deleteIfExists(out);
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(codec)
                .withWriterVersion(WriterVersion.PARQUET_1_0)
                .withRowGroupSize(16L * 1024 * 1024)
                .build()) {
            for (int i = 0; i < rowCount; i++) {
                writer.write(rowFactory.row(i));
            }
        }
        System.out.println("  wrote " + out.getFileName() + " (" + rowCount + " rows, " + codec + ")");
    }

    private static Schema parseSchema(String json) {
        return new Schema.Parser().parse(json);
    }

    private static Path resolveFixtureDirectory() {
        Path candidate = locateModuleRoot().resolve("src/test/resources/v1-fixtures");
        if (!Files.isDirectory(candidate)) {
            throw new IllegalStateException(
                    "Expected fixture directory does not exist; create it before running the generator: " + candidate);
        }
        return candidate;
    }

    /**
     * Walks up from {@code user.dir} until a directory containing {@code parquetry-core/pom.xml} is found; the fixture
     * generator may be launched either from the repo root or from the module directory.
     */
    private static Path locateModuleRoot() {
        Path cwd = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        if (cwd.endsWith("parquetry-core")) {
            return cwd;
        }
        Path nested = cwd.resolve("parquetry-core");
        if (Files.isRegularFile(nested.resolve("pom.xml"))) {
            return nested;
        }
        throw new IllegalStateException("Could not locate parquetry-core module from user.dir=" + cwd
                + "; run from the repo root or module directory");
    }

    @FunctionalInterface
    private interface RowFactory {
        GenericData.Record row(int rowIndex);
    }
}
