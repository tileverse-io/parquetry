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
package io.tileverse.parquetry.conformance.geoparquet;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeometryColumns;
import io.tileverse.parquetry.testkit.TestCorpus;

import tools.jackson.databind.json.JsonMapper;

/**
 * Shared access to the {@code geoparquet/geoparquet-testing} corpus for the core GeoParquet conformance suites. Each
 * tier is extracted once into a temp directory; helpers find the geometry leaves and pull a length-bounded copy of a
 * geometry value's WKB bytes from a {@link ParquetRecord} (the bound matters: a conformant decoder must not read past
 * the value's length).
 */
final class GeoParquetCorpus {

    private static final Path DATA = extractOnce("geoparquet-testing/data");
    private static final Path SAMPLES = extractOnce("geoparquet-testing/samples");
    private static final Path BAD_DATA = extractOnce("geoparquet-testing/bad_data");

    private GeoParquetCorpus() {}

    static Path data() {
        return DATA;
    }

    static Path samples() {
        return SAMPLES;
    }

    static Path badData() {
        return BAD_DATA;
    }

    /** The {@code data/crs/} subdirectory, where the five PROJJSON CRS fixtures live. */
    static Path crs() {
        return DATA.resolve("crs");
    }

    /** Leaf paths annotated with the native GEOMETRY / GEOGRAPHY logical type. */
    static Set<ColumnPath> geometryColumns(ParquetSchema schema) {
        return GeometryColumns.resolve(schema, Optional.empty());
    }

    /** A length-bounded copy of the WKB bytes at {@code column}, or {@code null} when the value is absent. */
    static byte[] wkbBytes(ParquetRecord record, ColumnPath column) {
        return record.readBinary(
                column,
                (backing, offset, length) -> backing.asSlice(offset, length).toArray(JAVA_BYTE));
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> manifest() {
        try {
            Path manifest = BAD_DATA.resolve("manifest.json");
            return JsonMapper.builder().build().readValue(Files.readAllBytes(manifest), Map.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read the bad_data manifest", e);
        }
    }

    private static Path extractOnce(String resourceDir) {
        try {
            Path target = Files.createTempDirectory("geoparquet-testing-");
            return TestCorpus.extractDirectory(resourceDir, target);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not extract " + resourceDir, e);
        }
    }
}
