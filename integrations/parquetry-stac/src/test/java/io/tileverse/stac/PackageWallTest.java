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
package io.tileverse.stac;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * Pins the extraction boundary: no class in {@code io.tileverse.stac} may depend on a parquetry type. The model, the
 * JSON reader, and the SPI must stay portable to a future standalone library; this scans the package's Java sources for
 * a forbidden import.
 */
class PackageWallTest {

    @Test
    void noSourceImportsParquetry() throws IOException {
        Path packageDir = Path.of("src", "main", "java", "io", "tileverse", "stac");
        assertThat(Files.isDirectory(packageDir))
                .as("expected to run from the parquetry-stac module directory, looking at "
                        + packageDir.toAbsolutePath())
                .isTrue();

        List<String> offenders = new ArrayList<>();
        try (Stream<Path> sources = Files.walk(packageDir)) {
            sources.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
                if (importsParquetry(path)) {
                    offenders.add(path.getFileName().toString());
                }
            });
        }
        assertThat(offenders)
                .as("io.tileverse.stac must not import any io.tileverse.parquetry type")
                .isEmpty();
    }

    private static boolean importsParquetry(Path source) {
        try {
            return Files.readAllLines(source).stream()
                    .anyMatch(line -> line.trim().startsWith("import io.tileverse.parquetry."));
        } catch (IOException unreadable) {
            throw new IllegalStateException("reading " + source, unreadable);
        }
    }
}
