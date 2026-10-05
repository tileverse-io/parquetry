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
package io.tileverse.parquetry.format;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * An index section located outside its file is malformed, and is refused before a buffer of its declared length is
 * allocated.
 */
class ParquetFormatSectionBoundsTest {

    private static final int FILE_BYTES = 16;

    @TempDir
    Path tempDir;

    @ParameterizedTest(name = "{0}")
    @MethodSource("sectionsOutsideTheFile")
    void indexSectionOutsideTheFileIsMalformed(String name, long offset, int length) throws IOException {
        Path file = Files.write(tempDir.resolve("sixteen-bytes.bin"), new byte[FILE_BYTES]);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            assertThatThrownBy(() -> ParquetFormat.readColumnIndex(source, offset, length))
                    .isInstanceOf(MalformedFileException.class)
                    .hasMessageContaining("reaches outside the file");
            assertThatThrownBy(() -> ParquetFormat.readOffsetIndex(source, offset, length))
                    .isInstanceOf(MalformedFileException.class)
                    .hasMessageContaining("reaches outside the file");
        }
    }

    static Stream<Arguments> sectionsOutsideTheFile() {
        return Stream.of(
                Arguments.of("one byte past the end", 8L, 9),
                Arguments.of("2 GB from the start", 0L, Integer.MAX_VALUE),
                Arguments.of("starting past the end", 32L, 4),
                Arguments.of("starting before the file", -4L, 8));
    }
}
