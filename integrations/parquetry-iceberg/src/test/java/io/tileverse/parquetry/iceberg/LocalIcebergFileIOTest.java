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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.io.ByteRangeSource;

class LocalIcebergFileIOTest {

    @TempDir
    Path tempDir;

    @Test
    void relocatesLogicalUriUnderPhysicalDir() throws Exception {
        Path dataDir = Files.createDirectories(tempDir.resolve("table/data"));
        Files.write(dataDir.resolve("a.parquet"), new byte[] {1, 2, 3, 4});
        IcebergFileIO io = new LocalIcebergFileIO("file:///warehouse/table", tempDir.resolve("table"));
        try (ByteRangeSource source = io.open("file:///warehouse/table/data/a.parquet")) {
            assertThat(source.size()).isEqualTo(4L);
        }
    }

    @Test
    void rejectsLocationOutsideTheLogicalRoot() {
        IcebergFileIO io = new LocalIcebergFileIO("file:///warehouse/table", tempDir);
        assertThatThrownBy(() -> io.open("file:///other/place/x.parquet"))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("file:///other/place/x.parquet");
    }
}
