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
package io.tileverse.parquetry.format.codec;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.io.ByteRangeSource;

class FormatEmptyFileTest {

    @Test
    void readsEmptyFileMetaData(@TempDir Path tmp) throws Exception {
        Path file = TestFiles.emptyIntColumn(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);

            assertThat(footer.version()).isPositive();
            assertThat(footer.numRows()).isZero();
            assertThat(footer.schema()).hasSizeGreaterThanOrEqualTo(2);
            assertThat(footer.schema().get(1).type()).contains(PhysicalType.INT32);
            assertThat(footer.schema().get(1).name()).isEqualTo("id");
            assertThat(footer.rowGroups()).isEmpty();
        }
    }
}
