/*
 * Copyright (c) 2026 Tileverse.io
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

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.enums.Type;

class FormatEmptyFileTest {

    @Test
    void readsEmptyFileMetaData(@TempDir Path tmp) throws Exception {
        Path file = TestFiles.emptyIntColumn(tmp);
        try (Storage storage = StorageFactory.open(tmp.toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            FileMetaData footer = Format.readFooter(reader);

            assertThat(footer.version()).isPositive();
            assertThat(footer.numRows()).isZero();
            assertThat(footer.schema()).hasSizeGreaterThanOrEqualTo(2);
            assertThat(footer.schema().get(1).type()).contains(Type.INT32);
            assertThat(footer.schema().get(1).name()).isEqualTo("id");
            assertThat(footer.rowGroups()).isEmpty();
        }
    }
}
