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
package io.tileverse.parquetry.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.io.ByteRangeSource;

class SchemaBuilderTest {

    @Test
    void buildsFromEmptyIntColumn(@TempDir Path tmp) throws Exception {
        Path file = TestFixtures.emptyIntColumn(tmp);
        FileMetaData footer = readFooter(file);

        ParquetSchema schema = SchemaBuilder.build(footer.schema());

        assertThat(schema.root().children()).hasSize(1);
        SchemaNode idField = schema.root().children().get(0);
        assertThat(idField.name()).isEqualTo("id");
        assertThat(idField).isInstanceOf(SchemaNode.Primitive.class);
        assertThat(((SchemaNode.Primitive) idField).kind()).isEqualTo(PrimitiveKind.INT32);
    }

    @Test
    void buildsFromFlatIntSnappy(@TempDir Path tmp) throws Exception {
        Path file = TestFixtures.flatIntSnappy(tmp);
        FileMetaData footer = readFooter(file);

        ParquetSchema schema = SchemaBuilder.build(footer.schema());

        // 5 columns: int, long, float, double, boolean
        assertThat(schema.root().children()).hasSize(5);
        assertThat(schema.leafColumns()).hasSize(5);
    }

    @Test
    void buildsFromNestedList(@TempDir Path tmp) throws Exception {
        Path file = TestFixtures.nestedList(tmp);
        FileMetaData footer = readFooter(file);

        ParquetSchema schema = SchemaBuilder.build(footer.schema());

        // Avro array<int> -> Parquet list<int>: the total leaf count is exactly 1 (the int element).
        assertThat(schema.leafColumns()).hasSize(1);
    }

    @Test
    void buildsFromNestedMap(@TempDir Path tmp) throws Exception {
        Path file = TestFixtures.nestedMap(tmp);
        FileMetaData footer = readFooter(file);

        ParquetSchema schema = SchemaBuilder.build(footer.schema());

        // map<string,int> -> 2 leaves: key (string) and value (int)
        assertThat(schema.leafColumns()).hasSize(2);
    }

    private static FileMetaData readFooter(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFormat.readFooter(source);
        }
    }
}
