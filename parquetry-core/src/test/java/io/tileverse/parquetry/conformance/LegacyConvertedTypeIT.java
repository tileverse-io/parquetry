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
package io.tileverse.parquetry.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Reads {@code apache/parquet-testing} fixtures written with only the deprecated {@code ConvertedType} (no modern
 * {@code LogicalType}) and asserts each annotated column gains the equivalent logical type. These files come from
 * legacy writers (duckdb, older parquet-mr); without the backfill a {@code UTF8} column reads as raw binary and a
 * {@code DECIMAL} column loses its scale and precision.
 *
 * <p>{@code SchemaBuilderConvertedTypeTest} covers the converted-type mapping exhaustively over synthetic elements;
 * this proves the same backfill end-to-end over a real file footer decoded from the wire.
 */
class LegacyConvertedTypeIT {

    @ParameterizedTest(name = "{0} [{1}] backfills to {2}")
    @MethodSource("legacyConvertedTypeColumns")
    void backfillsLogicalTypeFromRealFileFooter(String fixture, String column, LogicalType expected) throws Exception {
        Path file = CorpusFixtures.parquetTestingData().resolve(fixture);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            SchemaNode leaf = reader.schema().find(ColumnPath.of(column)).orElseThrow();
            assertThat(((SchemaNode.Primitive) leaf).logicalType()).contains(expected);
        }
    }

    static Stream<Arguments> legacyConvertedTypeColumns() {
        return Stream.of(
                Arguments.of("datapage_v2.snappy.parquet", "a", new LogicalType.StringType()),
                Arguments.of("int32_decimal.parquet", "value", new LogicalType.Decimal(2, 4)),
                Arguments.of("int64_decimal.parquet", "value", new LogicalType.Decimal(2, 10)),
                Arguments.of("byte_array_decimal.parquet", "value", new LogicalType.Decimal(2, 4)),
                Arguments.of("fixed_length_decimal.parquet", "value", new LogicalType.Decimal(2, 25)));
    }
}
