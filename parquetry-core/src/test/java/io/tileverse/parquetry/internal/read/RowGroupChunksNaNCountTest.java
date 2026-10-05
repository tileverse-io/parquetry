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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.filter.NaNCells;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;

/**
 * Covers how the NaN count of a chunk reaches the pruning inputs. A count of zero rules NaN cells out, and NaN and null
 * counts adding up to the values of the chunk tell that it holds no number. A flat column has one value per row: counts
 * adding up to another value count come from a writer counting its values some other way, and tell nothing.
 */
class RowGroupChunksNaNCountTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final long ROWS = 10L;

    static Stream<Arguments> countedChunks() {
        return Stream.of(
                Arguments.of("a NaN count of zero", count(0L), count(3L), ROWS, NaNCells.ABSENT),
                Arguments.of("NaN and null counts adding up to the rows", count(7L), count(3L), ROWS, NaNCells.ALL),
                Arguments.of("a number among the values", count(4L), count(3L), ROWS, NaNCells.POSSIBLE),
                Arguments.of("no NaN count", OptionalLong.empty(), count(3L), ROWS, NaNCells.POSSIBLE),
                Arguments.of(
                        "counts adding up to a value count short of the rows",
                        count(6L),
                        count(3L),
                        ROWS - 1,
                        NaNCells.POSSIBLE));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("countedChunks")
    void chunkStatisticsTellWhatItsCountsProve(
            String scenario, OptionalLong nanCount, OptionalLong nullCount, long numValues, NaNCells expected) {
        FileMetaData footer = footer(nanCount, nullCount, numValues);
        ParquetSchema schema = SchemaBuilder.build(footer.schema());

        RowGroupChunks chunks = TestRowGroupChunks.of(footer, 0, schema, TestRowGroupChunks.noIndexSections());

        assertThat(chunks.stats(V).orElseThrow().nans()).isEqualTo(expected);
    }

    private static OptionalLong count(long value) {
        return OptionalLong.of(value);
    }

    /** A one-row-group footer of {@link #ROWS} rows with one nullable DOUBLE column. */
    private static FileMetaData footer(OptionalLong nanCount, OptionalLong nullCount, long numValues) {
        Statistics statistics =
                Statistics.builder().nullCount(nullCount).nanCount(nanCount).build();
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.DOUBLE)
                .codec(CompressionCodec.UNCOMPRESSED)
                .pathInSchema(List.of("v"))
                .numValues(numValues)
                .totalCompressedSize(10L)
                .dataPageOffset(4L)
                .statistics(Optional.of(statistics))
                .build();
        ColumnChunk chunk = ColumnChunk.builder().metaData(Optional.of(meta)).build();
        RowGroup rowGroup =
                RowGroup.builder().columns(List.of(chunk)).numRows(ROWS).build();
        return FileMetaData.builder()
                .version(2)
                .schema(List.of(element("schema", Optional.empty(), OptionalInt.of(1)), leafElement()))
                .numRows(ROWS)
                .rowGroups(List.of(rowGroup))
                .build();
    }

    private static SchemaElement leafElement() {
        return element("v", Optional.of(PhysicalType.DOUBLE), OptionalInt.empty());
    }

    private static SchemaElement element(String name, Optional<PhysicalType> type, OptionalInt children) {
        Optional<FieldRepetitionType> repetition =
                type.isPresent() ? Optional.of(FieldRepetitionType.OPTIONAL) : Optional.empty();
        return new SchemaElement(
                type,
                OptionalInt.empty(),
                repetition,
                name,
                children,
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }
}
