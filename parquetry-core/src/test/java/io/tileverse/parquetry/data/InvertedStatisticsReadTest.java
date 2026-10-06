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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.FooterRewrite;

/**
 * Reads a file with the decimal bounds left by a writer ordering a binary decimal by its unsigned bytes, as parquetry
 * releases up to 1.0-RC3 did: a row group holding both signs gets its smallest positive value as the minimum and its
 * negative value nearest to zero as the maximum. Pruning must ignore such a pair and keep the row group.
 */
class InvertedStatisticsReadTest {

    private static final ColumnPath AMOUNT = ColumnPath.of("amount");
    private static final int SCALE = 2;
    private static final int PRECISION = 9;
    private static final List<Integer> UNSCALED_AMOUNTS = List.of(-4995, 550, -111, 4991);

    @TempDir
    Path tempDir;

    @Test
    void invertedDecimalBoundsDropNoMatchingRowGroup() throws IOException {
        Path file = withBoundsOrderedByUnsignedBytes(writeAmounts());
        Predicate aboveTen = new Predicate.Gt(AMOUNT, decimal(1000));
        Predicate belowMinusTen = new Predicate.Lt(AMOUNT, decimal(-1000));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.count(aboveTen, ReadOptions.DEFAULTS)).isEqualTo(1L);
            assertThat(reader.count(belowMinusTen, ReadOptions.DEFAULTS)).isEqualTo(1L);
            assertThat(statsDecision(reader, aboveTen)).isInstanceOf(PruningDecision.NotApplied.class);
        }
    }

    private static PruningDecision statsDecision(ParquetFileReader reader, Predicate predicate) {
        ExplainPlan plan = reader.explain(predicate, Projection.ALL, ReadOptions.DEFAULTS);
        return plan.rowGroups().getFirst().tiers().getFirst();
    }

    private Path withBoundsOrderedByUnsignedBytes(Path intact) throws IOException {
        MemorySegment smallestPositive = MemorySegment.ofArray(bigEndian(550));
        MemorySegment negativeNearestToZero = MemorySegment.ofArray(bigEndian(-111));
        return FooterRewrite.rewrite(
                intact,
                tempDir.resolve("inverted.parquet"),
                FooterRewrite.statisticsBounds(AMOUNT, 0, smallestPositive, negativeNearestToZero));
    }

    private Path writeAmounts() throws IOException {
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        List<Map<ColumnPath, Object>> rows = UNSCALED_AMOUNTS.stream()
                .map(unscaled -> Map.<ColumnPath, Object>of(AMOUNT, bigEndian(unscaled)))
                .toList();
        return WriteFixtures.writeRows(tempDir.resolve("amounts.parquet"), decimalSchema(), options, rows);
    }

    private static ParquetSchema decimalSchema() {
        LogicalType decimal = new LogicalType.Decimal(SCALE, PRECISION);
        SchemaNode.Primitive amount = WriteFixtures.requiredLeaf(
                "amount", PrimitiveKind.FIXED_LEN_BYTE_ARRAY, OptionalInt.of(Integer.BYTES), decimal);
        return WriteFixtures.schemaOf(amount);
    }

    private static Value decimal(int unscaled) {
        return new Value.DecimalVal(BigDecimal.valueOf(unscaled, SCALE));
    }

    /** A decimal's unscaled value as four big-endian two's complement bytes. */
    private static byte[] bigEndian(int unscaled) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(unscaled).array();
    }
}
