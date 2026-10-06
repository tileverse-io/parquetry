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
package io.tileverse.parquetry.format.codec;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BoundaryOrder;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.Statistics;

/**
 * Wire-level coverage for the NaN counts of floating-point columns: {@code Statistics.nan_count} (field 9) and
 * {@code ColumnIndex.nan_counts} (field 8). A count of zero is written and read back as zero, distinct from a count
 * never recorded.
 */
class NaNCountWireTest {

    private static final short STATISTICS_NAN_COUNT = 9;
    private static final short COLUMN_INDEX_NAN_COUNTS = 8;

    @Test
    void statisticsNanCountRoundTrips() throws IOException {
        Statistics original = Statistics.builder()
                .nullCount(OptionalLong.of(2L))
                .nanCount(OptionalLong.of(7L))
                .build();

        Statistics decoded = readStatistics(writeStatistics(original));

        assertThat(decoded.nanCount()).hasValue(7L);
        assertThat(decoded.nullCount()).hasValue(2L);
    }

    @Test
    void statisticsNanCountOfZeroIsWritten() throws IOException {
        Statistics original = Statistics.builder().nanCount(OptionalLong.of(0L)).build();

        Statistics decoded = readStatistics(writeStatistics(original));

        assertThat(decoded.nanCount()).hasValue(0L);
    }

    @Test
    void statisticsWithoutANanCountReadBackWithoutOne() throws IOException {
        Statistics original =
                Statistics.builder().nullCount(OptionalLong.of(2L)).build();

        Statistics decoded = readStatistics(writeStatistics(original));

        assertThat(decoded.nanCount()).isEmpty();
    }

    @Test
    void statisticsNanCountIsThriftFieldNine() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CompactProtocolWriter writer = new CompactProtocolWriter(bytes);
        writer.writeStructBegin();
        writer.writeI64Field(STATISTICS_NAN_COUNT, 5L);
        writer.writeFieldStop();
        writer.writeStructEnd();

        Statistics decoded = readStatistics(bytes.toByteArray());

        assertThat(decoded.nanCount()).hasValue(5L);
    }

    @Test
    void columnIndexNanCountsRoundTrip() throws IOException {
        ColumnIndex original = columnIndex(Optional.of(List.of(0L, 3L)));

        ColumnIndex decoded = readColumnIndex(writeColumnIndex(original));

        assertThat(decoded.nanCounts()).contains(List.of(0L, 3L));
        assertThat(decoded.nullCounts()).contains(List.of(1L, 0L));
    }

    @Test
    void columnIndexWithoutNanCountsReadsBackWithoutThem() throws IOException {
        ColumnIndex original = columnIndex(Optional.empty());

        ColumnIndex decoded = readColumnIndex(writeColumnIndex(original));

        assertThat(decoded.nanCounts()).isEmpty();
    }

    @Test
    void columnIndexNanCountsAreThriftFieldEight() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CompactProtocolWriter writer = new CompactProtocolWriter(bytes);
        writer.writeStructBegin();
        writer.writeListField(
                COLUMN_INDEX_NAN_COUNTS, CompactType.I64, List.of(4L, 0L), CompactProtocolWriter::writeI64);
        writer.writeFieldStop();
        writer.writeStructEnd();

        ColumnIndex decoded = readColumnIndex(bytes.toByteArray());

        assertThat(decoded.nanCounts()).contains(List.of(4L, 0L));
    }

    private static ColumnIndex columnIndex(Optional<List<Long>> nanCounts) {
        MemorySegment bound = MemorySegment.ofArray(new byte[] {0, 0, (byte) 0x80, 0x3F});
        return new ColumnIndex(
                List.of(false, false),
                List.of(bound, bound),
                List.of(bound, bound),
                BoundaryOrder.UNORDERED,
                Optional.of(List.of(1L, 0L)),
                Optional.empty(),
                Optional.empty(),
                nanCounts);
    }

    private static byte[] writeStatistics(Statistics statistics) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        StatisticsSerializer.serialize(new CompactProtocolWriter(bytes), statistics);
        return bytes.toByteArray();
    }

    private static Statistics readStatistics(byte[] wire) throws IOException {
        return StatisticsDeserializer.read(new CompactProtocolReader(new ByteArrayInputStream(wire)));
    }

    private static byte[] writeColumnIndex(ColumnIndex index) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ColumnIndexSerializer.serialize(new CompactProtocolWriter(bytes), index);
        return bytes.toByteArray();
    }

    private static ColumnIndex readColumnIndex(byte[] wire) throws IOException {
        return ColumnIndexDeserializer.read(new CompactProtocolReader(new ByteArrayInputStream(wire)));
    }
}
