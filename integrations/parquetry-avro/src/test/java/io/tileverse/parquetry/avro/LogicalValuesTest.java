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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class LogicalValuesTest {

    private static MemorySegment segment(int... unsignedBytes) {
        byte[] bytes = new byte[unsignedBytes.length];
        for (int i = 0; i < unsignedBytes.length; i++) {
            bytes[i] = (byte) unsignedBytes[i];
        }
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    @Test
    void convertsPositiveDecimal() {
        Object converted = LogicalValues.convert(new LogicalType.Decimal(9, 2), segment(0x30, 0x39));
        assertThat(converted).isEqualTo(new BigDecimal("123.45"));
    }

    @Test
    void convertsNegativeDecimal() {
        Object converted = LogicalValues.convert(new LogicalType.Decimal(9, 2), segment(0xCF, 0xC7));
        assertThat(converted).isEqualTo(new BigDecimal("-123.45"));
    }

    @Test
    void rejectsEmptyDecimalBytes() {
        LogicalType.Decimal decimal = new LogicalType.Decimal(9, 2);
        MemorySegment empty = segment();
        assertThatThrownBy(() -> LogicalValues.convert(decimal, empty))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("empty unscaled value");
    }

    @Test
    void convertsDate() {
        assertThat(LogicalValues.convert(LogicalType.Date.INSTANCE, 0)).isEqualTo(LocalDate.of(1970, 1, 1));
        assertThat(LogicalValues.convert(LogicalType.Date.INSTANCE, 19000)).isEqualTo(LocalDate.of(2022, 1, 8));
    }

    @Test
    void convertsTimeMillis() {
        Object converted = LogicalValues.convert(LogicalType.TimeMillis.INSTANCE, 3_661_001);
        assertThat(converted).isEqualTo(LocalTime.of(1, 1, 1, 1_000_000));
    }

    @Test
    void convertsTimeMicros() {
        Object converted = LogicalValues.convert(LogicalType.TimeMicros.INSTANCE, 3_661_000_001L);
        assertThat(converted).isEqualTo(LocalTime.of(1, 1, 1, 1_000));
    }

    @Test
    void rejectsNegativeTimeMillis() {
        assertThatThrownBy(() -> LogicalValues.convert(LogicalType.TimeMillis.INSTANCE, -1))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("out of day range");
    }

    @Test
    void rejectsTimeMicrosOutsideDayRange() {
        // would wrap to 616ns under unchecked multiplication by 1000
        long wrapping = -18_446_744_073_709_551L;
        assertThatThrownBy(() -> LogicalValues.convert(LogicalType.TimeMicros.INSTANCE, wrapping))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("out of day range");
    }

    @Test
    void convertsTimestampMillis() {
        Object converted = LogicalValues.convert(LogicalType.TimestampMillis.INSTANCE, 1_000L);
        assertThat(converted).isEqualTo(Instant.parse("1970-01-01T00:00:01Z"));
    }

    @Test
    void convertsNegativeTimestampMicros() {
        Object converted = LogicalValues.convert(LogicalType.TimestampMicros.INSTANCE, -1L);
        assertThat(converted).isEqualTo(Instant.parse("1969-12-31T23:59:59.999999Z"));
    }

    @Test
    void convertsLocalTimestampMillis() {
        Object converted = LogicalValues.convert(LogicalType.LocalTimestampMillis.INSTANCE, 86_400_000L);
        assertThat(converted).isEqualTo(LocalDateTime.of(1970, 1, 2, 0, 0));
    }

    @Test
    void convertsLocalTimestampMicros() {
        Object converted = LogicalValues.convert(LogicalType.LocalTimestampMicros.INSTANCE, 1_000_001L);
        assertThat(converted).isEqualTo(LocalDateTime.of(1970, 1, 1, 0, 0, 1, 1_000));
    }

    @Test
    void convertsLittleEndianDuration() {
        MemorySegment fixed12 = segment(1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0);
        Object converted = LogicalValues.convert(LogicalType.Duration.INSTANCE, fixed12);
        assertThat(converted).isEqualTo(new AvroDuration(1, 2, 3));
    }

    @Test
    void convertsUuidFromString() {
        String text = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6";
        Object converted = LogicalValues.convert(LogicalType.Uuid.INSTANCE, text);
        assertThat(converted).isEqualTo(UUID.fromString(text));
    }

    @Test
    void convertsUuidFromBigEndianFixed16() {
        UUID expected = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
        MemorySegment fixed16 =
                segment(0xF8, 0x1D, 0x4F, 0xAE, 0x7D, 0xEC, 0x11, 0xD0, 0xA7, 0x65, 0x00, 0xA0, 0xC9, 0x1E, 0x6B, 0xF6);
        assertThat(LogicalValues.convert(LogicalType.Uuid.INSTANCE, fixed16)).isEqualTo(expected);
    }

    @Test
    void rejectsMalformedUuidString() {
        assertThatThrownBy(() -> LogicalValues.convert(LogicalType.Uuid.INSTANCE, "not-a-uuid"))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("Malformed uuid string: not-a-uuid");
    }

    /**
     * The most extreme encodable raw values stay inside java.time's supported ranges (verified here); the
     * DateTimeException wrapping in convert() is insurance for the day a raw type widens.
     */
    @Test
    void convertsExtremeTimestampMicros() {
        Object converted = LogicalValues.convert(LogicalType.TimestampMicros.INSTANCE, Long.MIN_VALUE);
        assertThat(converted).isEqualTo(Instant.ofEpochSecond(-9_223_372_036_855L, 224_192_000L));
    }

    @Test
    void convertsExtremeDate() {
        Object converted = LogicalValues.convert(LogicalType.Date.INSTANCE, Integer.MIN_VALUE);
        assertThat(converted).isEqualTo(LocalDate.ofEpochDay(Integer.MIN_VALUE));
    }

    @Test
    void passesUnknownLogicalTypeThrough() {
        Object raw = 42L;
        assertThat(LogicalValues.convert(new LogicalType.Unknown("custom"), raw))
                .isSameAs(raw);
    }
}
