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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class LogicalEncodingTest {

    private final AvroDatumEncoder encoder = new AvroDatumEncoder();
    private final AvroDatumDecoder decoder = new AvroDatumDecoder();

    private Object roundTrip(AvroSchema schema, Object value) {
        AvroBinaryEncoder out = new AvroBinaryEncoder();
        encoder.encode(schema, value, out);
        return decoder.decode(schema, new AvroBinaryDecoder(MemorySegment.ofArray(out.toByteArray())));
    }

    @Test
    void roundTripsDate() {
        AvroSchema schema = AvroSchema.parse("{\"type\":\"int\",\"logicalType\":\"date\"}");
        assertThat(roundTrip(schema, LocalDate.of(2026, 6, 11))).isEqualTo(LocalDate.of(2026, 6, 11));
    }

    @Test
    void roundTripsTimestampMillisAndTimeMillis() {
        AvroSchema ts = AvroSchema.parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");
        Instant instant = Instant.ofEpochMilli(1_700_000_000_000L);
        assertThat(roundTrip(ts, instant)).isEqualTo(instant);

        AvroSchema time = AvroSchema.parse("{\"type\":\"int\",\"logicalType\":\"time-millis\"}");
        assertThat(roundTrip(time, LocalTime.of(1, 2, 3))).isEqualTo(LocalTime.of(1, 2, 3));
    }

    @Test
    void roundTripsUuid() {
        AvroSchema schema = AvroSchema.parse("{\"type\":\"string\",\"logicalType\":\"uuid\"}");
        UUID id = UUID.fromString("12345678-1234-1234-1234-123456789abc");
        assertThat(roundTrip(schema, id)).isEqualTo(id);
    }

    @Test
    void roundTripsUuidOnFixed16() {
        AvroSchema schema =
                AvroSchema.parse("{\"type\":\"fixed\",\"name\":\"U\",\"size\":16,\"logicalType\":\"uuid\"}");
        UUID id = UUID.fromString("12345678-1234-1234-1234-123456789abc");
        assertThat(roundTrip(schema, id)).isEqualTo(id);
    }

    @Test
    void roundTripsDecimalOnBytes() {
        AvroSchema schema =
                AvroSchema.parse("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}");
        BigDecimal value = new BigDecimal("123.45");
        assertThat(roundTrip(schema, value)).isEqualTo(value);
    }

    @Test
    void roundTripsDecimalOnFixedWithSignExtension() {
        AvroSchema schema = AvroSchema.parse(
                "{\"type\":\"fixed\",\"name\":\"D\",\"size\":8,\"logicalType\":\"decimal\",\"precision\":18,\"scale\":4}");
        BigDecimal value = new BigDecimal("-1.2345");
        assertThat(roundTrip(schema, value)).isEqualTo(value);
    }

    @Test
    void roundTripsDuration() {
        AvroSchema schema =
                AvroSchema.parse("{\"type\":\"fixed\",\"name\":\"Dur\",\"size\":12,\"logicalType\":\"duration\"}");
        AvroDuration value = new AvroDuration(3, 4, 5000);
        assertThat(roundTrip(schema, value)).isEqualTo(value);
    }

    @Test
    void defersWhenValueIsAlreadyRaw() {
        AvroSchema schema = AvroSchema.parse("{\"type\":\"int\",\"logicalType\":\"date\"}");
        assertThat(roundTrip(schema, 20250)).isEqualTo(LocalDate.ofEpochDay(20250));
    }

    @Test
    void rejectsWrongDecimalScale() {
        AvroSchema schema =
                AvroSchema.parse("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}");
        assertThatThrownBy(() -> {
                    AvroBinaryEncoder out = new AvroBinaryEncoder();
                    encoder.encode(schema, new BigDecimal("1.234"), out);
                })
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("scale");
    }

    @Test
    void selectsLogicalUnionBranchFromTypedValue() {
        AvroSchema schema = AvroSchema.parse("[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]");
        LocalDate date = LocalDate.of(2026, 1, 2);
        assertThat(roundTrip(schema, date)).isEqualTo(date);
        assertThat(roundTrip(schema, null)).isNull();
    }
}
