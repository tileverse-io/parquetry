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
package io.tileverse.parquetry.iceberg;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.UuidConverter;
import io.tileverse.parquetry.testkit.TestCorpus;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class IcebergScalarValuesTest {

    private static final IcebergType TS = new IcebergType.TimestampType(false, TimeUnit.MICROS);
    private static final IcebergType TSTZ = new IcebergType.TimestampType(true, TimeUnit.MICROS);
    private static final IcebergType TSTZ_NS = new IcebergType.TimestampType(true, TimeUnit.NANOS);
    private static final IcebergType TIME = new IcebergType.TimeType();
    private static final IcebergType DEC = new IcebergType.DecimalType(9, 2);
    private static final IcebergType UUID_TYPE = new IcebergType.UuidType();
    private static final IcebergType BINARY = new IcebergType.BinaryType();
    private static final IcebergType FIXED4 = new IcebergType.FixedType(4);
    private static final IcebergType GEOMETRY = new IcebergType.GeometryType(Optional.empty());
    private static final IcebergType UNKNOWN = new IcebergType.UnknownType();
    private static final UUID SAMPLE_UUID = UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7");
    private static final LocalDateTime SAMPLE = LocalDateTime.of(2024, 6, 15, 12, 30, 45, 123_456_000);

    @TempDir
    Path tempDir;

    @Test
    void boundDecodesMicrosTimestamp() {
        long micros = SAMPLE.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + 123_456L;
        assertThat(IcebergScalarValues.fromBound(TS, le(micros))).isEqualTo(new Value.TimestampVal(SAMPLE, false));
    }

    @Test
    void boundDecodesNanosTimestamp() {
        LocalDateTime withNanos = LocalDateTime.of(2024, 1, 1, 0, 0, 0, 7);
        long nanos = withNanos.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L + 7L;
        assertThat(IcebergScalarValues.fromBound(TSTZ_NS, le(nanos)))
                .isEqualTo(new Value.TimestampVal(withNanos, true));
    }

    @Test
    void boundDecodesPre1970TimestampExactly() {
        LocalDateTime before = LocalDateTime.of(1969, 12, 31, 23, 59, 59, 500_000_000);
        assertThat(IcebergScalarValues.fromBound(TS, le(-500_000L))).isEqualTo(new Value.TimestampVal(before, false));
    }

    @Test
    void boundDecodesTimeAsMicrosSinceMidnight() {
        assertThat(IcebergScalarValues.fromBound(TIME, le(45_296_000_789L)))
                .isEqualTo(new Value.TimeVal(LocalTime.of(12, 34, 56, 789_000)));
    }

    @Test
    void boundDecodesNegativeDecimalFromMinimalTwosComplement() {
        assertThat(IcebergScalarValues.fromBound(DEC, bytes(0xfd, 0x8f)))
                .isEqualTo(new Value.DecimalVal(new BigDecimal("-6.25")));
    }

    @Test
    void boundDecodesDecimalWiderThanALong() {
        BigDecimal wide = new BigDecimal("12345678901234567.890");
        byte[] unscaled = wide.unscaledValue().toByteArray();
        assertThat(unscaled).hasSizeGreaterThan(8);
        IcebergType bigDecimal = new IcebergType.DecimalType(20, 3);
        assertThat(IcebergScalarValues.fromBound(bigDecimal, MemorySegment.ofArray(unscaled)))
                .isEqualTo(new Value.DecimalVal(wide));
    }

    @Test
    void boundDecodesUuidBigEndian() {
        MemorySegment raw = MemorySegment.ofArray(UuidConverter.toBytes(SAMPLE_UUID));
        assertThat(IcebergScalarValues.fromBound(UUID_TYPE, raw)).isEqualTo(new Value.UuidVal(SAMPLE_UUID));
    }

    @Test
    void boundKeepsFixedAndBinaryBytes() {
        MemorySegment raw = bytes(1, 2, 3, 4);
        assertThat(bytesOf(IcebergScalarValues.fromBound(FIXED4, raw))).containsExactly(1, 2, 3, 4);
        assertThat(bytesOf(IcebergScalarValues.fromBound(BINARY, raw))).containsExactly(1, 2, 3, 4);
    }

    @Test
    void boundStillDecodesTheOriginalTypes() {
        assertThat(IcebergScalarValues.fromBound(new IcebergType.IntType(), leInt(42)))
                .isEqualTo(new Value.IntVal(42));
        assertThat(IcebergScalarValues.fromBound(new IcebergType.LongType(), le(1234567890123L)))
                .isEqualTo(new Value.LongVal(1234567890123L));
        assertThat(IcebergScalarValues.fromBound(new IcebergType.DoubleType(), le(Double.doubleToLongBits(-122.5))))
                .isEqualTo(new Value.DoubleVal(-122.5));
        assertThat(IcebergScalarValues.fromBound(
                        new IcebergType.StringType(), MemorySegment.ofArray("abc".getBytes(UTF_8))))
                .isEqualTo(new Value.StringVal("abc"));
        assertThat(IcebergScalarValues.fromBound(new IcebergType.DateType(), leInt(19000)))
                .isEqualTo(new Value.DateVal(LocalDate.ofEpochDay(19000)));
        assertThat(IcebergScalarValues.fromBound(new IcebergType.BoolType(), bytes(1)))
                .isEqualTo(new Value.BoolVal(true));
    }

    @Test
    void boundRejectsGeometryUnknownTruncatedAndEmpty() {
        MemorySegment four = leInt(7);
        MemorySegment empty = MemorySegment.ofArray(new byte[0]);
        IcebergType longType = new IcebergType.LongType();
        assertThatThrownBy(() -> IcebergScalarValues.fromBound(GEOMETRY, four))
                .isInstanceOf(IcebergFormatException.class);
        assertThatThrownBy(() -> IcebergScalarValues.fromBound(UNKNOWN, four))
                .isInstanceOf(IcebergFormatException.class);
        assertThatThrownBy(() -> IcebergScalarValues.fromBound(longType, four))
                .isInstanceOf(IcebergFormatException.class);
        assertThatThrownBy(() -> IcebergScalarValues.fromBound(DEC, empty)).isInstanceOf(IcebergFormatException.class);
    }

    @Test
    void jsonDecodesEveryDefaultForm() {
        assertThat(fromJson(UUID_TYPE, "\"f79c3e09-677c-4bbd-a479-3f349cb785e7\""))
                .isEqualTo(new Value.UuidVal(SAMPLE_UUID));
        assertThat(fromJson(TS, "\"2017-11-16T22:31:08.123456\""))
                .isEqualTo(new Value.TimestampVal(LocalDateTime.of(2017, 11, 16, 22, 31, 8, 123_456_000), false));
        assertThat(fromJson(TSTZ, "\"2017-11-16T22:31:08.123456+00:00\""))
                .isEqualTo(new Value.TimestampVal(LocalDateTime.of(2017, 11, 16, 22, 31, 8, 123_456_000), true));
        assertThat(fromJson(TSTZ, "\"2017-11-16T22:31:08.123456+02:00\""))
                .isEqualTo(new Value.TimestampVal(LocalDateTime.of(2017, 11, 16, 20, 31, 8, 123_456_000), true));
        assertThat(fromJson(TIME, "\"22:31:08.123456\""))
                .isEqualTo(new Value.TimeVal(LocalTime.of(22, 31, 8, 123_456_000)));
        assertThat(fromJson(DEC, "\"14.20\"")).isEqualTo(new Value.DecimalVal(new BigDecimal("14.20")));
        assertThat(bytesOf(fromJson(BINARY, "\"0AFF\""))).containsExactly(0x0a, 0xff);
        assertThat(bytesOf(fromJson(new IcebergType.FixedType(2), "\"0102\""))).containsExactly(1, 2);
        assertThat(fromJson(new IcebergType.DateType(), "\"2024-01-15\""))
                .isEqualTo(new Value.DateVal(LocalDate.of(2024, 1, 15)));
        assertThat(fromJson(new IcebergType.StringType(), "\"n/a\"")).isEqualTo(new Value.StringVal("n/a"));
        assertThat(fromJson(new IcebergType.IntType(), "7")).isEqualTo(new Value.IntVal(7));
        assertThat(fromJson(new IcebergType.LongType(), "9000000000")).isEqualTo(new Value.LongVal(9_000_000_000L));
        assertThat(fromJson(new IcebergType.FloatType(), "1.5")).isEqualTo(new Value.FloatVal(1.5f));
        assertThat(fromJson(new IcebergType.DoubleType(), "2.5")).isEqualTo(new Value.DoubleVal(2.5));
        assertThat(fromJson(new IcebergType.BoolType(), "true")).isEqualTo(new Value.BoolVal(true));
    }

    @Test
    void jsonRejectsMalformedText() {
        IcebergType date = new IcebergType.DateType();
        assertThatThrownBy(() -> fromJson(UUID_TYPE, "\"nope\""))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("f");
        assertThatThrownBy(() -> fromJson(TIME, "\"25:00:00\"")).isInstanceOf(IcebergFormatException.class);
        assertThatThrownBy(() -> fromJson(DEC, "\"abc\"")).isInstanceOf(IcebergFormatException.class);
        assertThatThrownBy(() -> fromJson(BINARY, "\"0G\"")).isInstanceOf(IcebergFormatException.class);
        assertThatThrownBy(() -> fromJson(date, "\"15/01/2024\"")).isInstanceOf(IcebergFormatException.class);
    }

    @Test
    void jsonRejectsADecimalDefaultAtAnotherScale() {
        assertThatThrownBy(() -> fromJson(DEC, "\"14.2\""))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("scale");
    }

    @Test
    void jsonRejectsANanosecondTimestampDefault() {
        assertThatThrownBy(() -> fromJson(TSTZ_NS, "\"2017-11-16T22:31:08.123456789+00:00\""))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("nanosecond");
    }

    @Test
    void jsonRejectsAWrongJsonShape() {
        IcebergType intType = new IcebergType.IntType();
        IcebergType stringType = new IcebergType.StringType();
        IcebergType boolType = new IcebergType.BoolType();
        assertThatThrownBy(() -> fromJson(intType, "\"7\"")).isInstanceOf(IcebergFormatException.class);
        assertThatThrownBy(() -> fromJson(stringType, "7")).isInstanceOf(IcebergFormatException.class);
        assertThatThrownBy(() -> fromJson(boolType, "1")).isInstanceOf(IcebergFormatException.class);
    }

    @Test
    void jsonRejectsANumericDefaultThatItsTypeCannotHoldExactly() {
        IcebergType intType = new IcebergType.IntType();
        IcebergType longType = new IcebergType.LongType();
        assertThatThrownBy(() -> fromJson(intType, "3000000000"))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("f")
                .hasMessageContaining("int");
        assertThatThrownBy(() -> fromJson(intType, "1.5"))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("f");
        assertThatThrownBy(() -> fromJson(longType, "9223372036854775808"))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("f");
    }

    @Test
    void jsonKeepsANumericDefaultThatItsTypeHoldsExactly() {
        assertThat(fromJson(new IcebergType.IntType(), "7")).isEqualTo(new Value.IntVal(7));
        assertThat(fromJson(new IcebergType.LongType(), "3000000000")).isEqualTo(new Value.LongVal(3_000_000_000L));
    }

    @Test
    void jsonRejectsGeometryAndUnknownDefaults() {
        assertThatThrownBy(() -> fromJson(GEOMETRY, "\"x\""))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("geometry");
        assertThatThrownBy(() -> fromJson(UNKNOWN, "1")).isInstanceOf(IcebergFormatException.class);
    }

    @Test
    void partitionAcceptsLogicalObjectsAndRawFallbacks() {
        assertThat(fromPartition(UUID_TYPE, SAMPLE_UUID)).isEqualTo(new Value.UuidVal(SAMPLE_UUID));
        assertThat(fromPartition(UUID_TYPE, MemorySegment.ofArray(UuidConverter.toBytes(SAMPLE_UUID))))
                .isEqualTo(new Value.UuidVal(SAMPLE_UUID));
        assertThat(fromPartition(DEC, new BigDecimal("-6.25")))
                .isEqualTo(new Value.DecimalVal(new BigDecimal("-6.25")));
        assertThat(fromPartition(DEC, bytes(0xfd, 0x8f))).isEqualTo(new Value.DecimalVal(new BigDecimal("-6.25")));
        Instant instant = SAMPLE.toInstant(ZoneOffset.UTC);
        assertThat(fromPartition(TS, instant)).isEqualTo(new Value.TimestampVal(SAMPLE, false));
        assertThat(fromPartition(TSTZ, SAMPLE)).isEqualTo(new Value.TimestampVal(SAMPLE, true));
        long micros = SAMPLE.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + 123_456L;
        assertThat(fromPartition(TS, micros)).isEqualTo(new Value.TimestampVal(SAMPLE, false));
        long nanos = SAMPLE.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L + 123_456_000L;
        assertThat(fromPartition(TSTZ_NS, nanos)).isEqualTo(new Value.TimestampVal(SAMPLE, true));
        assertThat(fromPartition(TIME, LocalTime.NOON)).isEqualTo(new Value.TimeVal(LocalTime.NOON));
        assertThat(fromPartition(TIME, 45_296_000_789L))
                .isEqualTo(new Value.TimeVal(LocalTime.of(12, 34, 56, 789_000)));
        assertThat(bytesOf(fromPartition(BINARY, bytes(1, 2)))).containsExactly(1, 2);
        assertThat(bytesOf(fromPartition(FIXED4, new byte[] {1, 2, 3, 4}))).containsExactly(1, 2, 3, 4);
        assertThat(fromPartition(new IcebergType.DateType(), LocalDate.ofEpochDay(19000)))
                .isEqualTo(new Value.DateVal(LocalDate.ofEpochDay(19000)));
        assertThat(fromPartition(new IcebergType.DateType(), 19000))
                .isEqualTo(new Value.DateVal(LocalDate.ofEpochDay(19000)));
        assertThat(fromPartition(new IcebergType.StringType(), "a")).isEqualTo(new Value.StringVal("a"));
        assertThat(fromPartition(new IcebergType.LongType(), 7L)).isEqualTo(new Value.LongVal(7L));
    }

    @Test
    void partitionRescalesADecimalToTheTypeScaleExactly() {
        BigDecimal beyondTheScale = new BigDecimal("1.234");
        assertThat(fromPartition(DEC, new BigDecimal("1.2"))).isEqualTo(new Value.DecimalVal(new BigDecimal("1.20")));
        assertThatThrownBy(() -> fromPartition(DEC, beyondTheScale)).isInstanceOf(IcebergFormatException.class);
    }

    @Test
    void partitionRejectsAnUnconvertibleShapeNamingTheColumn() {
        assertThatThrownBy(() -> IcebergScalarValues.fromPartition(BINARY, "x", "blob"))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("blob");
        assertThatThrownBy(() -> IcebergScalarValues.fromPartition(GEOMETRY, "x", "geom"))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("geom");
    }

    @Test
    void nullArchetypesSelectTheVectorKind() {
        // The archetype spells out the table precision at the table scale, and its null column's leaf declares both.
        assertThat(IcebergScalarValues.nullValue(DEC, "f")).isInstanceOfSatisfying(Value.DecimalVal.class, dec -> {
            assertThat(dec.value().precision()).isEqualTo(9);
            assertThat(dec.value().scale()).isEqualTo(2);
        });
        assertThat(IcebergScalarValues.nullValue(TIME, "f")).isEqualTo(new Value.TimeVal(LocalTime.MIDNIGHT));
        assertThat(IcebergScalarValues.nullValue(BINARY, "f")).isInstanceOf(Value.BinaryVal.class);
        assertThat(IcebergScalarValues.nullValue(FIXED4, "f")).isInstanceOf(Value.BinaryVal.class);
        assertThat(IcebergScalarValues.nullValue(TS, "f")).isEqualTo(new Value.LongVal(0L));
        assertThat(IcebergScalarValues.nullValue(UUID_TYPE, "f")).isEqualTo(new Value.UuidVal(new UUID(0L, 0L)));
        assertThat(IcebergScalarValues.nullValue(UNKNOWN, "f")).isEqualTo(new Value.IntVal(0));
        assertThatThrownBy(() -> IcebergScalarValues.nullValue(GEOMETRY, "geom"))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("geom");
    }

    @Test
    void cellDecodesUuidAndInt32DecimalFromTheFixtureDeleteFiles() {
        Path root = TestCorpus.extractDirectory("iceberg-scalar-types/scalars", tempDir);
        assertThat(firstCell(root.resolve("data/p0/eq-delete-u.parquet"), UUID_TYPE, "u"))
                .isEqualTo(new Value.UuidVal(new UUID(2L, 2L * 1_000_003L)));
        // Iceberg wrote decimal(9, 2) as INT32; the cell is the unscaled integer.
        assertThat(firstCell(root.resolve("data/p1/eq-delete-dec.parquet"), DEC, "dec"))
                .isEqualTo(new Value.DecimalVal(new BigDecimal("1.25")));
    }

    @Test
    void cellDecodesFixedLengthAndInt64Decimals() {
        Path root = TestCorpus.extractDirectory("iceberg-scalar-types/scalars", tempDir);
        IcebergType bigDecimal = new IcebergType.DecimalType(20, 3);
        assertThat(firstCell(root.resolve("data/p0/data.parquet"), bigDecimal, "bigdec"))
                .isEqualTo(new Value.DecimalVal(new BigDecimal("123456789012345.678")));

        Path int64Decimal = TestCorpus.extractFile("parquet-testing/data/int64_decimal.parquet", tempDir);
        Value decoded = firstCell(int64Decimal, new IcebergType.DecimalType(10, 2), "value");
        Value expected = withFirstRow(
                int64Decimal, row -> new Value.DecimalVal(BigDecimal.valueOf(row.getLong(ColumnPath.of("value")), 2)));
        assertThat(decoded).isEqualTo(expected);
    }

    private static Value firstCell(Path file, IcebergType type, String column) {
        return withFirstRow(file, row -> IcebergScalarValues.fromCell(type, row, ColumnPath.of(column)));
    }

    private static Value withFirstRow(Path file, Function<ParquetRecord, Value> decode) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetSource parquet = ParquetSource.open(source);
            try (Stream<ParquetRecord> rows =
                    parquet.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return rows.findFirst().map(decode).orElseThrow();
            }
        }
    }

    private static Value fromJson(IcebergType type, String json) {
        JsonNode node = JsonMapper.shared().readTree(json);
        return IcebergScalarValues.fromJson(type, node, "f");
    }

    private static Value fromPartition(IcebergType type, Object raw) {
        return IcebergScalarValues.fromPartition(type, raw, "c");
    }

    private static MemorySegment le(long value) {
        return MemorySegment.ofArray(
                ByteBuffer.allocate(8).order(LITTLE_ENDIAN).putLong(value).array());
    }

    private static MemorySegment leInt(int value) {
        return MemorySegment.ofArray(
                ByteBuffer.allocate(4).order(LITTLE_ENDIAN).putInt(value).array());
    }

    private static MemorySegment bytes(int... unsigned) {
        byte[] raw = new byte[unsigned.length];
        for (int i = 0; i < unsigned.length; i++) {
            raw[i] = (byte) unsigned[i];
        }
        return MemorySegment.ofArray(raw);
    }

    /** Binary values compare by bytes: a segment's own equality is identity of the backing region. */
    private static int[] bytesOf(Value value) {
        byte[] raw = ((Value.BinaryVal) value).value().toArray(ValueLayout.JAVA_BYTE);
        int[] unsigned = new int[raw.length];
        for (int i = 0; i < raw.length; i++) {
            unsigned[i] = raw[i] & 0xff;
        }
        return unsigned;
    }
}
