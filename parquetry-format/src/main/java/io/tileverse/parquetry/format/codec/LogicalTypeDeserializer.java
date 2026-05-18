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

import java.io.IOException;

import io.tileverse.parquetry.format.logical.LogicalType;

/**
 * Deserializer for the Thrift {@code LogicalType} union.
 *
 * <p>Each field ID in the union corresponds to one variant. Variants with no payload are empty
 * Thrift structs (STRUCT type, zero fields before STOP). Variants with payload are decoded into
 * the corresponding record.
 *
 * <pre>
 * union LogicalType {
 *   1: StringType STRING
 *   2: MapType MAP
 *   3: ListType LIST
 *   4: EnumType ENUM
 *   5: DecimalType DECIMAL   {scale:i32, precision:i32}
 *   6: DateType DATE
 *   7: TimeType TIME         {isAdjustedToUTC:bool, unit:TimeUnit}
 *   8: TimestampType TIMESTAMP {isAdjustedToUTC:bool, unit:TimeUnit}
 *   10: IntType INTEGER      {bitWidth:i8, isSigned:bool}
 *   11: NullType UNKNOWN
 *   12: JsonType JSON
 *   13: BsonType BSON
 *   14: UUIDType UUID
 *   15: Float16Type FLOAT16
 *   16: VariantType VARIANT
 *   17: GeometryType GEOMETRY
 *   18: GeographyType GEOGRAPHY
 * }
 * </pre>
 */
final class LogicalTypeDeserializer {

    private LogicalTypeDeserializer() {}

    static LogicalType read(CompactProtocolReader r) throws IOException {
        // A union has exactly one field set. Read the single field header.
        FieldHeader fh = r.readFieldHeader(0);
        if (fh.isStop()) {
            // Empty union - should not happen but tolerate it
            return new LogicalType.UnknownType();
        }
        LogicalType result = decodeVariant(fh.fieldId(), r);
        // Consume the trailing STOP of the union struct
        FieldHeader stop = r.readFieldHeader(fh.fieldId());
        if (!stop.isStop()) {
            // Forward-compat: skip any extra fields a newer writer may have added
            r.skipField(stop.type());
            while (true) {
                FieldHeader extra = r.readFieldHeader(stop.fieldId());
                if (extra.isStop()) break;
                r.skipField(extra.type());
            }
        }
        return result;
    }

    private static LogicalType decodeVariant(int fieldId, CompactProtocolReader r) throws IOException {
        return switch (fieldId) {
            case 1 -> {
                skipEmptyStruct(r);
                yield new LogicalType.StringType();
            }
            case 2 -> {
                skipEmptyStruct(r);
                yield new LogicalType.MapType();
            }
            case 3 -> {
                skipEmptyStruct(r);
                yield new LogicalType.ListType();
            }
            case 4 -> {
                skipEmptyStruct(r);
                yield new LogicalType.EnumType();
            }
            case 5 -> readDecimal(r);
            case 6 -> {
                skipEmptyStruct(r);
                yield new LogicalType.DateType();
            }
            case 7 -> readTime(r);
            case 8 -> readTimestamp(r);
            case 10 -> readIntType(r);
            case 11 -> {
                skipEmptyStruct(r);
                yield new LogicalType.UnknownType();
            }
            case 12 -> {
                skipEmptyStruct(r);
                yield new LogicalType.JsonType();
            }
            case 13 -> {
                skipEmptyStruct(r);
                yield new LogicalType.BsonType();
            }
            case 14 -> {
                skipEmptyStruct(r);
                yield new LogicalType.UuidType();
            }
            case 15 -> {
                skipEmptyStruct(r);
                yield new LogicalType.Float16Type();
            }
            case 16 -> {
                r.skipStruct();
                yield new LogicalType.VariantStub();
            }
            case 17 -> {
                r.skipStruct();
                yield new LogicalType.GeometryStub();
            }
            case 18 -> {
                r.skipStruct();
                yield new LogicalType.GeographyStub();
            }
            default -> {
                r.skipStruct();
                yield new LogicalType.UnknownType();
            }
        };
    }

    /**
     * Reads and discards the body of an empty Thrift struct (just the STOP byte).
     * Used for logical type variants that carry no payload (StringType, MapType, etc.).
     */
    private static void skipEmptyStruct(CompactProtocolReader r) throws IOException {
        r.skipStruct();
    }

    /**
     * Reads {@code DecimalType}: scale (field 1, i32) and precision (field 2, i32).
     */
    private static LogicalType.Decimal readDecimal(CompactProtocolReader r) throws IOException {
        int scale = 0;
        int precision = 0;
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) break;
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> scale = r.readI32();
                case 2 -> precision = r.readI32();
                default -> r.skipField(fh.type());
            }
        }
        return new LogicalType.Decimal(scale, precision);
    }

    /**
     * Reads {@code TimeType}: isAdjustedToUTC (field 1, bool) and unit (field 2, TimeUnit union).
     */
    private static LogicalType.Time readTime(CompactProtocolReader r) throws IOException {
        boolean isAdjustedToUTC = false;
        LogicalType.TimeUnit unit = LogicalType.TimeUnit.MILLIS;
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) break;
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> isAdjustedToUTC = fh.type() == CompactType.BOOLEAN_TRUE;
                case 2 -> unit = readTimeUnit(r);
                default -> r.skipField(fh.type());
            }
        }
        return new LogicalType.Time(isAdjustedToUTC, unit);
    }

    /**
     * Reads {@code TimestampType}: isAdjustedToUTC (field 1, bool) and unit (field 2, TimeUnit).
     */
    private static LogicalType.Timestamp readTimestamp(CompactProtocolReader r) throws IOException {
        boolean isAdjustedToUTC = false;
        LogicalType.TimeUnit unit = LogicalType.TimeUnit.MILLIS;
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) break;
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> isAdjustedToUTC = fh.type() == CompactType.BOOLEAN_TRUE;
                case 2 -> unit = readTimeUnit(r);
                default -> r.skipField(fh.type());
            }
        }
        return new LogicalType.Timestamp(isAdjustedToUTC, unit);
    }

    /**
     * Reads {@code IntType}: bitWidth (field 1, i8) and isSigned (field 2, bool).
     */
    private static LogicalType.IntType readIntType(CompactProtocolReader r) throws IOException {
        byte bitWidth = 0;
        boolean isSigned = false;
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) break;
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> bitWidth = (byte) r.readI32();
                case 2 -> isSigned = fh.type() == CompactType.BOOLEAN_TRUE;
                default -> r.skipField(fh.type());
            }
        }
        return new LogicalType.IntType(bitWidth, isSigned);
    }

    /**
     * Reads the {@code TimeUnit} union: a struct with one of three empty-struct fields set.
     *
     * <pre>
     * union TimeUnit {
     *   1: MilliSeconds MILLIS
     *   2: MicroSeconds MICROS
     *   3: NanoSeconds NANOS
     * }
     * </pre>
     */
    private static LogicalType.TimeUnit readTimeUnit(CompactProtocolReader r) throws IOException {
        FieldHeader fh = r.readFieldHeader(0);
        if (fh.isStop()) {
            return LogicalType.TimeUnit.MILLIS;
        }
        LogicalType.TimeUnit unit =
                switch (fh.fieldId()) {
                    case 1 -> LogicalType.TimeUnit.MILLIS;
                    case 2 -> LogicalType.TimeUnit.MICROS;
                    case 3 -> LogicalType.TimeUnit.NANOS;
                    default -> LogicalType.TimeUnit.MILLIS;
                };
        // Skip the empty-struct payload of the chosen TimeUnit variant
        r.skipStruct();
        // Consume the STOP of the TimeUnit union struct
        FieldHeader stop = r.readFieldHeader(fh.fieldId());
        if (!stop.isStop()) {
            r.skipField(stop.type());
            while (true) {
                FieldHeader extra = r.readFieldHeader(stop.fieldId());
                if (extra.isStop()) break;
                r.skipField(extra.type());
            }
        }
        return unit;
    }
}
