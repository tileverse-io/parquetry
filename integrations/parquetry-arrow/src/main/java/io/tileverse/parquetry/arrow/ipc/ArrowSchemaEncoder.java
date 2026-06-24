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
package io.tileverse.parquetry.arrow.ipc;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.arrow.flatbuf.Binary;
import org.apache.arrow.flatbuf.Bool;
import org.apache.arrow.flatbuf.Date;
import org.apache.arrow.flatbuf.DateUnit;
import org.apache.arrow.flatbuf.Decimal;
import org.apache.arrow.flatbuf.Endianness;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.FixedSizeBinary;
import org.apache.arrow.flatbuf.FloatingPoint;
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.flatbuf.Struct_;
import org.apache.arrow.flatbuf.Time;
import org.apache.arrow.flatbuf.Timestamp;
import org.apache.arrow.flatbuf.Type;

import com.google.flatbuffers.FlatBufferBuilder;

import io.tileverse.parquetry.arrow.ipc.LogicalColumns.LogicalColumn;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;

/** Encodes a {@link ParquetSchema} as an Arrow IPC Schema message (a flatbuffer {@link Message}). */
final class ArrowSchemaEncoder {

    private ArrowSchemaEncoder() {}

    static ByteBuffer encode(ParquetSchema schema, GeoArrowFields geometry) {
        return encode(LogicalColumns.of(schema, geometry));
    }

    static ByteBuffer encode(List<LogicalColumn> columns) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int[] fieldOffsets = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            fieldOffsets[i] = encodeField(builder, columns.get(i).field());
        }
        int fieldsVector = Schema.createFieldsVector(builder, fieldOffsets);
        Schema.startSchema(builder);
        Schema.addEndianness(builder, Endianness.Little);
        Schema.addFields(builder, fieldsVector);
        int schemaOffset = Schema.endSchema(builder);

        Message.startMessage(builder);
        Message.addVersion(builder, MetadataVersion.V5);
        Message.addHeaderType(builder, MessageHeader.Schema);
        Message.addHeader(builder, schemaOffset);
        Message.addBodyLength(builder, 0L);
        int messageOffset = Message.endMessage(builder);
        builder.finish(messageOffset);
        return builder.dataBuffer();
    }

    /**
     * Emits one flatbuffer {@code Field} for {@code field}, recursing into its children for nested kinds. Child fields
     * are built before the parent table opens, as FlatBuffers requires nested objects to be finished first.
     */
    private static int encodeField(FlatBufferBuilder builder, ArrowField field) {
        int nameOffset = builder.createString(field.name());
        TypeOffset typeOffset = encodeType(builder, field);
        int[] childOffsets = new int[field.children().size()];
        for (int i = 0; i < field.children().size(); i++) {
            childOffsets[i] = encodeField(builder, field.children().get(i));
        }
        int childrenVector = Field.createChildrenVector(builder, childOffsets);
        int customMetadata = customMetadata(builder, field.extensionMetadata());

        Field.startField(builder);
        Field.addName(builder, nameOffset);
        Field.addNullable(builder, field.nullable());
        Field.addTypeType(builder, typeOffset.typeId());
        Field.addType(builder, typeOffset.offset());
        Field.addChildren(builder, childrenVector);
        if (customMetadata != 0) {
            Field.addCustomMetadata(builder, customMetadata);
        }
        return Field.endField(builder);
    }

    /**
     * Maps an {@link ArrowField} to its flatbuffer type table. A list is Arrow {@code List}, a struct and a Parquet
     * Variant are both {@code Struct_} (the Variant adds an extension tag on the field), and a map is {@code Map}. A
     * primitive delegates to the leaf type path.
     */
    private static TypeOffset encodeType(FlatBufferBuilder builder, ArrowField field) {
        return switch (field.kind()) {
            case PRIMITIVE -> encodeLeafType(builder, field.leaf());
            case LIST -> new TypeOffset(Type.List, listType(builder));
            case STRUCT, VARIANT -> new TypeOffset(Type.Struct_, structType(builder));
            case MAP -> new TypeOffset(Type.Map, mapType(builder));
        };
    }

    private static int listType(FlatBufferBuilder builder) {
        org.apache.arrow.flatbuf.List.startList(builder);
        return org.apache.arrow.flatbuf.List.endList(builder);
    }

    private static int structType(FlatBufferBuilder builder) {
        Struct_.startStruct_(builder);
        return Struct_.endStruct_(builder);
    }

    private static int mapType(FlatBufferBuilder builder) {
        org.apache.arrow.flatbuf.Map.startMap(builder);
        org.apache.arrow.flatbuf.Map.addKeysSorted(builder, false);
        return org.apache.arrow.flatbuf.Map.endMap(builder);
    }

    private static TypeOffset encodeLeafType(FlatBufferBuilder builder, ArrowFieldType type) {
        return switch (type.kind()) {
            case BOOL -> new TypeOffset(Type.Bool, boolType(builder));
            case INT -> new TypeOffset(Type.Int, intType(builder, type.bitWidth(), type.signed()));
            case FLOATING_POINT -> new TypeOffset(Type.FloatingPoint, floatType(builder, type.bitWidth()));
            case UTF8 -> new TypeOffset(Type.Utf8, utf8Type(builder));
            case BINARY -> new TypeOffset(Type.Binary, binaryType(builder));
            case FIXED_SIZE_BINARY ->
                new TypeOffset(Type.FixedSizeBinary, fixedSizeBinaryType(builder, type.byteWidth()));
            case DECIMAL ->
                new TypeOffset(Type.Decimal, decimalType(builder, type.precision(), type.scale(), type.bitWidth()));
            case DATE32 -> new TypeOffset(Type.Date, dateType(builder));
            case TIME -> new TypeOffset(Type.Time, timeType(builder, type));
            case TIMESTAMP -> new TypeOffset(Type.Timestamp, timestampType(builder, type));
        };
    }

    private static int boolType(FlatBufferBuilder builder) {
        Bool.startBool(builder);
        return Bool.endBool(builder);
    }

    private static int intType(FlatBufferBuilder builder, int bitWidth, boolean signed) {
        Int.startInt(builder);
        Int.addBitWidth(builder, bitWidth);
        Int.addIsSigned(builder, signed);
        return Int.endInt(builder);
    }

    private static int floatType(FlatBufferBuilder builder, int bitWidth) {
        FloatingPoint.startFloatingPoint(builder);
        FloatingPoint.addPrecision(builder, precision(bitWidth));
        return FloatingPoint.endFloatingPoint(builder);
    }

    private static short precision(int bitWidth) {
        return switch (bitWidth) {
            case 16 -> Precision.HALF;
            case 32 -> Precision.SINGLE;
            case 64 -> Precision.DOUBLE;
            default -> throw new ParquetSchemaException("unsupported floating point bit width " + bitWidth);
        };
    }

    private static int utf8Type(FlatBufferBuilder builder) {
        org.apache.arrow.flatbuf.Utf8.startUtf8(builder);
        return org.apache.arrow.flatbuf.Utf8.endUtf8(builder);
    }

    private static int binaryType(FlatBufferBuilder builder) {
        Binary.startBinary(builder);
        return Binary.endBinary(builder);
    }

    private static int fixedSizeBinaryType(FlatBufferBuilder builder, int byteWidth) {
        FixedSizeBinary.startFixedSizeBinary(builder);
        FixedSizeBinary.addByteWidth(builder, byteWidth);
        return FixedSizeBinary.endFixedSizeBinary(builder);
    }

    private static int decimalType(FlatBufferBuilder builder, int precision, int scale, int bitWidth) {
        Decimal.startDecimal(builder);
        Decimal.addPrecision(builder, precision);
        Decimal.addScale(builder, scale);
        Decimal.addBitWidth(builder, bitWidth);
        return Decimal.endDecimal(builder);
    }

    private static int dateType(FlatBufferBuilder builder) {
        Date.startDate(builder);
        Date.addUnit(builder, DateUnit.DAY);
        return Date.endDate(builder);
    }

    private static int timeType(FlatBufferBuilder builder, ArrowFieldType type) {
        Time.startTime(builder);
        Time.addBitWidth(builder, type.bitWidth());
        Time.addUnit(builder, (short) type.timeUnit().ordinal());
        return Time.endTime(builder);
    }

    private static int timestampType(FlatBufferBuilder builder, ArrowFieldType type) {
        int timezoneOffset = 0;
        if (type.utcAdjusted()) {
            timezoneOffset = builder.createString("UTC");
        }
        Timestamp.startTimestamp(builder);
        Timestamp.addUnit(builder, (short) type.timeUnit().ordinal());
        if (timezoneOffset != 0) {
            Timestamp.addTimezone(builder, timezoneOffset);
        }
        return Timestamp.endTimestamp(builder);
    }

    private static int customMetadata(FlatBufferBuilder builder, Map<String, String> metadata) {
        if (metadata.isEmpty()) {
            return 0;
        }
        int[] entries = new int[metadata.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            int key = builder.createString(entry.getKey());
            int value = builder.createString(entry.getValue());
            entries[i++] = KeyValue.createKeyValue(builder, key, value);
        }
        return Field.createCustomMetadataVector(builder, entries);
    }

    private record TypeOffset(byte typeId, int offset) {}
}
