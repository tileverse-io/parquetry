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
package io.tileverse.parquetry.data.variant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.ParquetFormatException;

class VariantTest {

    private static Variant load(String name) {
        VariantMetadata metadata = new VariantMetadata(
                MemorySegment.ofArray(VariantCorpus.metadata(name)).asReadOnly());
        return Variant.of(MemorySegment.ofArray(VariantCorpus.value(name)).asReadOnly(), metadata);
    }

    @Nested
    class Scalars {

        @Test
        void decodesScalars() {
            assertThat(load("primitive_int32").type()).as("int32 type").isEqualTo(Variant.Type.INT32);
            assertThat(load("primitive_int32").getInt()).as("int32 value").isEqualTo(123456);
            assertThat(load("primitive_boolean_true").getBoolean())
                    .as("bool true")
                    .isTrue();
            assertThat(load("primitive_boolean_false").getBoolean())
                    .as("bool false")
                    .isFalse();
            assertThat(load("primitive_string").getString())
                    .as("string non-empty")
                    .isNotEmpty();
            assertThat(load("short_string").type()).as("short string type").isEqualTo(Variant.Type.STRING);
            assertThat(load("primitive_decimal4").getBigDecimal())
                    .as("decimal4")
                    .isEqualByComparingTo(new BigDecimal("12.34"));
            assertThat(load("primitive_double").type()).as("double type").isEqualTo(Variant.Type.DOUBLE);
            assertThat(load("primitive_int64").type()).as("int64 type").isEqualTo(Variant.Type.INT64);
        }

        @Test
        void decodesEachScalarAccessor() {
            assertThat(load("primitive_int8").getByte()).as("int8 value").isEqualTo((byte) 42);
            assertThat(load("primitive_int16").getShort()).as("int16 value").isEqualTo((short) 1234);
            assertThat(load("primitive_int64").getLong()).as("int64 value").isEqualTo(1234567890123456789L);
            assertThat(load("primitive_string").getString())
                    .as("long string content")
                    .startsWith("This string is longer");
            assertThat(load("short_string").getString())
                    .as("short string content")
                    .startsWith("Less than 64 bytes");
            assertThat(load("primitive_double").getDouble()).as("double value").isEqualTo(1234567890.1234);
            assertThat(load("primitive_float").getFloat()).as("float value").isEqualTo(1234567940.0f);
            assertThat(load("primitive_null").type()).as("null type").isEqualTo(Variant.Type.NULL);

            MemorySegment binary = load("primitive_binary").getBinary();
            assertThat(binary.byteSize()).as("binary length").isEqualTo(9L);
            assertThat(binary.get(ValueLayout.JAVA_BYTE, 0))
                    .as("binary first byte")
                    .isEqualTo((byte) 0x03);
        }

        @Test
        void decodesTemporalAccessors() {
            assertThat(load("primitive_date").type()).as("date type").isEqualTo(Variant.Type.DATE);
            assertThat(load("primitive_date").getDateDays()).as("date days").isEqualTo(20194);
            assertThat(load("primitive_time").type()).as("time type").isEqualTo(Variant.Type.TIME_NTZ);
            assertThat(load("primitive_time").getTimeMicros()).as("time micros").isEqualTo(45234123456L);
            assertThat(load("primitive_timestamp").type()).as("timestamp type").isEqualTo(Variant.Type.TIMESTAMP_TZ);
            assertThat(load("primitive_timestamp").getTimestampMicros())
                    .as("timestamp micros")
                    .isEqualTo(1744821296780000L);
            assertThat(load("primitive_timestamp_nanos").type())
                    .as("timestamp nanos type")
                    .isEqualTo(Variant.Type.TIMESTAMP_NANOS_TZ);
            assertThat(load("primitive_timestamp_nanos").getTimestampNanos())
                    .as("timestamp nanos")
                    .isEqualTo(1730982834123456789L);
        }

        @Test
        void unknownPrimitiveIdThrows() {
            byte[] bad = {(byte) 0xFC};
            Variant variant = Variant.of(MemorySegment.ofArray(bad).asReadOnly(), null);
            assertThatThrownBy(variant::type)
                    .as("unknown primitive id rejected")
                    .isInstanceOf(ParquetFormatException.class);
        }
    }

    @Nested
    class Navigation {

        @Test
        void navigatesPrimitiveArray() {
            Variant array = load("array_primitive");
            assertThat(array.type()).as("array type").isEqualTo(Variant.Type.ARRAY);
            assertThat(array.numElements()).as("4 elements").isEqualTo(4);
            assertThat(array.getElement(0).getByte()).as("element 0").isEqualTo((byte) 2);
            assertThat(array.getElement(3).getByte()).as("element 3").isEqualTo((byte) 9);
        }

        @Test
        void navigatesObjectFields() {
            Variant object = load("object_primitive");
            assertThat(object.type()).as("object type").isEqualTo(Variant.Type.OBJECT);
            assertThat(object.numFields()).as("7 fields").isEqualTo(7);
            assertThat(object.getField("int_field").getByte()).as("int_field").isEqualTo((byte) 1);
            assertThat(object.getField("boolean_true_field").getBoolean())
                    .as("bool field")
                    .isTrue();
            assertThat(object.getField("string_field").getString())
                    .as("string field")
                    .isEqualTo("Apache Parquet");
            assertThat(object.getField("missing")).as("absent field").isNull();
        }

        @Test
        void navigatesNestedObject() {
            Variant object = load("object_nested");
            Variant observation = object.getField("observation");
            assertThat(observation.type()).as("nested object").isEqualTo(Variant.Type.OBJECT);
            assertThat(observation.getField("value").getField("temperature").getByte())
                    .as("deeply nested scalar")
                    .isEqualTo((byte) 123);
        }

        @Test
        void emptyContainersHaveNoElements() {
            assertThat(load("array_empty").numElements()).as("empty array").isZero();
            assertThat(load("object_empty").numFields()).as("empty object").isZero();
        }

        @Test
        void listsFieldNamesAndReadsByIndex() {
            Variant object = load("object_nested");
            assertThat(object.fieldNames()).as("declared object field names").contains("id", "observation", "species");
            assertThat(object.getField("species").getField("name").getString())
                    .as("nested species name")
                    .isEqualTo("lava monster");
            Variant firstField = object.getFieldAtIndex(0);
            assertThat(firstField.type())
                    .as("first field by index resolves to a value")
                    .isNotNull();
        }

        @Test
        void navigatesNestedArrayWithNullElement() {
            Variant array = load("array_nested");
            assertThat(array.type()).as("nested array type").isEqualTo(Variant.Type.ARRAY);
            assertThat(array.numElements()).as("3 elements").isEqualTo(3);
            assertThat(array.getElement(1).type()).as("null element").isEqualTo(Variant.Type.NULL);
        }
    }

    @Nested
    class Serialize {

        @Test
        void serializeIsMetadataFollowedByValue() {
            VariantEncoder.Encoded encoded = new VariantEncoder()
                    .startObject()
                    .field("n")
                    .addInt(42)
                    .endObject()
                    .encode();
            VariantMetadata metadata = new VariantMetadata(encoded.metadata());
            Variant variant = Variant.of(encoded.value(), metadata);

            byte[] metadataBytes = encoded.metadata().toArray(ValueLayout.JAVA_BYTE);
            byte[] valueBytes = encoded.value().toArray(ValueLayout.JAVA_BYTE);
            byte[] expected = new byte[metadataBytes.length + valueBytes.length];
            System.arraycopy(metadataBytes, 0, expected, 0, metadataBytes.length);
            System.arraycopy(valueBytes, 0, expected, metadataBytes.length, valueBytes.length);

            assertThat(variant.serialize()).as("metadata ++ value").isEqualTo(expected);
        }
    }
}
