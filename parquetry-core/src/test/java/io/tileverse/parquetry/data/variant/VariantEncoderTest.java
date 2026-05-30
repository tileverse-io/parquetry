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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class VariantEncoderTest {

    private static Variant roundTrip(VariantEncoder encoder) {
        VariantEncoder.Encoded encoded = encoder.encode();
        VariantMetadata metadata = new VariantMetadata(encoded.metadata());
        return Variant.of(encoded.value(), metadata);
    }

    @Test
    void roundTripsScalars() {
        assertThat(roundTrip(new VariantEncoder().addInt(123456)).getInt())
                .as("int")
                .isEqualTo(123456);
        assertThat(roundTrip(new VariantEncoder().addLong(9_000_000_000L)).getLong())
                .as("long")
                .isEqualTo(9_000_000_000L);
        assertThat(roundTrip(new VariantEncoder().addBoolean(true)).getBoolean())
                .as("bool")
                .isTrue();
        assertThat(roundTrip(new VariantEncoder().addString("hello variant")).getString())
                .as("string")
                .isEqualTo("hello variant");
        assertThat(roundTrip(new VariantEncoder().addDouble(1.25)).getDouble())
                .as("double")
                .isEqualTo(1.25);
    }

    @Test
    void roundTripsObjectAndArray() {
        VariantEncoder object = new VariantEncoder();
        object.startObject();
        object.field("a").addInt(1);
        object.field("b").addString("x");
        object.endObject();
        Variant decoded = roundTrip(object);
        assertThat(decoded.type()).as("object type").isEqualTo(Variant.Type.OBJECT);
        assertThat(decoded.getField("a").getInt()).as("object field a").isEqualTo(1);
        assertThat(decoded.getField("b").getString()).as("object field b").isEqualTo("x");

        VariantEncoder array = new VariantEncoder();
        array.startArray();
        array.addInt(10);
        array.addInt(20);
        array.endArray();
        Variant decodedArray = roundTrip(array);
        assertThat(decodedArray.numElements()).as("array size").isEqualTo(2);
        assertThat(decodedArray.getElement(1).getInt()).as("array element 1").isEqualTo(20);
    }

    @Test
    void roundTripsRemainingScalars() {
        assertThat(roundTrip(new VariantEncoder().addNull()).type()).as("null").isEqualTo(Variant.Type.NULL);
        assertThat(roundTrip(new VariantEncoder().addBoolean(false)).getBoolean())
                .as("bool false")
                .isFalse();
        assertThat(roundTrip(new VariantEncoder().addByte((byte) 7)).getByte())
                .as("byte")
                .isEqualTo((byte) 7);
        assertThat(roundTrip(new VariantEncoder().addShort((short) 1234)).getShort())
                .as("short")
                .isEqualTo((short) 1234);
        assertThat(roundTrip(new VariantEncoder().addFloat(2.5f)).getFloat())
                .as("float")
                .isEqualTo(2.5f);
        String longString = "a longer than sixty-three bytes string value goes here for the long string path!!";
        assertThat(roundTrip(new VariantEncoder().addString(longString)).getString())
                .as("long string")
                .isEqualTo(longString);
        UUID uuid = new UUID(0x0123456789abcdefL, 0xfedcba9876543210L);
        assertThat(roundTrip(new VariantEncoder().addUuid(uuid)).getUuid())
                .as("uuid")
                .isEqualTo(uuid);
    }

    @Test
    void roundTripsDecimalsAcrossWidthsAndSign() {
        assertDecimalRoundTrips(new BigDecimal("12.34")); // decimal4
        assertDecimalRoundTrips(new BigDecimal("-12.34"));
        assertDecimalRoundTrips(new BigDecimal("1234567890.1")); // decimal8
        assertDecimalRoundTrips(new BigDecimal("-1234567890.1"));
        assertDecimalRoundTrips(new BigDecimal("123456789012345678.90")); // decimal16
        assertDecimalRoundTrips(new BigDecimal("-123456789012345678.90"));
    }

    private static void assertDecimalRoundTrips(BigDecimal expected) {
        assertThat(roundTrip(new VariantEncoder().addBigDecimal(expected)).getBigDecimal())
                .as("decimal %s", expected)
                .isEqualByComparingTo(expected);
    }

    @Test
    void roundTripsBinaryAsBinaryType() {
        byte[] payload = {0x03, 0x13, 0x37, (byte) 0xde, (byte) 0xad};
        MemorySegment source = MemorySegment.ofArray(payload).asReadOnly();
        Variant decoded = roundTrip(new VariantEncoder().addBinary(source));
        assertThat(decoded.type()).as("binary type").isEqualTo(Variant.Type.BINARY);
        MemorySegment readBack = decoded.getBinary();
        assertThat(readBack.byteSize()).as("binary length").isEqualTo((long) payload.length);
        assertThat(readBack.toArray(ValueLayout.JAVA_BYTE)).as("binary content").isEqualTo(payload);
    }

    @Test
    void buildsSortedDictionaryAcrossNestedObjects() {
        VariantEncoder encoder = new VariantEncoder();
        encoder.startObject();
        encoder.field("zebra").addInt(1);
        encoder.field("alpha").startObject();
        encoder.field("mango").addInt(2);
        encoder.endObject();
        encoder.endObject();
        Variant decoded = roundTrip(encoder);
        assertThat(decoded.getField("zebra").getInt()).as("zebra").isEqualTo(1);
        assertThat(decoded.getField("alpha").getField("mango").getInt())
                .as("nested mango")
                .isEqualTo(2);
    }
}
