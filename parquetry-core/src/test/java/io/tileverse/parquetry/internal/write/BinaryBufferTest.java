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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.internal.write.page.BinaryPayload;
import io.tileverse.parquetry.internal.write.page.GrowableByteSink;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Exercises the packed-store behavior of {@code ValueBuffer.BinaryBuffer} through the {@link ValueBuffer} facade:
 * values fed as memory segments (the BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY path) and as byte arrays (the INT96 path)
 * round-trip through the {@link BinaryPayload} handed to the encoders, and a cleared buffer reuses its backing without
 * corrupting a page that was already encoded out of it.
 */
class BinaryBufferTest {

    @Test
    void segmentFedValuesRoundTripThroughThePayload() {
        byte[][] oracle = {{1, 2, 3}, {}, {4}, {5, 6}, {7, 8, 9, 10}};
        ValueBuffer buffer = ValueBuffer.forKind(PrimitiveKind.BYTE_ARRAY);
        addSegments(buffer, oracle);
        assertMatchesOracle((BinaryPayload) buffer.payloadValues(oracle.length), oracle);
    }

    @Test
    void aZeroLengthValueRoundTrips() {
        byte[][] oracle = {{}};
        ValueBuffer buffer = ValueBuffer.forKind(PrimitiveKind.BYTE_ARRAY);
        addSegments(buffer, oracle);
        assertMatchesOracle((BinaryPayload) buffer.payloadValues(oracle.length), oracle);
    }

    @Test
    void countIsHonoredNotTheLiveBackingSize() {
        byte[][] oracle = {{10, 20}, {30}, {40, 50, 60}};
        ValueBuffer buffer = ValueBuffer.forKind(PrimitiveKind.BYTE_ARRAY);
        addSegments(buffer, oracle);
        BinaryPayload payload = (BinaryPayload) buffer.payloadValues(2);
        assertThat(payload.count()).isEqualTo(2);
        assertMatchesOracle(payload, new byte[][] {{10, 20}, {30}});
    }

    @Test
    void byteArrayFedValuesRoundTripThroughThePayload() {
        byte[][] oracle = {packed(0), packed(1), packed(2)};
        ValueBuffer buffer = ValueBuffer.forKind(PrimitiveKind.INT96);
        for (byte[] value : oracle) {
            buffer.addBinary(value);
        }
        assertMatchesOracle((BinaryPayload) buffer.payloadValues(oracle.length), oracle);
    }

    @Test
    void clearReusesTheBackingWithoutCorruptingAnAlreadyEncodedPage() {
        ValueBuffer buffer = ValueBuffer.forKind(PrimitiveKind.BYTE_ARRAY);

        byte[][] firstPage = {{1, 1, 1}, {2, 2}, {3}};
        addSegments(buffer, firstPage);
        byte[] encodedFirstPage = encodeAll((BinaryPayload) buffer.payloadValues(firstPage.length));

        buffer.clear();

        byte[][] secondPage = {{9, 8, 7}, {6, 5}, {4}};
        addSegments(buffer, secondPage);
        byte[] encodedSecondPage = encodeAll((BinaryPayload) buffer.payloadValues(secondPage.length));

        assertThat(encodedFirstPage).isEqualTo(concat(firstPage));
        assertThat(encodedSecondPage).isEqualTo(concat(secondPage));
    }

    @Test
    void growthAcrossManySmallValuesReadsBackCorrectly() {
        int count = 5000;
        byte[][] oracle = new byte[count][];
        for (int i = 0; i < count; i++) {
            oracle[i] = new byte[] {(byte) i, (byte) (i >>> 8), (byte) (i >>> 16)};
        }
        ValueBuffer buffer = ValueBuffer.forKind(PrimitiveKind.BYTE_ARRAY);
        addSegments(buffer, oracle);
        assertMatchesOracle((BinaryPayload) buffer.payloadValues(oracle.length), oracle);
    }

    private static void addSegments(ValueBuffer buffer, byte[][] values) {
        for (byte[] value : values) {
            MemorySegment segment = MemorySegment.ofArray(value);
            buffer.addBinary(segment, 0, segment.byteSize());
        }
    }

    private static void assertMatchesOracle(BinaryPayload payload, byte[][] oracle) {
        assertThat(payload.count()).isEqualTo(oracle.length);
        for (int i = 0; i < oracle.length; i++) {
            assertThat(payload.length(i)).as("length(%d)", i).isEqualTo(oracle[i].length);
            assertThat(payload.valueAt(i)).as("valueAt(%d)", i).isEqualTo(oracle[i]);
            assertThat(writtenBytes(payload, i)).as("writeValueInto(%d)", i).isEqualTo(oracle[i]);
        }
    }

    private static byte[] writtenBytes(BinaryPayload payload, int i) {
        GrowableByteSink sink = new GrowableByteSink(1);
        payload.writeValueInto(i, sink);
        return sink.toByteArray();
    }

    private static byte[] encodeAll(BinaryPayload payload) {
        GrowableByteSink sink = new GrowableByteSink(1);
        for (int i = 0; i < payload.count(); i++) {
            payload.writeValueInto(i, sink);
        }
        return sink.toByteArray();
    }

    private static byte[] concat(byte[][] values) {
        int total = 0;
        for (byte[] value : values) {
            total += value.length;
        }
        byte[] joined = new byte[total];
        int pos = 0;
        for (byte[] value : values) {
            System.arraycopy(value, 0, joined, pos, value.length);
            pos += value.length;
        }
        return joined;
    }

    private static byte[] packed(int seed) {
        byte[] value = new byte[12];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) (seed + i);
        }
        return value;
    }
}
