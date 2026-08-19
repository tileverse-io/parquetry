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
package io.tileverse.parquetry.internal.write.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BinaryPayloadTest {

    @ParameterizedTest
    @MethodSource("oracles")
    void packedImplAgreesWithOracle(byte[][] oracle) {
        assertMatchesOracle(packedOf(oracle), oracle);
    }

    @ParameterizedTest
    @MethodSource("oracles")
    void arrayImplAgreesWithOracle(byte[][] oracle) {
        assertMatchesOracle(arrayOf(oracle), oracle);
    }

    static Stream<Arguments> oracles() {
        return Stream.of(
                arguments(named(
                        "values of differing lengths including an empty value",
                        new byte[][] {{1, 2, 3}, {}, {4}, {5, 6}, {7, 8, 9, 10}})),
                arguments(named("a single value", new byte[][] {{42}})),
                arguments(named("a single empty value", new byte[][] {{}})));
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

    /**
     * Lays the oracle values end-to-end in one backing array with parallel offset/length arrays sized larger than the
     * value count; the extra slots hold poison values to prove {@code count} is honored, not the array lengths.
     */
    private static PackedBinaryPayload packedOf(byte[][] oracle) {
        int total = 0;
        for (byte[] value : oracle) {
            total += value.length;
        }
        byte[] backing = new byte[total];
        int slack = 3;
        int[] offsets = new int[oracle.length + slack];
        int[] lengths = new int[oracle.length + slack];
        Arrays.fill(offsets, Integer.MIN_VALUE);
        Arrays.fill(lengths, Integer.MIN_VALUE);
        int pos = 0;
        for (int i = 0; i < oracle.length; i++) {
            System.arraycopy(oracle[i], 0, backing, pos, oracle[i].length);
            offsets[i] = pos;
            lengths[i] = oracle[i].length;
            pos += oracle[i].length;
        }
        return new PackedBinaryPayload(backing, offsets, lengths, oracle.length);
    }

    /**
     * Wraps the oracle in a {@code byte[][]} sized larger than the value count; the extra slots hold poison arrays to
     * prove {@code count} is honored, not the array length.
     */
    private static ArrayBinaryPayload arrayOf(byte[][] oracle) {
        int slack = 3;
        byte[][] values = Arrays.copyOf(oracle, oracle.length + slack);
        for (int i = oracle.length; i < values.length; i++) {
            values[i] = new byte[] {(byte) 0xEE};
        }
        return new ArrayBinaryPayload(values, oracle.length);
    }
}
