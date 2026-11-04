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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class BoundTruncatorTest {

    private static final int LIMIT = 64;

    private static final String LAST_CODE_POINT = Character.toString(Character.MAX_CODE_POINT);

    /** Two bytes in UTF-8. */
    private static final String E_ACUTE = Character.toString(0xE9);

    private static final String E_CIRCUMFLEX = Character.toString(0xEA);

    /** U+007F, the last character of one byte in UTF-8. */
    private static final String LAST_ONE_BYTE = Character.toString(0x7F);

    private static final String FIRST_TWO_BYTES = Character.toString(0x80);

    /** U+D7FF, the last character before the surrogates. */
    private static final String BEFORE_SURROGATES = Character.toString(0xD7FF);

    private static final String AFTER_SURROGATES = Character.toString(0xE000);

    /** Three bytes in UTF-8. */
    private static final String CJK_CHARACTER = Character.toString(0x65E5);

    /** Four bytes in UTF-8. */
    private static final String EMOJI = Character.toString(0x1F600);

    @Test
    void shortValueStaysWhole() {
        MemorySegment value = text("a".repeat(LIMIT));

        assertThat(BoundTruncator.lowerBound(value, LIMIT)).isSameAs(value);
        assertThat(BoundTruncator.upperBound(value, LIMIT)).isSameAs(value);
    }

    @Test
    void lowerBoundIsPrefix() {
        MemorySegment value = text("a".repeat(100));

        assertThat(utf8(BoundTruncator.lowerBound(value, LIMIT))).isEqualTo("a".repeat(LIMIT));
    }

    @Test
    void upperBoundIncrementsLastChar() {
        MemorySegment value = text("a".repeat(100));

        assertThat(utf8(BoundTruncator.upperBound(value, LIMIT))).isEqualTo("a".repeat(LIMIT - 1) + "b");
    }

    // A cut at 64 bytes would split the two bytes of the last character.
    @Test
    void cutFallsBetweenCharacters() {
        MemorySegment value = text("a".repeat(LIMIT - 1) + E_ACUTE + "z".repeat(10));

        assertThat(utf8(BoundTruncator.lowerBound(value, LIMIT))).isEqualTo("a".repeat(LIMIT - 1));
        assertThat(utf8(BoundTruncator.upperBound(value, LIMIT))).isEqualTo("a".repeat(LIMIT - 2) + "b");
    }

    @Test
    void incrementsMultibyteChar() {
        MemorySegment value = text("a".repeat(LIMIT - 2) + E_ACUTE + "z".repeat(10));

        assertThat(utf8(BoundTruncator.upperBound(value, LIMIT))).isEqualTo("a".repeat(LIMIT - 2) + E_CIRCUMFLEX);
    }

    // No character follows U+10FFFF, and the character before it is incremented.
    @Test
    void skipsLastCodePoint() {
        MemorySegment value = text("a".repeat(LIMIT - 4) + LAST_CODE_POINT + "z".repeat(10));

        assertThat(utf8(BoundTruncator.upperBound(value, LIMIT))).isEqualTo("a".repeat(LIMIT - 5) + "b");
    }

    // U+D800 to U+DFFF are surrogates, not characters; U+E000 follows U+D7FF.
    @Test
    void skipsSurrogateRange() {
        MemorySegment value = text(BEFORE_SURROGATES.repeat(30));

        assertThat(utf8(BoundTruncator.upperBound(value, LIMIT)))
                .isEqualTo(BEFORE_SURROGATES.repeat(20) + AFTER_SURROGATES);
    }

    // U+0080 after the last U+007F would end past the limit, and the U+007F before it is incremented.
    @Test
    void upperBoundFitsTheLimit() {
        MemorySegment value = text(LAST_ONE_BYTE.repeat(100));

        MemorySegment upper = BoundTruncator.upperBound(value, LIMIT);

        assertThat(utf8(upper)).isEqualTo(LAST_ONE_BYTE.repeat(LIMIT - 2) + FIRST_TWO_BYTES);
        assertThat(upper.byteSize()).isEqualTo(LIMIT);
    }

    // The byte 0xFF never occurs in UTF-8, and the value is cut as bytes.
    @Test
    void incrementsLastByteOfBinary() {
        byte[] value = new byte[100];
        Arrays.fill(value, (byte) 0xFF);
        value[0] = 0x01;

        assertThat(bytesOf(BoundTruncator.lowerBound(segment(value), LIMIT))).isEqualTo(Arrays.copyOf(value, LIMIT));
        assertThat(bytesOf(BoundTruncator.upperBound(segment(value), LIMIT))).isEqualTo(new byte[] {0x02});
    }

    // No value of 64 bytes or fewer orders after a value starting with 64 bytes of 0xFF.
    @Test
    void unincrementableHasNoBound() {
        byte[] bytes = new byte[100];
        Arrays.fill(bytes, (byte) 0xFF);
        MemorySegment value = segment(bytes);

        assertThat(BoundTruncator.lowerBound(value, LIMIT).byteSize()).isEqualTo(LIMIT);
        assertThat(BoundTruncator.upperBound(value, LIMIT)).isSameAs(MemorySegment.NULL);
    }

    @Test
    void lastCodePointsHaveNoBound() {
        MemorySegment value = text(LAST_CODE_POINT.repeat(30));

        assertThat(BoundTruncator.upperBound(value, LIMIT)).isSameAs(MemorySegment.NULL);
    }

    @ParameterizedTest
    @MethodSource("longValues")
    void boundsEncloseTheValue(MemorySegment value) {
        MemorySegment lower = BoundTruncator.lowerBound(value, LIMIT);
        MemorySegment upper = BoundTruncator.upperBound(value, LIMIT);

        assertThat(BoundsOrder.UNSIGNED_BYTES.compare(lower, value)).isNotPositive();
        assertThat(BoundsOrder.UNSIGNED_BYTES.compare(upper, value)).isPositive();
    }

    @ParameterizedTest
    @MethodSource("longValues")
    void boundsFitTheLimit(MemorySegment value) {
        assertThat(BoundTruncator.lowerBound(value, LIMIT).byteSize()).isLessThanOrEqualTo(LIMIT);
        assertThat(BoundTruncator.upperBound(value, LIMIT).byteSize()).isLessThanOrEqualTo(LIMIT);
    }

    static Stream<MemorySegment> longValues() {
        byte[] lowThenHigh = new byte[100];
        Arrays.fill(lowThenHigh, (byte) 0xFF);
        lowThenHigh[0] = 0x01;
        byte[] cutSequenceAfterText = Arrays.copyOf("a".repeat(70).getBytes(UTF_8), 71);
        cutSequenceAfterText[70] = (byte) 0xC3;
        return Stream.of(
                text("a".repeat(100)),
                text("a".repeat(LIMIT - 1) + E_ACUTE + "z".repeat(10)),
                text("a".repeat(LIMIT - 4) + LAST_CODE_POINT + "z".repeat(10)),
                text(LAST_ONE_BYTE.repeat(100)),
                text(BEFORE_SURROGATES.repeat(30)),
                text(CJK_CHARACTER.repeat(90)),
                text(EMOJI.repeat(30)),
                segment(lowThenHigh),
                segment(cutSequenceAfterText));
    }

    private static MemorySegment text(String value) {
        return segment(value.getBytes(UTF_8));
    }

    private static MemorySegment segment(byte[] bytes) {
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static byte[] bytesOf(MemorySegment segment) {
        return segment.toArray(JAVA_BYTE);
    }

    private static String utf8(MemorySegment segment) {
        return new String(bytesOf(segment), UTF_8);
    }
}
