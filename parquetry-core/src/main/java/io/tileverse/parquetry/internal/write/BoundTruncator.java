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

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.Optional;

/**
 * Cuts a long bound of a column ordered by unsigned bytes to a few bytes. A lower bound is cut to a prefix of the
 * value, and a prefix orders before the value. An upper bound is the same prefix with its last character incremented,
 * and it orders after the value. parquet-java and arrow-rs cut the bounds of a column index the same way.
 *
 * <p>A prefix of UTF-8 text ends between two characters, and its upper bound stays text and within the limit. Bytes
 * that are no text are cut as bytes, and their upper bound increments the last byte below {@code 0xFF}. When no
 * character or byte of the prefix can be incremented, no short upper bound exists, and none is given. The whole value
 * would keep the column index as long as the value.
 */
final class BoundTruncator {

    private BoundTruncator() {}

    /**
     * A lower bound of {@code value} that takes at most {@code maxBytes}. A value that is not longer is its own bound.
     */
    static MemorySegment lowerBound(MemorySegment value, int maxBytes) {
        if (value.byteSize() <= maxBytes) {
            return value;
        }
        return readOnly(prefix(value, maxBytes));
    }

    /**
     * An upper bound of {@code value} that takes at most {@code maxBytes}. A value that is not longer is its own bound.
     * A value without such a bound gets {@link MemorySegment#NULL}.
     */
    static MemorySegment upperBound(MemorySegment value, int maxBytes) {
        if (value.byteSize() <= maxBytes) {
            return value;
        }
        byte[] prefix = prefix(value, maxBytes);
        Optional<String> text = decodedUtf8(ByteBuffer.wrap(prefix));
        Optional<byte[]> incremented =
                text.isPresent() ? incrementLastCharacter(text.orElseThrow(), maxBytes) : incrementLastByte(prefix);
        return incremented.map(BoundTruncator::readOnly).orElse(MemorySegment.NULL);
    }

    /**
     * The first {@code maxBytes} bytes of {@code value}. When the bytes before a split UTF-8 character are text, the
     * prefix ends before that character.
     *
     * <p>The prefix is a copy. A column index keeps the bounds of its pages until its row group is written, and a view
     * would keep the whole value reachable with them.
     */
    private static byte[] prefix(MemorySegment value, int maxBytes) {
        MemorySegment text = value.asSlice(0, characterBoundary(value, maxBytes));
        boolean isText = decodedUtf8(text.asByteBuffer()).isPresent();
        MemorySegment prefix = isText ? text : value.asSlice(0, maxBytes);
        return prefix.toArray(JAVA_BYTE);
    }

    /**
     * Where a cut at {@code limit} lands when moved back to the start of a UTF-8 character. The byte at {@code limit}
     * is the first one left out. While it continues a character, the cut moves back, at most three bytes, because a
     * character takes at most four.
     */
    private static int characterBoundary(MemorySegment value, int limit) {
        int lowest = Math.max(0, limit - 3);
        int end = limit;
        while (end > lowest && isContinuationByte(value.get(JAVA_BYTE, end))) {
            end--;
        }
        return end;
    }

    private static boolean isContinuationByte(byte b) {
        return (b & 0xC0) == 0x80;
    }

    private static Optional<String> decodedUtf8(ByteBuffer bytes) {
        try {
            return Optional.of(UTF_8.newDecoder().decode(bytes).toString());
        } catch (CharacterCodingException _) {
            return Optional.empty();
        }
    }

    /**
     * The UTF-8 bytes of {@code text} with its last character replaced by the next character, and the characters after
     * it dropped. A character is passed over for the one before it when it has no next character, as U+10FFFF, or when
     * the next character would end past {@code maxBytes}, as U+0080 after U+007F at the limit.
     */
    private static Optional<byte[]> incrementLastCharacter(String text, int maxBytes) {
        int end = text.length();
        while (end > 0) {
            int codePoint = text.codePointBefore(end);
            int start = end - Character.charCount(codePoint);
            if (codePoint < Character.MAX_CODE_POINT) {
                String incremented = text.substring(0, start) + Character.toString(nextCharacter(codePoint));
                byte[] bytes = incremented.getBytes(UTF_8);
                if (bytes.length <= maxBytes) {
                    return Optional.of(bytes);
                }
            }
            end = start;
        }
        return Optional.empty();
    }

    /** The character after {@code codePoint}. The surrogates between U+D7FF and U+E000 are no characters. */
    private static int nextCharacter(int codePoint) {
        int next = codePoint + 1;
        if (next == Character.MIN_SURROGATE) {
            return Character.MAX_SURROGATE + 1;
        }
        return next;
    }

    /** The bytes up to the last one below {@code 0xFF}, with that byte incremented. */
    private static Optional<byte[]> incrementLastByte(byte[] bytes) {
        for (int i = bytes.length - 1; i >= 0; i--) {
            if (bytes[i] != (byte) 0xFF) {
                byte[] incremented = Arrays.copyOf(bytes, i + 1);
                incremented[i]++;
                return Optional.of(incremented);
            }
        }
        return Optional.empty();
    }

    private static MemorySegment readOnly(byte[] bytes) {
        return MemorySegment.ofArray(bytes).asReadOnly();
    }
}
