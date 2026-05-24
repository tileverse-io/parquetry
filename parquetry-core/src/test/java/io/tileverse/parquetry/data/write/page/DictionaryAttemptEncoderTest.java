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
package io.tileverse.parquetry.data.write.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.IntBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.read.page.Dictionary;
import io.tileverse.parquetry.data.read.page.PlainInt32Decoder;
import io.tileverse.parquetry.data.read.page.RleDictionaryPageDecoder;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class DictionaryAttemptEncoderTest {

    @Test
    void firstPageStaysDictionaryEncodedWhenUnderBudget() throws Exception {
        DictionaryAttemptEncoder<Integer, int[]> encoder = newInt32Encoder(/* budgetBytes= */ 64);
        encoder.appendValue(10);
        encoder.appendValue(20);
        encoder.appendValue(10);
        encoder.appendValue(20);

        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        DictionaryAttemptEncoder.PageResult result = encoder.flushPage(out);

        assertThat(result.v2Encoding()).isEqualTo(Encoding.RLE_DICTIONARY);
        assertThat(result.v1Encoding()).isEqualTo(Encoding.PLAIN_DICTIONARY);
        assertThat(result.valueCount()).isEqualTo(4);

        Dictionary.IntDict dict = newIntDict(encoder.dictionaryValues());
        assertThat(decodeIndices(out.toByteArray(), 4, dict)).containsExactly(10, 20, 10, 20);
    }

    @Test
    void secondPageFallsBackToPlainOnceBudgetIsExceeded() throws Exception {
        // Budget = 8 bytes -> exactly two INT32 dictionary entries before overflow.
        DictionaryAttemptEncoder<Integer, int[]> encoder = newInt32Encoder(/* budgetBytes= */ 8);

        // Page 1: only two distinct values, fits in budget.
        encoder.appendValue(1);
        encoder.appendValue(2);
        encoder.appendValue(1);
        ByteArrayWritableChannel page1 = new ByteArrayWritableChannel();
        DictionaryAttemptEncoder.PageResult result1 = encoder.flushPage(page1);
        assertThat(result1.v2Encoding()).isEqualTo(Encoding.RLE_DICTIONARY);
        assertThat(result1.valueCount()).isEqualTo(3);
        Dictionary.IntDict dict = newIntDict(encoder.dictionaryValues());
        assertThat(decodeIndices(page1.toByteArray(), 3, dict)).containsExactly(1, 2, 1);

        // Page 2: a third distinct value triggers overflow; subsequent values flow into the PLAIN fallback page.
        encoder.appendValue(3);
        encoder.appendValue(4);
        encoder.appendValue(3);
        assertThat(encoder.overflowed()).isTrue();

        ByteArrayWritableChannel page2 = new ByteArrayWritableChannel();
        DictionaryAttemptEncoder.PageResult result2 = encoder.flushPage(page2);
        assertThat(result2.v2Encoding()).isEqualTo(Encoding.PLAIN);
        assertThat(result2.v1Encoding()).isEqualTo(Encoding.PLAIN);
        assertThat(result2.valueCount()).isEqualTo(3);

        PlainInt32Decoder plainDecoder = new PlainInt32Decoder();
        plainDecoder.load(MemorySegment.ofArray(page2.toByteArray()), 3);
        int[] decoded = new int[3];
        plainDecoder.decodeInts(3, decoded, 0);
        assertThat(decoded).containsExactly(3, 4, 3);
    }

    @Test
    void exactlyAtBudgetIsAccepted() throws Exception {
        // Budget = 8 bytes -> exactly two INT32 dictionary entries; the second entry brings size to 8 (== budget),
        // which is the inclusive upper edge: overflow should NOT trigger.
        DictionaryAttemptEncoder<Integer, int[]> encoder = newInt32Encoder(/* budgetBytes= */ 8);
        encoder.appendValue(10);
        encoder.appendValue(20);

        assertThat(encoder.overflowed()).isFalse();
        assertThat(encoder.dictionaryValues()).containsExactly(10, 20);

        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        DictionaryAttemptEncoder.PageResult result = encoder.flushPage(out);
        assertThat(result.v2Encoding()).isEqualTo(Encoding.RLE_DICTIONARY);
        assertThat(result.valueCount()).isEqualTo(2);
    }

    @Test
    void midPageOverflowReplaysIndicesAsRawValues() throws Exception {
        // Tight budget allows only one INT32 entry.
        DictionaryAttemptEncoder<Integer, int[]> encoder = newInt32Encoder(/* budgetBytes= */ 4);

        encoder.appendValue(10);
        encoder.appendValue(10);
        encoder.appendValue(20); // triggers overflow mid-page
        encoder.appendValue(20);
        encoder.appendValue(10);

        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        DictionaryAttemptEncoder.PageResult result = encoder.flushPage(out);
        assertThat(result.v2Encoding()).isEqualTo(Encoding.PLAIN);
        assertThat(result.valueCount()).isEqualTo(5);

        PlainInt32Decoder plainDecoder = new PlainInt32Decoder();
        plainDecoder.load(MemorySegment.ofArray(out.toByteArray()), 5);
        int[] decoded = new int[5];
        plainDecoder.decodeInts(5, decoded, 0);
        assertThat(decoded).containsExactly(10, 10, 20, 20, 10);
    }

    private static DictionaryAttemptEncoder<Integer, int[]> newInt32Encoder(long budgetBytes) {
        return new DictionaryAttemptEncoder<>(
                new PlainInt32Encoder(),
                values -> {
                    int[] carrier = new int[values.size()];
                    for (int i = 0; i < values.size(); i++) {
                        carrier[i] = values.get(i);
                    }
                    return carrier;
                },
                value -> Integer.BYTES,
                budgetBytes);
    }

    private static Dictionary.IntDict newIntDict(List<Integer> values) {
        int[] dictValues = new int[values.size()];
        for (int i = 0; i < values.size(); i++) {
            dictValues[i] = values.get(i);
        }
        return new Dictionary.IntDict(IntBuffer.wrap(dictValues));
    }

    private static int[] decodeIndices(byte[] pageBytes, int valueCount, Dictionary.IntDict dict) {
        RleDictionaryPageDecoder<Integer> decoder = new RleDictionaryPageDecoder<>(dict);
        decoder.load(MemorySegment.ofArray(pageBytes), valueCount);
        int[] decoded = new int[valueCount];
        for (int i = 0; i < valueCount; i++) {
            decoded[i] = decoder.next();
        }
        return decoded;
    }
}
