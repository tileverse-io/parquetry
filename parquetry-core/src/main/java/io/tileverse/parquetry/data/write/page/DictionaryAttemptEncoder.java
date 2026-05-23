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

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.format.Encoding;

/**
 * Page-level dictionary attempt with PLAIN fallback. Tracks unique values in a column-chunk dictionary while the
 * dictionary's serialized payload would stay under a byte budget; falls back to writing PLAIN pages once the budget is
 * exceeded.
 *
 * <p>The dataset-writer driver feeds this encoder one column value at a time: append every column value via
 * {@link #appendValue(Object)}, then close each page with {@link #flushPage(WritableByteChannel)}. As long as the
 * dictionary fits inside {@code dictionaryByteLimit} the encoder emits {@link Encoding#RLE_DICTIONARY} pages of
 * dictionary indices; once it overflows, subsequent {@code appendValue} calls record the original value, and
 * {@link #flushPage} emits a {@link Encoding#PLAIN} page using {@code plainEncoder}.
 *
 * <p>One {@link DictionaryAttemptEncoder} instance is dedicated to a single primitive kind; the geometry-aware policy
 * lives outside this class.
 *
 * @param <V> the value type the dictionary keys against; for binary kinds use a stable wrapper (e.g.,
 *     {@link java.nio.ByteBuffer#wrap(byte[])} or a {@code byte[]}-keyed {@code IdentityHashMap}); for primitives use
 *     the boxed {@code Integer}/{@code Long}/{@code Float}/{@code Double}/{@code Boolean}.
 * @param <C> the carrier array type the fallback {@link Encoder} accepts (e.g., {@code int[]}, {@code byte[][]})
 */
public final class DictionaryAttemptEncoder<V, C> {

    private final Encoder<C> plainEncoder;
    private final CarrierFactory<V, C> carrierFactory;
    private final ValueSizer<V> valueSizer;
    private final long dictionaryByteLimit;

    private final Map<V, Integer> dictionaryIndex = new HashMap<>();
    private final List<V> dictionaryValues = new ArrayList<>();
    private long dictionaryBytes;
    private boolean overflowed;

    private final List<Integer> pageIndices = new ArrayList<>();
    private final List<V> pageFallbackValues = new ArrayList<>();

    public DictionaryAttemptEncoder(
            Encoder<C> plainEncoder,
            CarrierFactory<V, C> carrierFactory,
            ValueSizer<V> valueSizer,
            long dictionaryByteLimit) {
        this.plainEncoder = plainEncoder;
        this.carrierFactory = carrierFactory;
        this.valueSizer = valueSizer;
        this.dictionaryByteLimit = dictionaryByteLimit;
    }

    /** Append one value to the current page; updates the chunk dictionary or, if overflowed, records the raw value. */
    public void appendValue(V value) {
        if (overflowed) {
            pageFallbackValues.add(value);
            return;
        }
        Integer existing = dictionaryIndex.get(value);
        if (existing != null) {
            pageIndices.add(existing);
            return;
        }
        long candidateBytes = dictionaryBytes + valueSizer.sizeOf(value);
        if (candidateBytes > dictionaryByteLimit) {
            overflowed = true;
            // Replay the page's indices as raw values so the fallback page reflects the same row sequence.
            for (Integer idx : pageIndices) {
                pageFallbackValues.add(dictionaryValues.get(idx));
            }
            pageIndices.clear();
            pageFallbackValues.add(value);
            return;
        }
        int newIndex = dictionaryValues.size();
        dictionaryValues.add(value);
        dictionaryIndex.put(value, newIndex);
        dictionaryBytes = candidateBytes;
        pageIndices.add(newIndex);
    }

    /**
     * Close the current page: write its encoded body to {@code dst} and return the page's encoding marker. The next
     * appendValue call starts a fresh page on the same column chunk.
     */
    public PageResult flushPage(WritableByteChannel dst) throws IOException {
        if (overflowed) {
            return flushPlainFallbackPage(dst);
        }
        return flushDictionaryPage(dst);
    }

    /** Snapshot of the column-chunk dictionary as it stands now. */
    public List<V> dictionaryValues() {
        return List.copyOf(dictionaryValues);
    }

    /** True if this column chunk has overflowed the byte budget and falls back to PLAIN pages. */
    public boolean overflowed() {
        return overflowed;
    }

    private PageResult flushDictionaryPage(WritableByteChannel dst) throws IOException {
        int n = pageIndices.size();
        int[] indices = new int[n];
        for (int i = 0; i < n; i++) {
            indices[i] = pageIndices.get(i);
        }
        pageIndices.clear();
        RleDictionaryEncoder encoder = new RleDictionaryEncoder();
        int bytesWritten = encoder.encode(indices, n, dst);
        return new PageResult(Encoding.RLE_DICTIONARY, Encoding.PLAIN_DICTIONARY, n, bytesWritten);
    }

    private PageResult flushPlainFallbackPage(WritableByteChannel dst) throws IOException {
        int n = pageFallbackValues.size();
        C carrier = carrierFactory.from(pageFallbackValues);
        pageFallbackValues.clear();
        int bytesWritten = plainEncoder.encode(carrier, n, dst);
        return new PageResult(Encoding.PLAIN, Encoding.PLAIN, n, bytesWritten);
    }

    /** Builds the typed carrier the fallback {@link Encoder} accepts from the boxed values buffered for a page. */
    @FunctionalInterface
    public interface CarrierFactory<V, C> {
        C from(List<V> values);
    }

    /** Computes the serialized byte size of one value as it would land in the dictionary page. */
    @FunctionalInterface
    public interface ValueSizer<V> {
        long sizeOf(V value);
    }

    /** Outcome of {@link #flushPage(WritableByteChannel)}: page-header encoding markers, value count, and byte size. */
    public record PageResult(Encoding v2Encoding, Encoding v1Encoding, int valueCount, int bytesWritten) {}
}
