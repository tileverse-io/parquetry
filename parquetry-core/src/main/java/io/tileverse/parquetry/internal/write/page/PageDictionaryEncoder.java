/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import io.tileverse.parquetry.format.Encoding;

/**
 * Page-flush contract of a dictionary attempt encoder. Implementations accumulate a column chunk's unique values while
 * the dictionary's serialized payload stays under a byte budget, emit {@link Encoding#RLE_DICTIONARY} pages of
 * dictionary indices until the budget overflows, and emit {@link Encoding#PLAIN} pages afterwards. The column-chunk
 * writer drives one instance per dictionary-active column and closes each page with
 * {@link #flushPage(WritableByteChannel)}.
 */
public interface PageDictionaryEncoder {

    /**
     * Close the current page: write its encoded body to {@code dst} and return the page's encoding marker. The next
     * append starts a fresh page on the same column chunk.
     */
    PageResult flushPage(WritableByteChannel dst) throws IOException;

    /** True if this column chunk has overflowed the byte budget and falls back to PLAIN pages. */
    boolean overflowed();

    /**
     * True once at least one page has been flushed as {@link Encoding#RLE_DICTIONARY}. Those pages index into the chunk
     * dictionary; the dictionary page must therefore be written even when a later page overflows to PLAIN.
     */
    boolean emittedDictionaryPage();

    /** Outcome of {@link #flushPage(WritableByteChannel)}: page-header encoding markers, value count, and byte size. */
    record PageResult(Encoding v2Encoding, Encoding v1Encoding, int valueCount, int bytesWritten) {}
}
