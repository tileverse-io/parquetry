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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.io.RecordingByteRangeSource;
import io.tileverse.parquetry.io.RecordingByteRangeSource.Range;

/**
 * The framing cursor fetches a read-ahead window per source request and serves the varints, sync markers and small
 * payloads of the container framing from it, going to the source only for bytes beyond the window.
 */
class ByteRangeCursorTest {

    private static final int WINDOW = 16;

    private final byte[] bytes = sequence(100);
    private final RecordingByteRangeSource source = new RecordingByteRangeSource(new InMemoryByteRangeSource(bytes));

    @Test
    void servesBytesWithinTheWindowFromOneFetch() {
        ByteRangeCursor cursor = new ByteRangeCursor(source, 0, WINDOW);
        for (int i = 0; i < WINDOW; i++) {
            assertThat(cursor.readRawByte()).isEqualTo(i);
        }
        assertThat(source.ranges()).containsExactly(new Range(0, WINDOW));

        assertThat(cursor.readRawByte()).isEqualTo(WINDOW);
        assertThat(source.ranges()).containsExactly(new Range(0, WINDOW), new Range(WINDOW, WINDOW));
    }

    @Test
    void windowFetchStopsAtTheEndOfTheSource() {
        ByteRangeCursor cursor = new ByteRangeCursor(source, 90, WINDOW);
        assertThat(cursor.readRawByte()).isEqualTo(90);
        assertThat(source.ranges()).containsExactly(new Range(90, 10));
    }

    @Test
    void readsPayloadsSpanningWindowsWithoutRefetchingBufferedBytes() {
        ByteRangeCursor cursor = new ByteRangeCursor(source, 0, WINDOW);
        cursor.readRawByte();
        MemorySegment payload = cursor.readSegment(20);
        assertThat(payload.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(Arrays.copyOfRange(bytes, 1, 21));
        assertThat(source.ranges()).containsExactly(new Range(0, WINDOW), new Range(WINDOW, WINDOW));
        assertThat(cursor.position()).isEqualTo(21);
    }

    @Test
    void readsPayloadsLargerThanTheWindowStraightFromTheSource() {
        ByteRangeCursor cursor = new ByteRangeCursor(source, 0, WINDOW);
        cursor.readRawByte();
        MemorySegment payload = cursor.readSegment(50);
        assertThat(payload.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(Arrays.copyOfRange(bytes, 1, 51));
        assertThat(source.ranges()).containsExactly(new Range(0, WINDOW), new Range(WINDOW, 35));
        assertThat(cursor.position()).isEqualTo(51);
    }

    @Test
    void skipWithinTheWindowKeepsItsBytes() {
        ByteRangeCursor cursor = new ByteRangeCursor(source, 0, WINDOW);
        cursor.readRawByte();
        cursor.skip(10);
        assertThat(cursor.readRawByte()).isEqualTo(11);
        assertThat(source.requestCount()).isEqualTo(1);
    }

    @Test
    void skipBeyondTheWindowFetchesAtTheNewPosition() {
        ByteRangeCursor cursor = new ByteRangeCursor(source, 0, WINDOW);
        cursor.readRawByte();
        cursor.skip(40);
        assertThat(cursor.readRawByte()).isEqualTo(41);
        assertThat(source.ranges()).containsExactly(new Range(0, WINDOW), new Range(41, WINDOW));
    }

    @Test
    void readingPastTheEndFails() {
        ByteRangeCursor atEnd = new ByteRangeCursor(source, 100, WINDOW);
        assertThat(atEnd.hasRemaining()).isFalse();
        assertThatThrownBy(atEnd::readRawByte)
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("end of file");

        ByteRangeCursor nearEnd = new ByteRangeCursor(source, 95, WINDOW);
        assertThatThrownBy(() -> nearEnd.readSegment(10))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(EOFException.class);
    }

    @Test
    void bufferedPrefixIsTheLeadingBytesFetchedSoFar() {
        ByteRangeCursor cursor = new ByteRangeCursor(source, 0, WINDOW);
        assertThat(cursor.bufferedPrefix().byteSize()).isZero();

        cursor.readRawByte();
        assertThat(cursor.bufferedPrefix().toArray(ValueLayout.JAVA_BYTE)).isEqualTo(Arrays.copyOf(bytes, WINDOW));

        cursor.skip(40);
        cursor.readRawByte();
        assertThat(cursor.bufferedPrefix().byteSize()).isZero();
    }

    @Test
    void cursorSeededWithAPrefixReadsItWithoutFetching() {
        MemorySegment prefix = MemorySegment.ofArray(Arrays.copyOf(bytes, 30)).asReadOnly();
        ByteRangeCursor cursor = ByteRangeCursor.withPrefix(source, 5, prefix);
        assertThat(cursor.readSegment(25).toArray(ValueLayout.JAVA_BYTE)).isEqualTo(Arrays.copyOfRange(bytes, 5, 30));
        assertThat(source.requestCount()).isZero();

        assertThat(cursor.readRawByte()).isEqualTo(30);
        assertThat(source.ranges()).containsExactly(new Range(30, 70));
    }

    private static byte[] sequence(int length) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (byte) i;
        }
        return out;
    }
}
