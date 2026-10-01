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
package io.tileverse.parquetry.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;

/**
 * The default {@link ByteRangeSource#readFully(java.util.List)}: exactly the requested bytes, in file order, with
 * ranges that touch byte for byte read as one, and a result that accounts for the reads issued by it.
 */
class ByteRangeSourceBatchReadTest {

    private static final int FILE_LENGTH = 64;

    /**
     * The three request lengths, distinct on purpose: a count or a run of bytes of one width can only have come from
     * the request of that width. Equal widths would let a wrong request order pass the assertions.
     */
    private static final int FIRST_LENGTH = 4;

    private static final int SECOND_LENGTH = 8;

    private static final int THIRD_LENGTH = 12;

    /** The buffer holds the three ranges back to back, in request order. */
    private static final int BUFFER_LENGTH = FIRST_LENGTH + SECOND_LENGTH + THIRD_LENGTH;

    /** The touching ranges cover [0,12) of the file; no request names a byte between there and 40. */
    private static final int HOLE_START = FIRST_LENGTH + SECOND_LENGTH;

    private static final int HOLE_END = 40;

    /** Where the third range lands in the buffer, right behind the two touching ranges. */
    private static final int THIRD_TARGET_START = FIRST_LENGTH + SECOND_LENGTH;

    private RecordingByteRangeSource openRecording(Path dir) throws IOException {
        Path file = dir.resolve("data.bin");
        Files.write(file, fileBytes(0, FILE_LENGTH));
        return new RecordingByteRangeSource(ByteRangeSource.ofFile(file));
    }

    /** What the file holds over {@code length} bytes at {@code offset}: every byte is its own offset. */
    private static byte[] fileBytes(int offset, int length) {
        byte[] content = new byte[length];
        for (int i = 0; i < length; i++) {
            content[i] = (byte) (offset + i);
        }
        return content;
    }

    /**
     * Three requests of three widths into one buffer: file ranges [0,4) and [4,12) touch, and the third reads [40,52)
     * into the buffer's tail.
     */
    private static List<RangeRequest> threeRequestsInto(ByteBuffer buffer) {
        List<RangeRequest> requests = new ArrayList<>();
        requests.add(RangeRequest.of(0, FIRST_LENGTH, window(buffer, 0, FIRST_LENGTH)));
        requests.add(RangeRequest.of(FIRST_LENGTH, SECOND_LENGTH, window(buffer, FIRST_LENGTH, SECOND_LENGTH)));
        requests.add(RangeRequest.of(HOLE_END, THIRD_LENGTH, window(buffer, THIRD_TARGET_START, THIRD_LENGTH)));
        return requests;
    }

    /** The two reads issued by the default for {@link #threeRequestsInto}, in file order. */
    private static List<RecordingByteRangeSource.Range> theTwoReadsInFileOrder() {
        return List.of(
                new RecordingByteRangeSource.Range(0, HOLE_START),
                new RecordingByteRangeSource.Range(HOLE_END, THIRD_LENGTH));
    }

    /** The buffer as a filled batch leaves it: the three ranges in request order, whatever order they were read in. */
    private static byte[] theThreeRangesBackToBack() {
        ByteBuffer expected = ByteBuffer.allocate(BUFFER_LENGTH);
        expected.put(fileBytes(0, FIRST_LENGTH));
        expected.put(fileBytes(FIRST_LENGTH, SECOND_LENGTH));
        expected.put(fileBytes(HOLE_END, THIRD_LENGTH));
        return expected.array();
    }

    /**
     * A window on {@code buffer} at {@code position}, {@code length} bytes wide. A duplicate rather than a slice: the
     * joined read of two touching ranges widens the first window's limit, and a slice has no capacity past its own end.
     */
    private static ByteBuffer window(ByteBuffer buffer, int position, int length) {
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.limit(position + length);
        duplicate.position(position);
        return duplicate;
    }

    @Test
    void touchingRangesAreReadAsOne(@TempDir Path dir) throws IOException {
        try (RecordingByteRangeSource recording = openRecording(dir)) {
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_LENGTH);
            recording.readFully(threeRequestsInto(buffer));

            assertThat(recording.ranges()).containsExactlyElementsOf(theTwoReadsInFileOrder());
        }
    }

    @Test
    void noByteOfAHoleIsRead(@TempDir Path dir) throws IOException {
        try (RecordingByteRangeSource recording = openRecording(dir)) {
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_LENGTH);
            recording.readFully(threeRequestsInto(buffer));

            assertThat(recording.bytesInRange(HOLE_START, HOLE_END)).isZero();
            assertThat(recording.bytesRead()).isEqualTo(BUFFER_LENGTH);
        }
    }

    @Test
    void everyTargetHoldsItsOwnRange(@TempDir Path dir) throws IOException {
        try (RecordingByteRangeSource recording = openRecording(dir)) {
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_LENGTH);
            recording.readFully(threeRequestsInto(buffer));

            assertThat(buffer.array()).isEqualTo(theThreeRangesBackToBack());
        }
    }

    @Test
    void theResultAccountsForEveryRequestAndEveryReadIssued(@TempDir Path dir) throws IOException {
        try (RecordingByteRangeSource recording = openRecording(dir)) {
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_LENGTH);

            BatchReadResult result = recording.readFully(threeRequestsInto(buffer));

            assertThat(result.requests()).isEqualTo(3);
            assertThat(result.bytesRead(0)).isEqualTo(FIRST_LENGTH);
            assertThat(result.bytesRead(1)).isEqualTo(SECOND_LENGTH);
            assertThat(result.bytesRead(2)).isEqualTo(THIRD_LENGTH);
            assertThat(result.bytesRequested()).isEqualTo(BUFFER_LENGTH);
            assertThat(result.fetches())
                    .as("one read per run of touching ranges")
                    .isEqualTo(2);
            assertThat(result.bytesTransferred()).isEqualTo(BUFFER_LENGTH);
            assertThat(result.bytesFromCache()).isZero();
        }
    }

    @Test
    void requestOrderIsTheCallersToChoose(@TempDir Path dir) throws IOException {
        try (RecordingByteRangeSource recording = openRecording(dir)) {
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_LENGTH);
            List<RangeRequest> reversed = new ArrayList<>(threeRequestsInto(buffer));
            Collections.reverse(reversed);

            BatchReadResult result = recording.readFully(reversed);

            assertThat(recording.ranges())
                    .as("the reads go out in file order whatever order the requests arrived in")
                    .containsExactlyElementsOf(theTwoReadsInFileOrder());
            assertThat(result.bytesRead(0))
                    .as("the first count belongs to the first request, the widest one here")
                    .isEqualTo(THIRD_LENGTH);
            assertThat(result.bytesRead(1)).isEqualTo(SECOND_LENGTH);
            assertThat(result.bytesRead(2))
                    .as("the last count belongs to the last request, the narrowest one here")
                    .isEqualTo(FIRST_LENGTH);
            assertThat(buffer.array())
                    .as("every target still holds its own range, wherever its request sat in the list")
                    .isEqualTo(theThreeRangesBackToBack());
        }
    }

    @Test
    void eachTargetPositionAdvancesByItsRangeLength(@TempDir Path dir) throws IOException {
        try (RecordingByteRangeSource recording = openRecording(dir)) {
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_LENGTH);
            List<RangeRequest> requests = threeRequestsInto(buffer);

            recording.readFully(requests);

            assertThat(requests.get(0).target().position()).isEqualTo(FIRST_LENGTH);
            assertThat(requests.get(1).target().position()).isEqualTo(FIRST_LENGTH + SECOND_LENGTH);
            assertThat(requests.get(2).target().position()).isEqualTo(BUFFER_LENGTH);
        }
    }

    @Test
    void aZeroLengthRequestIssuesNoRead(@TempDir Path dir) throws IOException {
        try (RecordingByteRangeSource recording = openRecording(dir)) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            List<RangeRequest> requests =
                    List.of(RangeRequest.of(0, 8, buffer), RangeRequest.of(32, 0, ByteBuffer.allocate(0)));

            BatchReadResult result = recording.readFully(requests);

            assertThat(recording.requestCount()).isEqualTo(1);
            assertThat(result.bytesRead(1)).isZero();
            assertThat(result.fetches()).isEqualTo(1);
        }
    }

    @Test
    void aRangePastEndOfSourceFailsTheCall(@TempDir Path dir) throws IOException {
        try (RecordingByteRangeSource recording = openRecording(dir)) {
            ByteBuffer buffer = ByteBuffer.allocate(16);
            List<RangeRequest> requests = List.of(RangeRequest.of(FILE_LENGTH - 4, 16, window(buffer, 0, 16)));

            assertThatThrownBy(() -> recording.readFully(requests))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasCauseInstanceOf(EOFException.class)
                    .hasMessageContaining("offset " + FILE_LENGTH)
                    .hasMessageContaining("12 bytes still requested");
        }
    }

    @Test
    void aMalformedBatchFailsBeforeAnyRead(@TempDir Path dir) throws IOException {
        try (RecordingByteRangeSource recording = openRecording(dir)) {
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_LENGTH);
            List<RangeRequest> requests = threeRequestsInto(buffer);
            ByteBuffer drained = requests.get(1).target();
            drained.position(drained.limit());

            assertThatThrownBy(() -> recording.readFully(requests)).isInstanceOf(IllegalArgumentException.class);
            assertThat(recording.requestCount()).isZero();
        }
    }

    @Test
    void anEmptyBatchReadsNothing(@TempDir Path dir) throws IOException {
        try (RecordingByteRangeSource recording = openRecording(dir)) {
            assertThat(recording.readFully(List.of())).isEqualTo(BatchReadResult.EMPTY);
            assertThat(recording.requestCount()).isZero();
        }
    }
}
