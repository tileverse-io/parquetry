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
package io.tileverse.parquetry.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class PlainBinaryDecoderTest {

    @Test
    void decodesVariableLengthEntries() {
        byte[] empty = new byte[0];
        byte[] hi = "hi".getBytes(StandardCharsets.UTF_8);
        byte[] world = "world!".getBytes(StandardCharsets.UTF_8);

        // Total: 3 entries * 4-byte prefix + 0+2+6 payload = 12 + 8 = 20 bytes
        int totalSize = 3 * 4 + empty.length + hi.length + world.length;
        ByteBuffer page = ByteBuffer.allocate(totalSize).order(LITTLE_ENDIAN);
        page.putInt(empty.length);
        page.put(empty);
        page.putInt(hi.length);
        page.put(hi);
        page.putInt(world.length);
        page.put(world);
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainBinaryDecoder();
        decoder.load(page, 3);

        MemorySegment resultEmpty = decoder.next();
        assertThat(resultEmpty.byteSize()).isZero();

        MemorySegment resultHi = decoder.next();
        assertThat(resultHi.byteSize()).isEqualTo(2);
        assertThat(resultHi.toArray(JAVA_BYTE)).isEqualTo(hi);

        MemorySegment resultWorld = decoder.next();
        assertThat(resultWorld.byteSize()).isEqualTo(6);
        assertThat(resultWorld.toArray(JAVA_BYTE)).isEqualTo(world);
    }

    @Test
    void slicesAreReadOnly() {
        ByteBuffer page = ByteBuffer.allocate(4).order(LITTLE_ENDIAN);
        page.putInt(0); // empty entry
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainBinaryDecoder();
        decoder.load(page, 1);

        MemorySegment slice = decoder.next();
        assertThat(slice.isReadOnly()).isTrue();
    }

    @Test
    void skipJumpsOverLengthPrefixedEntries() {
        byte[] skipped1 = "skip1".getBytes(StandardCharsets.UTF_8);
        byte[] skipped2 = "skip2!".getBytes(StandardCharsets.UTF_8);
        byte[] kept = "kept".getBytes(StandardCharsets.UTF_8);

        int totalSize = 3 * 4 + skipped1.length + skipped2.length + kept.length;
        ByteBuffer page = ByteBuffer.allocate(totalSize).order(LITTLE_ENDIAN);
        page.putInt(skipped1.length);
        page.put(skipped1);
        page.putInt(skipped2.length);
        page.put(skipped2);
        page.putInt(kept.length);
        page.put(kept);
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainBinaryDecoder();
        decoder.load(page, 3);
        decoder.skip(2);

        MemorySegment result = decoder.next();
        assertThat(result.toArray(JAVA_BYTE)).isEqualTo(kept);
    }
}
