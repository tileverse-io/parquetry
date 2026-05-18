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
package io.tileverse.parquetry.page.plain;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.page.PageDecoder;

class PlainBinaryDecoderTest {

    @Test
    void decodesVariableLengthEntries() {
        byte[] empty = new byte[0];
        byte[] hi = "hi".getBytes(StandardCharsets.UTF_8);
        byte[] world = "world!".getBytes(StandardCharsets.UTF_8);

        // Total: 3 entries * 4-byte prefix + 0+2+6 payload = 12 + 8 = 20 bytes
        int totalSize = 3 * 4 + empty.length + hi.length + world.length;
        ByteBuffer page = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
        page.putInt(empty.length);
        page.put(empty);
        page.putInt(hi.length);
        page.put(hi);
        page.putInt(world.length);
        page.put(world);
        page.flip();

        PageDecoder<ByteBuffer> decoder = new PlainBinaryDecoder();
        decoder.load(page, 3);

        ByteBuffer resultEmpty = decoder.next();
        assertThat(resultEmpty.remaining()).isZero();

        ByteBuffer resultHi = decoder.next();
        assertThat(resultHi.remaining()).isEqualTo(2);
        assertThat(resultHi).isEqualTo(ByteBuffer.wrap(hi));

        ByteBuffer resultWorld = decoder.next();
        assertThat(resultWorld.remaining()).isEqualTo(6);
        assertThat(resultWorld).isEqualTo(ByteBuffer.wrap(world));
    }

    @Test
    void slicesAreReadOnly() {
        ByteBuffer page = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        page.putInt(0); // empty entry
        page.flip();

        PageDecoder<ByteBuffer> decoder = new PlainBinaryDecoder();
        decoder.load(page, 1);

        ByteBuffer slice = decoder.next();
        assertThat(slice.isReadOnly()).isTrue();
    }

    @Test
    void skipJumpsOverLengthPrefixedEntries() {
        byte[] skipped1 = "skip1".getBytes(StandardCharsets.UTF_8);
        byte[] skipped2 = "skip2!".getBytes(StandardCharsets.UTF_8);
        byte[] kept = "kept".getBytes(StandardCharsets.UTF_8);

        int totalSize = 3 * 4 + skipped1.length + skipped2.length + kept.length;
        ByteBuffer page = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
        page.putInt(skipped1.length);
        page.put(skipped1);
        page.putInt(skipped2.length);
        page.put(skipped2);
        page.putInt(kept.length);
        page.put(kept);
        page.flip();

        PageDecoder<ByteBuffer> decoder = new PlainBinaryDecoder();
        decoder.load(page, 3);
        decoder.skip(2);

        ByteBuffer result = decoder.next();
        assertThat(result).isEqualTo(ByteBuffer.wrap(kept));
    }
}
