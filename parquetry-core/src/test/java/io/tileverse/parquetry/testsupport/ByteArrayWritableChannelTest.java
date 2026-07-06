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
package io.tileverse.parquetry.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

class ByteArrayWritableChannelTest {

    @Test
    void writesHeapBackedBuffer() {
        ByteArrayWritableChannel channel = new ByteArrayWritableChannel();

        int written = channel.write(ByteBuffer.wrap(new byte[] {1, 2, 3}));

        assertThat(written).isEqualTo(3);
        assertThat(channel.size()).isEqualTo(3);
        assertThat(channel.toByteArray()).containsExactly(1, 2, 3);
    }

    @Test
    void writesDirectBuffer() {
        ByteArrayWritableChannel channel = new ByteArrayWritableChannel();
        ByteBuffer direct = ByteBuffer.allocateDirect(4);
        direct.put(new byte[] {10, 20, 30, 40}).flip();

        int written = channel.write(direct);

        assertThat(written).isEqualTo(4);
        assertThat(channel.toByteArray()).containsExactly(10, 20, 30, 40);
        assertThat(direct.hasRemaining()).isFalse();
    }

    @Test
    void writesArrayBackedBufferWithOffset() {
        ByteArrayWritableChannel channel = new ByteArrayWritableChannel();
        byte[] backing = {0, 0, 7, 8, 9, 0};
        ByteBuffer slice = ByteBuffer.wrap(backing, 2, 3).slice();

        int written = channel.write(slice);

        assertThat(written).isEqualTo(3);
        assertThat(channel.toByteArray()).containsExactly(7, 8, 9);
    }

    @Test
    void appendsAcrossMultipleWrites() {
        ByteArrayWritableChannel channel = new ByteArrayWritableChannel();

        channel.write(ByteBuffer.wrap(new byte[] {1, 2}));
        channel.write(ByteBuffer.wrap(new byte[] {3}));
        channel.write(ByteBuffer.wrap(new byte[] {4, 5}));

        assertThat(channel.size()).isEqualTo(5);
        assertThat(channel.toByteArray()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    void closeFlipsOpenFlag() {
        ByteArrayWritableChannel channel = new ByteArrayWritableChannel();
        assertThat(channel.isOpen()).isTrue();

        channel.close();

        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void emptyChannelHasZeroSize() {
        ByteArrayWritableChannel channel = new ByteArrayWritableChannel();

        assertThat(channel.size()).isZero();
        assertThat(channel.toByteArray()).isEmpty();
    }
}
