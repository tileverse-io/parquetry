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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.junit.jupiter.api.Test;

class ChannelWritesTest {

    @Test
    void drainsBufferInOneShotWhenDelegateAcceptsAll() throws IOException {
        ByteBuffer src = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5});
        FixedWindowChannel channel = new FixedWindowChannel(Integer.MAX_VALUE);

        ChannelWrites.writeFully(channel, src);

        assertThat(src.hasRemaining()).isFalse();
        assertThat(channel.callCount).isEqualTo(1);
        assertThat(channel.received.toByteArray()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    void loopsThroughMultiplePartialWrites() throws IOException {
        ByteBuffer src = ByteBuffer.wrap(new byte[] {10, 20, 30, 40, 50, 60, 70});
        FixedWindowChannel channel = new FixedWindowChannel(3);

        ChannelWrites.writeFully(channel, src);

        assertThat(src.hasRemaining()).isFalse();
        assertThat(channel.callCount).isEqualTo(3);
        assertThat(channel.received.toByteArray()).containsExactly(10, 20, 30, 40, 50, 60, 70);
    }

    @Test
    void noOpOnEmptyBuffer() throws IOException {
        ByteBuffer src = ByteBuffer.allocate(0);
        FixedWindowChannel channel = new FixedWindowChannel(8);

        ChannelWrites.writeFully(channel, src);

        assertThat(channel.callCount).isZero();
    }

    /** Channel whose {@code write} accepts at most {@code maxPerCall} bytes per invocation. */
    private static final class FixedWindowChannel implements WritableByteChannel {

        private final int maxPerCall;
        private final java.io.ByteArrayOutputStream received = new java.io.ByteArrayOutputStream();
        private int callCount;
        private boolean open = true;

        FixedWindowChannel(int maxPerCall) {
            this.maxPerCall = maxPerCall;
        }

        @Override
        public int write(ByteBuffer src) {
            callCount++;
            int take = Math.min(maxPerCall, src.remaining());
            byte[] slab = new byte[take];
            src.get(slab);
            received.writeBytes(slab);
            return take;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}
