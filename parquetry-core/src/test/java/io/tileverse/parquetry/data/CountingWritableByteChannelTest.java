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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.ParquetWriter.CountingWritableByteChannel;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

@SuppressWarnings("resource")
class CountingWritableByteChannelTest {

    @Test
    void countsBytesAcrossMultipleWrites() throws IOException {
        ByteArrayWritableChannel backing = new ByteArrayWritableChannel();
        CountingWritableByteChannel counting = new CountingWritableByteChannel(backing);

        counting.write(ByteBuffer.wrap(new byte[] {1, 2, 3}));
        counting.write(ByteBuffer.wrap(new byte[] {4, 5}));

        assertThat(counting.bytesWritten()).isEqualTo(5L);
        assertThat(backing.toByteArray()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    void respectsPartialWriteFromDelegate() throws IOException {
        PartialWriteChannel partial = new PartialWriteChannel(2);
        CountingWritableByteChannel counting = new CountingWritableByteChannel(partial);

        ByteBuffer src = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
        int firstWrite = counting.write(src);

        assertThat(firstWrite).isEqualTo(2);
        assertThat(counting.bytesWritten()).isEqualTo(2L);
        assertThat(src.remaining()).isEqualTo(2);
    }

    @Test
    void initialStateIsOpenAndZeroBytes() {
        ByteArrayWritableChannel backing = new ByteArrayWritableChannel();
        CountingWritableByteChannel counting = new CountingWritableByteChannel(backing);

        assertThat(counting.bytesWritten()).isZero();
        assertThat(counting.isOpen()).isTrue();
    }

    @Test
    void closeFlipsOpenFlagWithoutClosingDelegate() {
        ByteArrayWritableChannel backing = new ByteArrayWritableChannel();
        CountingWritableByteChannel counting = new CountingWritableByteChannel(backing);

        counting.close();

        assertThat(counting.isOpen()).isFalse();
        assertThat(backing.isOpen()).isTrue();
    }

    @Test
    void isOpenReflectsDelegateClose() {
        ByteArrayWritableChannel backing = new ByteArrayWritableChannel();
        CountingWritableByteChannel counting = new CountingWritableByteChannel(backing);

        backing.close();

        assertThat(counting.isOpen()).isFalse();
    }

    /** Test double whose {@code write} only consumes a fixed prefix of the buffer per call. */
    private static final class PartialWriteChannel implements WritableByteChannel {

        private final int maxPerCall;
        private boolean open = true;

        PartialWriteChannel(int maxPerCall) {
            this.maxPerCall = maxPerCall;
        }

        @Override
        public int write(ByteBuffer src) {
            int take = Math.min(maxPerCall, src.remaining());
            src.position(src.position() + take);
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
