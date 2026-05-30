/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.parquetry.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

class IpcFramingTest {

    @Test
    void framesMessageWithContinuationLengthAndEightBytePadding() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] metadata = new byte[] {1, 2, 3}; // 3 bytes -> padded to 8

        IpcFraming.writeMessage(out, ByteBuffer.wrap(metadata), new byte[0]);

        ByteBuffer buf = ByteBuffer.wrap(out.toByteArray()).order(ByteOrder.LITTLE_ENDIAN);
        assertThat(buf.getInt()).isEqualTo(0xFFFFFFFF); // continuation
        int metaLen = buf.getInt();
        assertThat(metaLen).isEqualTo(8); // 3 padded up to a multiple of 8
        assertThat((8 + metaLen) % 8).isZero();
        byte[] meta = new byte[metaLen];
        buf.get(meta);
        assertThat(meta).startsWith(new byte[] {1, 2, 3});
        assertThat(buf.hasRemaining()).isFalse();
    }

    @Test
    void endOfStreamIsContinuationThenZeroLength() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IpcFraming.writeEndOfStream(out);
        ByteBuffer buf = ByteBuffer.wrap(out.toByteArray()).order(ByteOrder.LITTLE_ENDIAN);
        assertThat(buf.getInt()).isEqualTo(0xFFFFFFFF);
        assertThat(buf.getInt()).isZero();
        assertThat(buf.hasRemaining()).isFalse();
    }
}
