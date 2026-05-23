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
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Channel-write helpers shared by every {@link Encoder} that emits its page bytes into a {@link WritableByteChannel}.
 *
 * <p>A single {@link WritableByteChannel#write(ByteBuffer)} call may report a partial write count. The encoders never
 * tolerate a short write, so each one loops until the buffer is drained.
 */
final class ChannelWrites {

    private ChannelWrites() {}

    /** Drains {@code buf} into {@code dst}, looping until {@code buf} has no remaining bytes. */
    static void writeFully(WritableByteChannel dst, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            dst.write(buf);
        }
    }
}
