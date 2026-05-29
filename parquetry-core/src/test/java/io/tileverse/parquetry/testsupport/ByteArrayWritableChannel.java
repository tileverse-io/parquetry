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
package io.tileverse.parquetry.testsupport;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * In-memory {@link WritableByteChannel} for write-side tests. Captures bytes via a backing
 * {@link ByteArrayOutputStream}.
 */
public final class ByteArrayWritableChannel implements WritableByteChannel {

    private final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    private boolean open = true;

    @Override
    public int write(ByteBuffer src) {
        int remaining = src.remaining();
        if (src.hasArray()) {
            sink.write(src.array(), src.arrayOffset() + src.position(), remaining);
            src.position(src.position() + remaining);
        } else {
            byte[] tmp = new byte[remaining];
            src.get(tmp);
            sink.write(tmp, 0, remaining);
        }
        return remaining;
    }

    public byte[] toByteArray() {
        return sink.toByteArray();
    }

    public int size() {
        return sink.size();
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
