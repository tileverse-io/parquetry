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

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

/** PLAIN decoder for DOUBLE: eight bytes, little-endian IEEE 754 per value. */
public final class PlainDoubleDecoder implements PageDecoder<Double> {

    private DoubleBuffer buffer;

    @Override
    public void load(ByteBuffer page, int valueCount) {
        this.buffer = page.order(LITTLE_ENDIAN).asDoubleBuffer();
    }

    @Override
    public Double next() {
        return buffer.get();
    }

    @Override
    public void decodeDoubles(int n, double[] dst, int offset) {
        buffer.get(dst, offset, n);
    }

    @Override
    public void skip(int n) {
        buffer.position(buffer.position() + n);
    }
}
