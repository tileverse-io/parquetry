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

import java.nio.ByteBuffer;

import io.tileverse.parquetry.page.PageDecoder;

/**
 * PLAIN decoder for BOOLEAN: bit-packed, LSB-first, eight values per byte.
 *
 * <p>Bit 0 of byte 0 is value 0, bit 1 of byte 0 is value 1, etc. The decoder maintains an internal bit cursor and does
 * not advance the underlying buffer's position on each call, keeping it simple and allocation-free.
 */
public final class PlainBooleanDecoder implements PageDecoder<Boolean> {

    private byte[] bytes;
    private int byteOffset;
    private int bitPosition;

    @Override
    public void load(ByteBuffer page, int valueCount) {
        // Copy remaining bytes out so we can index by absolute position.
        int remaining = page.remaining();
        bytes = new byte[remaining];
        page.get(bytes);
        byteOffset = 0;
        bitPosition = 0;
    }

    @Override
    public Boolean next() {
        int byteIndex = byteOffset + (bitPosition >>> 3);
        int bitInByte = bitPosition & 7;
        boolean value = (bytes[byteIndex] & (1 << bitInByte)) != 0;
        bitPosition++;
        return value;
    }

    @Override
    public void skip(int n) {
        bitPosition += n;
    }
}
