/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.internal.read.page;

import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.format.MalformedFileException;

/** Checks the value section of a page holding fixed-width values against the value count stated for it. */
public final class FixedWidthValues {

    private FixedWidthValues() {}

    /**
     * Fails when {@code valueBytes} is shorter than {@code valueCount} values of {@code bytesPerValue} bytes each. The
     * count comes from the page's levels or header and the bytes from its value section: a page short of bytes is
     * corrupt, and decoding on would read past the section.
     *
     * @param valueType the encoded type named in the error, such as {@code "INT32"}
     */
    public static void requireBytesFor(MemorySegment valueBytes, int valueCount, int bytesPerValue, String valueType) {
        long neededBytes = (long) valueCount * bytesPerValue;
        if (valueBytes.byteSize() < neededBytes) {
            throw new MalformedFileException(valueCount + " " + valueType + " values need " + neededBytes
                    + " bytes but the page holds " + valueBytes.byteSize());
        }
    }
}
