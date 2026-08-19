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
package io.tileverse.parquetry.internal.write.page;

/**
 * Write-only byte sink for a data page's encoded value bytes.
 *
 * <p>Multi-byte primitives are written little-endian, matching Parquet's PLAIN byte order
 */
public interface LittleEndianSink {

    /** Appends one byte (the low 8 bits of {@code b}). */
    void writeByte(int b);

    /** Appends {@code v} as four little-endian bytes. */
    void writeInt(int v);

    /** Appends {@code v} as eight little-endian bytes. */
    void writeLong(long v);

    /** Appends {@code v} as its four raw IEEE-754 bits, little-endian. */
    void writeFloat(float v);

    /** Appends {@code v} as its eight raw IEEE-754 bits, little-endian. */
    void writeDouble(double v);

    /** Appends {@code len} bytes from {@code src} starting at {@code off}. */
    void write(byte[] src, int off, int len);

    /** Appends all of {@code src}. */
    default void write(byte[] src) {
        write(src, 0, src.length);
    }

    /** The number of bytes appended since construction or the last reset. */
    int size();
}
