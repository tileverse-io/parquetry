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
package io.tileverse.parquetry.avro;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Locale;
import java.util.zip.CRC32;

import io.tileverse.parquetry.compression.Codec;

/**
 * The OCF block codecs this reader supports, mapped onto the {@code parquetry-compression} engines. All six codecs
 * named by the Avro spec are supported: {@code null} passes the block through; {@code deflate} is raw DEFLATE (RFC
 * 1951, no zlib/gzip wrapper); {@code snappy} is the raw snappy block format followed by a 4-byte big-endian CRC32 of
 * the uncompressed data; {@code zstandard} is zstd frames; {@code bzip2} is the bzip2 stream format; {@code xz} is the
 * xz container format. Because OCF blocks do not record the uncompressed size, decompression always discovers it from
 * the compressed stream. Each codec also compresses a block back into its OCF wire shape, the exact inverse of its
 * decompression: {@link #compress(MemorySegment)} reproduces the snappy CRC32 trailer and the raw DEFLATE framing.
 */
enum BlockCodec {
    NULL {
        @Override
        MemorySegment decompress(MemorySegment compressed) {
            return compressed;
        }

        @Override
        MemorySegment compress(MemorySegment uncompressed) {
            return uncompressed;
        }
    },
    DEFLATE {
        @Override
        MemorySegment decompress(MemorySegment compressed) {
            return discoverSize(Codec.deflate(), compressed, this);
        }

        @Override
        MemorySegment compress(MemorySegment uncompressed) {
            return compressWith(Codec.deflate(), uncompressed, this);
        }
    },
    SNAPPY {
        @Override
        MemorySegment decompress(MemorySegment compressed) {
            long size = compressed.byteSize();
            if (size < SNAPPY_CRC_BYTES) {
                throw new AvroFormatException("Snappy block too short to hold its CRC32: " + size + " bytes");
            }
            MemorySegment body = compressed.asSlice(0, size - SNAPPY_CRC_BYTES);
            int expectedCrc = compressed.get(INT_BIG_ENDIAN, size - SNAPPY_CRC_BYTES);
            MemorySegment data = discoverSize(Codec.snappy(), body, this);
            verifyCrc(data, expectedCrc);
            return data;
        }

        @Override
        MemorySegment compress(MemorySegment uncompressed) {
            MemorySegment body = compressWith(Codec.snappy(), uncompressed, this);
            return appendCrc32(body, uncompressed);
        }

        private void verifyCrc(MemorySegment data, int expectedCrc) {
            CRC32 crc = new CRC32();
            crc.update(data.asByteBuffer());
            int actualCrc = (int) crc.getValue();
            if (actualCrc != expectedCrc) {
                throw new AvroFormatException("Snappy block CRC32 mismatch: expected "
                        + Integer.toHexString(expectedCrc) + ", got " + Integer.toHexString(actualCrc));
            }
        }
    },
    ZSTANDARD {
        @Override
        MemorySegment decompress(MemorySegment compressed) {
            return discoverSize(Codec.zstd(), compressed, this);
        }

        @Override
        MemorySegment compress(MemorySegment uncompressed) {
            return compressWith(Codec.zstd(), uncompressed, this);
        }
    },
    BZIP2 {
        @Override
        MemorySegment decompress(MemorySegment compressed) {
            return discoverSize(Codec.bzip2(), compressed, this);
        }

        @Override
        MemorySegment compress(MemorySegment uncompressed) {
            return compressWith(Codec.bzip2(), uncompressed, this);
        }
    },
    XZ {
        @Override
        MemorySegment decompress(MemorySegment compressed) {
            return discoverSize(Codec.xz(), compressed, this);
        }

        @Override
        MemorySegment compress(MemorySegment uncompressed) {
            return compressWith(Codec.xz(), uncompressed, this);
        }
    };

    private static final int SNAPPY_CRC_BYTES = 4;
    private static final ValueLayout.OfInt INT_BIG_ENDIAN =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    abstract MemorySegment decompress(MemorySegment compressed);

    abstract MemorySegment compress(MemorySegment uncompressed);

    private static MemorySegment discoverSize(Codec codec, MemorySegment compressed, BlockCodec which) {
        try {
            return codec.decompress(compressed);
        } catch (IOException e) {
            throw new AvroFormatException("Truncated or corrupt " + which.codecName() + " block", e);
        }
    }

    private static MemorySegment compressWith(Codec codec, MemorySegment uncompressed, BlockCodec which) {
        try {
            long maxLength = codec.maxCompressedLength(uncompressed.byteSize());
            MemorySegment output = MemorySegment.ofArray(new byte[Math.toIntExact(maxLength)]);
            int written = codec.compress(uncompressed, output);
            return output.asSlice(0, written);
        } catch (IOException e) {
            throw new AvroFormatException("Failed to " + which.codecName() + "-compress an Avro block", e);
        }
    }

    private static MemorySegment appendCrc32(MemorySegment body, MemorySegment uncompressed) {
        CRC32 crc = new CRC32();
        crc.update(uncompressed.asByteBuffer());
        byte[] framed = new byte[Math.toIntExact(body.byteSize()) + SNAPPY_CRC_BYTES];
        MemorySegment.copy(body, ValueLayout.JAVA_BYTE, 0, framed, 0, Math.toIntExact(body.byteSize()));
        MemorySegment out = MemorySegment.ofArray(framed);
        out.set(INT_BIG_ENDIAN, body.byteSize(), (int) crc.getValue());
        return out;
    }

    private String codecName() {
        return name().toLowerCase(Locale.ROOT);
    }

    static BlockCodec fromName(String name) {
        if (name == null) {
            return NULL;
        }
        return switch (name) {
            case "null" -> NULL;
            case "deflate" -> DEFLATE;
            case "snappy" -> SNAPPY;
            case "zstandard" -> ZSTANDARD;
            case "bzip2" -> BZIP2;
            case "xz" -> XZ;
            default -> throw new AvroFormatException("Unsupported Avro codec: " + name);
        };
    }
}
