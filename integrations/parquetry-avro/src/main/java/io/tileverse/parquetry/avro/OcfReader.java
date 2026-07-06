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
package io.tileverse.parquetry.avro;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;

/**
 * Reads the data blocks of an OCF file as a single-pass forward sequence of decompressed blocks, starting at the
 * header's first-block offset. Holds one block in memory at a time; the trailing sync marker after each block is
 * validated against the header's marker. One instance backs one record stream; the header itself is parsed once by
 * {@link OcfHeader#read(ByteRangeCursor)} and shared across streams.
 */
final class OcfReader {

    private static final int SYNC_SIZE = 16;

    private final ByteRangeCursor cursor;
    private final OcfHeader header;
    private final SegmentPool pool = SegmentPool.getDefault();

    private OcfReader(ByteRangeCursor cursor, OcfHeader header) {
        this.cursor = cursor;
        this.header = header;
    }

    /** A fresh block sequence over {@code source}, positioned at the first data block after {@code header}. */
    static OcfReader blocks(ByteRangeSource source, OcfHeader header) {
        ByteRangeCursor cursor = new ByteRangeCursor(source, header.firstBlockOffset());
        return new OcfReader(cursor, header);
    }

    boolean hasNextBlock() {
        return cursor.hasRemaining();
    }

    /**
     * A decompressed data block with its record count. Owns the pooled native buffer the compressed block was read
     * into; closing the block returns that buffer to the pool. For an uncompressed ({@code null}) codec {@link #data}
     * is a read-only view of that same buffer, valid only until {@link #close()}. Because each record copies its bytes
     * out during decode, the buffer is safe to return once the block's records have been read.
     */
    record Block(long recordCount, MemorySegment data, SegmentPool.Pooled blockBuffer) implements AutoCloseable {
        @Override
        public void close() {
            blockBuffer.close();
        }
    }

    Block nextBlock() {
        long recordCount = cursor.readLong();
        if (recordCount < 0) {
            throw new AvroFormatException("OCF block record count out of range: " + recordCount);
        }
        long blockSize = cursor.readLong();
        SegmentPool.Pooled blockBuffer = pool.borrow(requireValidBlockSize(blockSize));
        try {
            cursor.readInto(blockBuffer.segment());
            verifySync();
            MemorySegment data = header.codec().decompress(blockBuffer.segment().asReadOnly());
            return new Block(recordCount, data, blockBuffer);
        } catch (RuntimeException failure) {
            blockBuffer.close();
            throw failure;
        }
    }

    private int requireValidBlockSize(long blockSize) {
        if (blockSize < 0 || blockSize > Integer.MAX_VALUE) {
            throw new AvroFormatException("OCF block size out of range: " + blockSize);
        }
        return (int) blockSize;
    }

    private void verifySync() {
        byte[] sync = cursor.readSegment(SYNC_SIZE).toArray(ValueLayout.JAVA_BYTE);
        if (!Arrays.equals(sync, header.syncMarker())) {
            throw new AvroFormatException("Corrupt OCF: sync marker mismatch at offset " + cursor.position());
        }
    }
}
