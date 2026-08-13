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
package io.tileverse.parquetry.internal.read;

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.BitSet;
import java.util.List;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.BooleanVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.Int96Vector;
import io.tileverse.parquetry.columnar.IntSequence;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Merges the per-page part vectors of one page-spanning row into a single leaf vector.
 *
 * <p>A row whose values cross a data-page boundary is read as one {@link BatchColumnReader#readBatch} slice per page.
 * Each part is already self-contained (its bytes were copied out of the page at slice time), but the parts of one leaf
 * can differ in concrete representation - a dictionary-encoded page followed by a PLAIN fallback page yields a
 * dictionary-view part and a consolidated part. The merge therefore reads every part through the leaf-kind accessors
 * and materializes one standard vector: fixed-width kinds into a pooled little-endian segment (null slots zeroed, the
 * decode contract), BOOLEAN into a pooled LSB-first bitmap, and the binary kinds into a pooled backing buffer with
 * row-indexed offsets. All pooled buffers are acquired through the decode valve and registered on the owning batch,
 * which releases them on close.
 */
final class SpanningRowVectors {

    private static final int INT96_WIDTH = 12;

    private SpanningRowVectors() {}

    static ColumnVector merge(
            SchemaNode.Primitive leaf,
            List<ColumnVector> parts,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {
        int size = totalSize(parts);
        Validity validity = mergedValidity(parts, size);
        return switch (leaf.kind()) {
            case INT32 -> mergeInts(parts, size, validity, allocator, acquiredBuffers);
            case INT64 -> mergeLongs(parts, size, validity, allocator, acquiredBuffers);
            case FLOAT -> mergeFloats(parts, size, validity, allocator, acquiredBuffers);
            case DOUBLE -> mergeDoubles(parts, size, validity, allocator, acquiredBuffers);
            case BOOLEAN -> mergeBooleans(parts, size, validity, allocator, acquiredBuffers);
            case BYTE_ARRAY -> mergeVariableBinary(parts, size, validity, allocator, acquiredBuffers);
            case FIXED_LEN_BYTE_ARRAY ->
                mergeFixedBinary(parts, size, requiredByteWidth(leaf), validity, allocator, acquiredBuffers);
            case INT96 -> mergeInt96(parts, size, validity, allocator, acquiredBuffers);
        };
    }

    private static int totalSize(List<ColumnVector> parts) {
        int size = 0;
        for (ColumnVector part : parts) {
            size += part.size();
        }
        return size;
    }

    private static Validity mergedValidity(List<ColumnVector> parts, int size) {
        BitSet valid = new BitSet(size);
        int base = 0;
        for (ColumnVector part : parts) {
            for (int i = 0; i < part.size(); i++) {
                if (part.isValid(i)) {
                    valid.set(base + i);
                }
            }
            base += part.size();
        }
        return Validity.of(valid, size);
    }

    private static ColumnVector mergeInts(
            List<ColumnVector> parts,
            int size,
            Validity validity,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {
        MemorySegment dst = acquire(allocator, acquiredBuffers, (long) size * Integer.BYTES);
        int row = 0;
        for (ColumnVector part : parts) {
            IntVector vector = (IntVector) part;
            for (int i = 0; i < vector.size(); i++, row++) {
                dst.setAtIndex(INT32, row, vector.isNull(i) ? 0 : vector.getInt(i));
            }
        }
        return IntVector.segmentBacked(dst, validity);
    }

    private static ColumnVector mergeLongs(
            List<ColumnVector> parts,
            int size,
            Validity validity,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {
        MemorySegment dst = acquire(allocator, acquiredBuffers, (long) size * Long.BYTES);
        int row = 0;
        for (ColumnVector part : parts) {
            LongVector vector = (LongVector) part;
            for (int i = 0; i < vector.size(); i++, row++) {
                dst.setAtIndex(INT64, row, vector.isNull(i) ? 0L : vector.getLong(i));
            }
        }
        return LongVector.segmentBacked(dst, validity);
    }

    private static ColumnVector mergeFloats(
            List<ColumnVector> parts,
            int size,
            Validity validity,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {
        MemorySegment dst = acquire(allocator, acquiredBuffers, (long) size * Float.BYTES);
        int row = 0;
        for (ColumnVector part : parts) {
            FloatVector vector = (FloatVector) part;
            for (int i = 0; i < vector.size(); i++, row++) {
                dst.setAtIndex(FLOAT, row, vector.isNull(i) ? 0f : vector.getFloat(i));
            }
        }
        return FloatVector.segmentBacked(dst, validity);
    }

    private static ColumnVector mergeDoubles(
            List<ColumnVector> parts,
            int size,
            Validity validity,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {
        MemorySegment dst = acquire(allocator, acquiredBuffers, (long) size * Double.BYTES);
        int row = 0;
        for (ColumnVector part : parts) {
            DoubleVector vector = (DoubleVector) part;
            for (int i = 0; i < vector.size(); i++, row++) {
                dst.setAtIndex(DOUBLE, row, vector.isNull(i) ? 0d : vector.getDouble(i));
            }
        }
        return DoubleVector.segmentBacked(dst, validity);
    }

    private static ColumnVector mergeBooleans(
            List<ColumnVector> parts,
            int size,
            Validity validity,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {
        MemorySegment dst = acquire(allocator, acquiredBuffers, Math.max(1L, (size + 7) / 8));
        dst.fill((byte) 0);
        int row = 0;
        for (ColumnVector part : parts) {
            BooleanVector vector = (BooleanVector) part;
            for (int i = 0; i < vector.size(); i++, row++) {
                if (!vector.isNull(i) && vector.getBoolean(i)) {
                    long byteIndex = row >>> 3;
                    int current = dst.get(ValueLayout.JAVA_BYTE, byteIndex) & 0xff;
                    dst.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) (current | (1 << (row & 7))));
                }
            }
        }
        return BooleanVector.segmentBacked(dst, size, validity);
    }

    private static ColumnVector mergeVariableBinary(
            List<ColumnVector> parts,
            int size,
            Validity validity,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {
        long totalBytes = 0;
        for (ColumnVector part : parts) {
            BinaryVector vector = (BinaryVector) part;
            for (int i = 0; i < vector.size(); i++) {
                if (!vector.isNull(i)) {
                    totalBytes += vector.get(i).byteSize();
                }
            }
        }
        // The valve never hands out a zero-length buffer; slice back to the true window for an all-null row.
        MemorySegment backing =
                acquire(allocator, acquiredBuffers, Math.max(1L, totalBytes)).asSlice(0L, totalBytes);
        MemorySegment offsets = acquire(allocator, acquiredBuffers, (size + 1L) * Integer.BYTES);
        long pos = 0;
        int row = 0;
        for (ColumnVector part : parts) {
            BinaryVector vector = (BinaryVector) part;
            for (int i = 0; i < vector.size(); i++, row++) {
                offsets.setAtIndex(INT32, row, (int) pos);
                if (!vector.isNull(i)) {
                    MemorySegment value = vector.get(i);
                    MemorySegment.copy(value, 0L, backing, pos, value.byteSize());
                    pos += value.byteSize();
                }
            }
        }
        offsets.setAtIndex(INT32, size, (int) pos);
        return BinaryVector.of(backing, IntSequence.ofSegment(offsets, size + 1), validity);
    }

    private static ColumnVector mergeFixedBinary(
            List<ColumnVector> parts,
            int size,
            int byteWidth,
            Validity validity,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {
        MemorySegment dst = fixedWidthSlots(parts, size, byteWidth, allocator, acquiredBuffers);
        return FixedLenBinaryVector.of(dst, byteWidth, validity);
    }

    private static ColumnVector mergeInt96(
            List<ColumnVector> parts,
            int size,
            Validity validity,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {
        MemorySegment dst = fixedWidthSlots(parts, size, INT96_WIDTH, allocator, acquiredBuffers);
        return Int96Vector.of(dst, validity);
    }

    /** Packs each part's full-width slots into one pooled backing, null slots zeroed. */
    private static MemorySegment fixedWidthSlots(
            List<ColumnVector> parts,
            int size,
            int byteWidth,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {
        MemorySegment dst = acquire(allocator, acquiredBuffers, (long) size * byteWidth);
        dst.fill((byte) 0);
        int row = 0;
        for (ColumnVector part : parts) {
            for (int i = 0; i < part.size(); i++, row++) {
                if (!part.isNull(i)) {
                    MemorySegment value = fixedWidthValue(part, i);
                    MemorySegment.copy(value, 0L, dst, (long) row * byteWidth, byteWidth);
                }
            }
        }
        return dst;
    }

    private static MemorySegment fixedWidthValue(ColumnVector part, int row) {
        return switch (part) {
            case Int96Vector vector -> vector.get(row);
            case FixedLenBinaryVector vector -> vector.get(row);
            default ->
                throw new IllegalStateException(
                        "a fixed-width spanning-row part must be an Int96Vector or FixedLenBinaryVector; got "
                                + part.getClass().getSimpleName());
        };
    }

    private static int requiredByteWidth(SchemaNode.Primitive leaf) {
        return leaf.typeLength()
                .orElseThrow(() -> new IllegalStateException(
                        "FIXED_LEN_BYTE_ARRAY leaf " + leaf.name() + " is missing typeLength in schema"));
    }

    private static MemorySegment acquire(
            DecodeBufferAllocator allocator, List<AutoCloseable> acquiredBuffers, long byteSize) {
        SegmentPool.Pooled pooled = allocator.acquireMandatory(byteSize);
        acquiredBuffers.add(pooled);
        return pooled.segment();
    }
}
