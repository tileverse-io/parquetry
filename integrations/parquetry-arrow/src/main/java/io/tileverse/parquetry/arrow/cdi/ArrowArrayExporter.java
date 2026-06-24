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
package io.tileverse.parquetry.arrow.cdi;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;

import io.tileverse.parquetry.arrow.columnar.EncodedBuffer;
import io.tileverse.parquetry.arrow.columnar.EncodedNode;
import io.tileverse.parquetry.io.SegmentPool;

/**
 * Exports an {@link EncodedNode} buffer tree into an {@code ArrowArray} C struct over the C Data Interface. Each leaf
 * buffer is copied once into a segment borrowed from a {@link SegmentPool}; the array's {@code release} callback
 * returns every borrowed buffer to the pool and frees the struct scaffolding. The copy is mandatory: a C consumer needs
 * a stable native address, while parquetry's heap-backed buffers are recycled when the batch closes.
 *
 * <p>The top-level array is filled into the caller's {@code out} segment (the array slot a consumer hands to
 * {@code get_next}); the children and the buffer and child pointer arrays are allocated in a per-export arena that the
 * release callback frees. The release reclaim state is keyed by an identifier kept in the array's {@code private_data},
 * which the release upcall reads to find and run the reclaim. Release is idempotent.
 *
 * @see <a href="https://arrow.apache.org/docs/format/CDataInterface.html">Arrow C Data Interface</a>
 */
// The release callbacks are bound as FFM upcall stubs by name (java:S1144 cannot trace the reflective binding) and
// must catch Throwable because an exception escaping an upcall stub crashes the JVM (java:S1181).
@SuppressWarnings({"java:S1144", "java:S1181"})
final class ArrowArrayExporter {

    private static final System.Logger LOGGER = System.getLogger(ArrowArrayExporter.class.getName());

    private static final CallbackRegistry<ExportedBuffers> RECLAIMS = new CallbackRegistry<>();

    private static final MemorySegment RELEASE_TOP = releaseStub("releaseTop");
    private static final MemorySegment RELEASE_CHILD = releaseStub("releaseChild");

    private ArrowArrayExporter() {
        // utility
    }

    /** The number of exported arrays the consumer has not yet released, for leak observability. */
    static int outstanding() {
        return RECLAIMS.outstanding();
    }

    /**
     * Fills {@code out} with {@code node}'s data, borrowing buffer segments from {@code pool}. The returned struct's
     * release callback returns those segments to the pool. The caller owns {@code out}; the children and pointer
     * scaffolding live in an arena the release frees.
     */
    static void export(EncodedNode node, SegmentPool pool, MemorySegment out) {
        Arena scaffolding = Arena.ofShared();
        ExportedBuffers reclaim = new ExportedBuffers(scaffolding);
        fill(out, node, pool, scaffolding, reclaim);
        CDataLayouts.arraySetPrivateData(out, RECLAIMS.register(reclaim));
        CDataLayouts.arraySetRelease(out, RELEASE_TOP);
    }

    private static void fill(
            MemorySegment array, EncodedNode node, SegmentPool pool, Arena scaffolding, ExportedBuffers reclaim) {
        CDataLayouts.arraySetLength(array, node.length());
        CDataLayouts.arraySetNullCount(array, node.nullCount());
        CDataLayouts.arraySetOffset(array, 0L);
        fillBuffers(array, node.buffers(), pool, scaffolding, reclaim);
        fillChildren(array, node.children(), pool, scaffolding, reclaim);
        CDataLayouts.arraySetDictionary(array, MemorySegment.NULL);
    }

    private static void fillBuffers(
            MemorySegment array,
            List<EncodedBuffer> buffers,
            SegmentPool pool,
            Arena scaffolding,
            ExportedBuffers reclaim) {
        MemorySegment pointers = scaffolding.allocate(ValueLayout.ADDRESS, buffers.size());
        for (int i = 0; i < buffers.size(); i++) {
            pointers.setAtIndex(ValueLayout.ADDRESS, i, copyBuffer(buffers.get(i), pool, reclaim));
        }
        CDataLayouts.arraySetBuffers(array, buffers.size(), pointers);
    }

    /** Copies one buffer into a pooled segment and returns its address; a zero-length buffer maps to a NULL pointer. */
    private static MemorySegment copyBuffer(EncodedBuffer buffer, SegmentPool pool, ExportedBuffers reclaim) {
        MemorySegment source = buffer.bytes();
        long size = source.byteSize();
        if (size == 0) {
            return MemorySegment.NULL;
        }
        SegmentPool.Pooled pooled = pool.borrow(size);
        MemorySegment.copy(source, 0L, pooled.segment(), 0L, size);
        reclaim.track(pooled);
        return pooled.segment();
    }

    private static void fillChildren(
            MemorySegment array,
            List<EncodedNode> children,
            SegmentPool pool,
            Arena scaffolding,
            ExportedBuffers reclaim) {
        if (children.isEmpty()) {
            CDataLayouts.arraySetChildren(array, 0L, MemorySegment.NULL);
            return;
        }
        MemorySegment pointers = scaffolding.allocate(ValueLayout.ADDRESS, children.size());
        for (int i = 0; i < children.size(); i++) {
            MemorySegment childArray = scaffolding.allocate(CDataLayouts.ARROW_ARRAY);
            fill(childArray, children.get(i), pool, scaffolding, reclaim);
            CDataLayouts.arraySetPrivateData(childArray, MemorySegment.NULL);
            CDataLayouts.arraySetRelease(childArray, RELEASE_CHILD);
            pointers.setAtIndex(ValueLayout.ADDRESS, i, childArray);
        }
        CDataLayouts.arraySetChildren(array, children.size(), pointers);
    }

    /**
     * Releases the top array: looks up the reclaim state by the {@code private_data} key, returns every borrowed buffer
     * to the pool, frees the scaffolding, and nulls the release pointer. A second call finds no state and is a no-op.
     * The pointer is nulled before the reclaim runs because the reclaim can throw (closing a reader over object
     * storage) and the whole body is wrapped: an exception escaping an FFM upcall stub crashes the JVM.
     */
    private static void releaseTop(MemorySegment self) {
        try {
            MemorySegment array = self.reinterpret(CDataLayouts.ARROW_ARRAY.byteSize());
            CDataLayouts.arraySetRelease(array, MemorySegment.NULL);
            ExportedBuffers reclaim = RECLAIMS.remove(CDataLayouts.arrayPrivateData(array));
            if (reclaim != null) {
                reclaim.release();
            }
        } catch (Throwable t) {
            LOGGER.log(System.Logger.Level.WARNING, "ArrowArray release callback failed", t);
        }
    }

    /**
     * A child array's release is a no-op: the top array's release reclaims the whole tree's buffers and scaffolding.
     * Wrapped in a catch-all because an exception escaping an FFM upcall stub crashes the JVM.
     */
    private static void releaseChild(MemorySegment self) {
        try {
            MemorySegment array = self.reinterpret(CDataLayouts.ARROW_ARRAY.byteSize());
            CDataLayouts.arraySetRelease(array, MemorySegment.NULL);
        } catch (Throwable t) {
            LOGGER.log(System.Logger.Level.WARNING, "ArrowArray child release callback failed", t);
        }
    }

    private static MemorySegment releaseStub(String methodName) {
        return CDataStubs.upcall(
                MethodHandles.lookup(),
                methodName,
                MethodType.methodType(void.class, MemorySegment.class),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    }
}
