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
package io.tileverse.parquetry.geo.jts;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.InStream;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

/**
 * Shared WKB decode utilities for JTS-backed classes in this package.
 *
 * <p>Provides {@link #read(WKBReader, MemorySegment)}, which adapts a read-only {@link MemorySegment} into the JTS
 * {@link InStream} API and delegates to the supplied reader. Callers own the lifecycle of the {@link WKBReader}
 * (thread-confinement or {@link ThreadLocal}); this class does not create or cache readers.
 */
final class JtsWkb {

    private JtsWkb() {}

    /**
     * Decodes WKB bytes from {@code wkb} using the supplied {@code reader}.
     *
     * @param reader the WKBReader to use; must not be shared across threads
     * @param wkb a read-only segment containing the WKB bytes
     * @return the decoded geometry
     * @throws IllegalStateException when the bytes are not valid WKB
     */
    static Geometry read(WKBReader reader, MemorySegment wkb) {
        try {
            return reader.read(new SegmentInStream(wkb));
        } catch (ParseException | IOException e) {
            throw new IllegalStateException("Failed to decode WKB geometry: " + e.getMessage(), e);
        }
    }

    /**
     * JTS {@link InStream} adapter that streams bytes out of a read-only {@link MemorySegment} using its own offset
     * cursor. The segment is shared safely across callers because position state lives on the adapter, not on the
     * segment itself.
     */
    static final class SegmentInStream implements InStream {

        private final MemorySegment segment;
        private final long size;
        private long offset;

        SegmentInStream(MemorySegment segment) {
            this.segment = segment;
            this.size = segment.byteSize();
        }

        @Override
        public int read(byte[] dst) {
            long remaining = size - offset;
            if (remaining == 0) {
                return -1;
            }
            int n = (int) Math.min(remaining, dst.length);
            MemorySegment.copy(segment, JAVA_BYTE, offset, dst, 0, n);
            offset += n;
            return n;
        }
    }
}
