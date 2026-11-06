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

import io.tileverse.parquetry.format.MalformedFileException;

/**
 * The most uncompressed bytes accepted from one page of a row group. The decode of a page allocates native memory by
 * the size declared in its header, outside the heap limit. A corrupt header can declare about 2 GiB, and on a small pod
 * the kernel kills the process without an exception.
 *
 * <p>A page holds part of a column chunk, and the footer states the uncompressed size of the chunks of each row group.
 * A page declaring more than that size is corrupt. A page of up to 64 MiB passes regardless of the size stated for its
 * row group, because some writers state a size a few bytes short of the true one. A row group stating no positive size
 * sets no limit.
 *
 * @param maxBytes the most uncompressed bytes of an accepted page
 */
public record PageSizeLimit(long maxBytes) {

    /** The limit of a row group stating no positive size. A page size is an int, and each one fits. */
    public static final PageSizeLimit NONE = new PageSizeLimit(Integer.MAX_VALUE);

    private static final long FLOOR_BYTES = 64L << 20;

    /** The limit of a row group whose column chunks hold {@code uncompressedSize} bytes. */
    public static PageSizeLimit ofRowGroup(long uncompressedSize) {
        if (uncompressedSize <= 0) {
            return NONE;
        }
        return new PageSizeLimit(Math.max(uncompressedSize, FLOOR_BYTES));
    }

    /** Whether a page declaring {@code uncompressedPageSize} bytes is accepted. */
    public boolean accepts(int uncompressedPageSize) {
        return uncompressedPageSize <= maxBytes;
    }

    /**
     * The failure of a page declaring {@code uncompressedPageSize} bytes above this limit.
     *
     * @param page the page as named by the failure, such as "Column a data page 3"
     */
    public MalformedFileException refusal(String page, int uncompressedPageSize) {
        return new MalformedFileException(page + " declares " + uncompressedPageSize
                + " uncompressed bytes, above the limit of " + maxBytes + " bytes set by the size of its row group");
    }
}
