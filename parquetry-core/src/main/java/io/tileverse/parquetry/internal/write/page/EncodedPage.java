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

import io.tileverse.parquetry.format.PageHeader;

/**
 * Receipt for a single page successfully emitted by {@link PageWriter}.
 *
 * <p>{@link #pageHeader()} is the exact header that was Thrift-serialized to the output stream; the writer relies on it
 * to refresh the offset-index entry that points at this page. {@link #headerBytes()} is the byte length of the
 * Thrift-encoded header itself; {@link #payloadBytes()} is the byte length of the bytes that follow the header on the
 * wire (compressed for V1 and dictionary pages; level-bytes plus compressed values for V2). The compressed-page-size
 * carried by {@link PageHeader#compressedPageSize()} equals {@link #payloadBytes()}; the offset-index entry covers the
 * sum {@code headerBytes + payloadBytes}.
 *
 * @param pageHeader the page header that was written to the output stream
 * @param headerBytes byte length of the Thrift-encoded {@link PageHeader} struct
 * @param payloadBytes byte length of the bytes written after the header (rep + def + compressed values for V2;
 *     compressed level-prefixed-payload for V1; compressed dictionary values for a dictionary page)
 */
public record EncodedPage(PageHeader pageHeader, int headerBytes, int payloadBytes) {

    /** Total bytes this page occupies on disk: the header plus its payload. */
    public int totalBytes() {
        return headerBytes + payloadBytes;
    }
}
