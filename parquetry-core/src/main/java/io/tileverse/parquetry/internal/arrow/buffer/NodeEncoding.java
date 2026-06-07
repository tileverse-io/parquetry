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
package io.tileverse.parquetry.internal.arrow.buffer;

/**
 * How an {@link EncodedNode}'s value buffers are laid out, picked by the encoder per column vector and read back by the
 * decoder.
 */
public sealed interface NodeEncoding permits NodeEncoding.Plain, NodeEncoding.FixedWidth, NodeEncoding.Dictionary {

    /**
     * Values stored directly, with no fixed per-value width (for example variable-width binary with an offsets buffer).
     */
    record Plain() implements NodeEncoding {}

    /** Values stored as a packed array of {@code byteWidth}-byte little-endian elements. */
    record FixedWidth(int byteWidth) implements NodeEncoding {}

    /**
     * Values stored as indices into a separately encoded dictionary node.
     *
     * @param dictionary the node holding the distinct values
     * @param byteWidth width of one packed dictionary value, or {@code -1} for variable-width dictionary values
     */
    record Dictionary(EncodedNode dictionary, int byteWidth) implements NodeEncoding {}
}
