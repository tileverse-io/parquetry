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
package io.tileverse.parquetry.internal.batch;

import java.util.List;

/**
 * One Arrow field node for a column vector: a row count, a null count, the ordered buffers that hold the node's data,
 * recursive child nodes for nested types, and the encoding tag that tells the decoder how to read the buffers.
 *
 * <p>The buffer and child lists are defensively copied into immutable lists at construction.
 *
 * @param length the number of logical rows this node covers
 * @param nullCount the number of null rows within {@code length}
 * @param buffers the node's buffers in Arrow order
 * @param children child nodes for nested types, empty for primitive nodes
 * @param encoding how to read the value buffers
 */
public record EncodedNode(
        int length, int nullCount, List<EncodedBuffer> buffers, List<EncodedNode> children, NodeEncoding encoding) {

    public EncodedNode {
        buffers = List.copyOf(buffers);
        children = List.copyOf(children);
    }
}
