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
package io.tileverse.parquetry.arrow.columnar;

/**
 * Tells the Arrow-buffer codec which {@code ColumnVector} type to rebuild from an {@link EncodedNode}. This is the
 * codec's own discriminator over the column-vector kinds, distinct from the physical Parquet type: several vector kinds
 * share one physical type, and the decoder selects among the vectors, not the wire types. Constants grow one per
 * supported vector kind as the codec learns to handle more of them.
 */
public enum LeafType {
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    BINARY,
    FIXED_LEN_BINARY,
    INT96
}
