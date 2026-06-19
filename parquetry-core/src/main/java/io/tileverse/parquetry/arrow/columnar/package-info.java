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
/**
 * Public Arrow columnar encoder: converts a parquetry {@code ColumnVector} (and a whole {@code ParquetRecordBatch}) to
 * and from the Apache Arrow buffer layout.
 *
 * <p>{@link io.tileverse.parquetry.arrow.columnar.ArrowBuffers} builds and reads the fixed-layout byte buffers of an
 * Arrow field node (validity bitmap, little-endian fixed-width values, and int32 offsets, each padded to an 8-byte
 * boundary). An {@link io.tileverse.parquetry.arrow.columnar.EncodedNode} is one field node (row count, null count,
 * ordered {@link io.tileverse.parquetry.arrow.columnar.EncodedBuffer}s, child nodes, and a
 * {@link io.tileverse.parquetry.arrow.columnar.NodeEncoding} tag), and
 * {@link io.tileverse.parquetry.arrow.columnar.EncodedBatch} is one such node per top-level column of a batch.
 * {@link io.tileverse.parquetry.arrow.columnar.ArrowBufferCodec} encodes and decodes a single vector,
 * {@link io.tileverse.parquetry.arrow.columnar.BatchArrowLayout} a whole batch, and
 * {@link io.tileverse.parquetry.arrow.columnar.ColumnType} is the recursive type descriptor the decoder follows.
 *
 * <p>Every produced buffer is a read-only {@link java.lang.foreign.MemorySegment} in the Apache Arrow columnar layout.
 * This package is the supported substrate for the spill round-trip in parquetry-core and for the Arrow IPC and C Data
 * Interface output planned in the parquetry-arrow integration.
 *
 * @see <a href="https://arrow.apache.org/docs/format/Columnar.html">Apache Arrow columnar format specification</a>
 */
package io.tileverse.parquetry.arrow.columnar;
