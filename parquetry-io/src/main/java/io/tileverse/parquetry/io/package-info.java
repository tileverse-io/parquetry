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
 * parquetry's read-IO SPIs - positional reads, file discovery, and native buffer pooling - with no domain knowledge.
 *
 * <p>{@link io.tileverse.parquetry.io.ByteRangeSource} is the single positional read dependency of the format and core
 * modules. Local files open through {@link io.tileverse.parquetry.io.ByteRangeSource#ofFile}, and any tileverse-storage
 * {@code RangeReader} (S3, Azure, GCS, HTTP, local) through {@link io.tileverse.parquetry.io.ByteRangeSource#of} or
 * {@link io.tileverse.parquetry.io.ByteRangeSource#owning}.
 *
 * <p>{@link io.tileverse.parquetry.io.FileSource} discovers the files under a root, each openable as a
 * {@link io.tileverse.parquetry.io.ByteRangeSource}. A source is built by one of the
 * {@link io.tileverse.parquetry.io.FileSource} factories: over a local directory, a local file, or a tileverse
 * {@code Storage} opened by the caller.
 *
 * <p>{@link io.tileverse.parquetry.io.SegmentPool} pools native {@link java.lang.foreign.MemorySegment} buffers for
 * column-chunk fetch and per-page decompression, keeping the streaming memory bounded. Its default retains a small
 * bounded set of buffers.
 *
 * <p>The {@link io.tileverse.parquetry.io.limits} sub-package reports the machine resource facts (memory, disk,
 * processors) and the derived caps the core read path sizes its budgets from.
 */
package io.tileverse.parquetry.io;
