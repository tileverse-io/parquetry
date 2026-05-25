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
package io.tileverse.parquetry.data;

/**
 * Minimal public view of one on-disk row group: index in file order, row count, and total byte sizes.
 *
 * <p>This is deliberately a thin record rather than the raw thrift {@link io.tileverse.parquetry.format.RowGroup} -
 * keeping the public API narrow avoids leaking the format module's internal types (column-chunk metadata, encryption,
 * etc.) through {@code ParquetDataset}. Callers needing the full thrift view can call
 * {@link io.tileverse.parquetry.format.ParquetFormat#readFooter ParquetFormat.readFooter} directly.
 *
 * @param index the row group's index in file order, starting at 0
 * @param rowCount number of rows in the row group
 * @param totalByteSize total uncompressed byte size of the row group on disk
 * @param totalCompressedSize total compressed byte size of the row group on disk, or {@code -1L} if the writer omitted
 *     this field (callers should treat {@code -1L} as "unknown")
 */
public record RowGroupSummary(int index, long rowCount, long totalByteSize, long totalCompressedSize) {}
