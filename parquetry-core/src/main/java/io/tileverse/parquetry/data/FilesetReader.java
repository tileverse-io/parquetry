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

import io.tileverse.storage.RangeReader;

/**
 * Adapter contract for resolving the per-file {@link RangeReader range readers} that a multi-file
 * {@link ParquetDataset} will stitch together at read time.
 *
 * <p>This is the public seam that future work (partitioned datasets, Iceberg / Delta manifest handlers) will satisfy
 * without forcing {@code ParquetDataset.open} to grow new overloads. The single-file path goes through
 * {@link ParquetDataset#open(RangeReader)} unchanged.
 */
public interface FilesetReader {

    /**
     * Returns a {@link RangeReader} positioned at file {@code index} of the fileset. Implementations decide their own
     * indexing (file order, manifest order, etc.); the caller of the multi-file dataset only iterates in increasing
     * {@code index} order.
     */
    RangeReader openFile(int index);

    /** Returns the number of files in the fileset. */
    int fileCount();
}
