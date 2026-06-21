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
package io.tileverse.parquetry.iceberg;

import java.util.List;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Opens an absolute Iceberg location (a {@code metadata.json}/manifest/data-file URI) as a byte source. The local
 * implementation serves the corpus; a tileverse-storage implementation for S3/Azure/GCS/HTTP is a follow-on.
 */
public interface IcebergFileIO extends AutoCloseable {

    /** Opens a fresh byte source over {@code location} (an absolute URI string). The caller owns and closes it. */
    ByteRangeSource open(String location);

    /**
     * Absolute locations of the entries directly under {@code prefix}; empty when the backend cannot list (for example
     * HTTP).
     */
    List<String> list(String prefix);

    @Override
    default void close() {}
}
