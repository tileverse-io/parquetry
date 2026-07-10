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
package io.tileverse.parquetry.io;

import java.net.URI;
import java.util.stream.Stream;

/**
 * Storage-agnostic discovery of the files under a single root. Each listed {@link FileEntry} is openable as a
 * {@link ByteRangeSource}. Implementations live in the IO layer (a JDK local-directory source) or in integrations (a
 * tileverse {@code Storage}-backed source).
 */
public interface FileSource extends AutoCloseable {

    /** The root the files live under: a directory, bucket, or bucket-prefix, or a single-file location. */
    URI root();

    /**
     * Lists the files under {@link #root()}; each {@link FileEntry#relativePath()} is relative to it. The returned
     * stream may hold backend resources and must be closed by the caller (try-with-resources).
     */
    Stream<FileEntry> list();

    /** Releases backend resources (e.g. an underlying tileverse Storage). Narrowed to no checked exception. */
    @Override
    void close();
}
