/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

/**
 * A file discovered under a {@link FileSource} root: its path relative to that root, its size, and a factory for a
 * fresh, thread-safe byte source over its contents.
 */
public interface FileEntry {

    /** POSIX-style path relative to the {@link FileSource#root()}, e.g. {@code "part-0.parquet"}. */
    String relativePath();

    /** File length in bytes, or {@code -1} when the backend does not report it cheaply. */
    long sizeBytes();

    /** Opens a fresh {@link ByteRangeSource} for this file. The caller owns and closes it. */
    ByteRangeSource open();
}
