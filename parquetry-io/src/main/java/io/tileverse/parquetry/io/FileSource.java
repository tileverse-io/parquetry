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
import java.nio.file.Path;
import java.util.Properties;
import java.util.stream.Stream;

import io.tileverse.storage.Storage;

/**
 * Discovery of the files under a single root, independent of the backing store. Each listed {@link FileEntry} is
 * openable as a {@link ByteRangeSource}. A source is built by one of the factories below: from a URI and storage
 * properties, over a local directory or a local file, or over a tileverse {@link Storage} opened by the caller.
 */
public interface FileSource extends AutoCloseable {

    /** The root the files live under: a directory, bucket, or bucket-prefix, or a single-file location. */
    URI root();

    /**
     * Lists the files under {@link #root()}; each {@link FileEntry#relativePath()} is relative to it. The returned
     * stream may hold backend resources and must be closed by the caller (try-with-resources).
     */
    Stream<FileEntry> list();

    /**
     * Releases backend resources: the tileverse Storage opened by this source, when it opened one. Narrowed to no
     * checked exception. A source over its own Storage then fails to open a {@link FileEntry} listed before the close;
     * a source over a caller's Storage keeps opening its entries. A {@link ByteRangeSource} opened before the close
     * keeps reading until its own close.
     */
    @Override
    void close();

    /**
     * A source over the entries under {@code baseUri} matching {@code glob}. A {@code file} scheme or a scheme-less URI
     * names a local directory, served as by {@link #directory}. Any other scheme is served over a tileverse
     * {@link Storage} opened here and closed by {@link #close()}; its entries borrow their readers, on the terms stated
     * on {@link #over}.
     *
     * <p>The Storage is configured by the {@code storage.*} entries of {@code properties} alone. The byte-range cache
     * therefore stays off unless {@code storage.caching.enabled} is set: a read asks for the exact ranges named by its
     * plan, and retaining them costs heap needed for decode.
     *
     * @throws IllegalStateException if no storage provider on the class path serves the scheme of {@code baseUri}
     */
    static FileSource open(URI baseUri, String glob, Properties properties) {
        return StorageFileSource.open(baseUri, glob, properties);
    }

    /**
     * A source over the single file or object at {@code objectUri}, resolved without listing. A local URI is served as
     * by {@link #file}. A remote object is resolved by a metadata lookup on its key, as by {@link #object}, over a
     * Storage opened for its parent container, configured as stated on {@link #open} and closed by {@link #close()}.
     *
     * <p>The container and the key come from the URI path, and the query string is dropped: a presigned URL does not
     * authenticate through this factory.
     *
     * @throws IllegalArgumentException if a remote {@code objectUri} has no path
     * @throws IllegalStateException if no storage provider on the class path serves the scheme of {@code objectUri}
     */
    static FileSource openObject(URI objectUri, Properties properties) {
        return StorageFileSource.openObject(objectUri, properties);
    }

    /**
     * A source over the files under {@code directory} matching {@code glob}.
     *
     * <p>The glob follows tileverse's {@code StoragePattern} syntax (DuckDB-style, with brace alternation), matched
     * against the {@code /}-separated path of a file relative to the directory; a backslash is an ordinary character on
     * any platform. A glob without metacharacters names a prefix: a file of that name is listed on its own, and a
     * directory of that name has its children listed. A prefix spelled differently from the name on disk matches
     * nothing, on a case-insensitive filesystem as well. A missing {@code directory} lists nothing, as does a path
     * naming a regular file.
     *
     * @throws IllegalArgumentException if {@code glob} is empty, starts with {@code /}, has a {@code .} or {@code ..}
     *     segment, contains a NUL character, or does not compile
     */
    static FileSource directory(Path directory, String glob) {
        return StorageFileSource.directory(directory, glob);
    }

    /**
     * A source over exactly one file; {@link #root()} is the file's parent directory. It reports that file alone, never
     * its siblings, and lists nothing when the file or its directory is missing.
     *
     * @throws IllegalArgumentException if {@code file} has no parent directory
     */
    static FileSource file(Path file) {
        return StorageFileSource.file(file);
    }

    /**
     * A source over the entries of {@code storage} matching {@code pattern}. The caller keeps the Storage:
     * {@link #close()} leaves it open, and the entries borrow their readers, because closing a caching remote reader
     * drops the process-shared range cache for that object.
     *
     * <p>The {@link ByteRangeSource} over a borrowed reader never closes it. A Storage handing out a fresh reader per
     * call, the local file backend among them, therefore leaves one reader unclosed per {@link FileEntry#open()}.
     */
    static FileSource over(Storage storage, String pattern) {
        return StorageFileSource.over(storage, pattern);
    }

    /**
     * A source over the single object of {@code storage} at {@code key}, resolved by a metadata lookup rather than by
     * listing the prefix around it. A credential with only GET permission can therefore read it.
     *
     * <p>The caller keeps the Storage, and the entry borrows its reader, on the terms stated on {@link #over},
     * including the reader left unclosed by each {@link FileEntry#open()}.
     */
    static FileSource object(Storage storage, String key) {
        return StorageFileSource.object(storage, key);
    }
}
