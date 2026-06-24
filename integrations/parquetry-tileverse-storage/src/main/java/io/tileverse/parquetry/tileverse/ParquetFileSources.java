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
package io.tileverse.parquetry.tileverse;

import java.net.URI;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

import io.tileverse.parquetry.io.FileSource;
import io.tileverse.parquetry.io.LocalFileSource;

/**
 * Opens a {@link FileSource} for a dataset container, routing local files to parquetry's native filesystem source and
 * remote URIs (S3/Azure/GCS/HTTP) to tileverse-storage. Because parquetry opens and closes a source per operation and
 * reads positionally, a local file is best served by the thin native {@code FileChannel} source rather than
 * tileverse-storage's reader, which keeps a channel open across idle periods for long-lived tile serving. Remote URIs
 * go through {@link StorageFileSource}, which applies parquetry's cache tuning (see {@link ParquetStorage}).
 *
 * <p>A {@code file} scheme or a scheme-less URI is local; every other scheme is remote.
 */
public final class ParquetFileSources {

    private ParquetFileSources() {}

    /** Opens a {@link FileSource} over {@code baseUri} listing entries that match {@code glob}. */
    public static FileSource open(URI baseUri, String glob, Properties properties) {
        Objects.requireNonNull(baseUri, "baseUri");
        Objects.requireNonNull(glob, "glob");
        Objects.requireNonNull(properties, "properties");
        if (isLocal(baseUri)) {
            return LocalFileSource.directory(toLocalPath(baseUri), glob);
        }
        return StorageFileSource.open(baseUri, glob, properties);
    }

    /**
     * Opens a {@link FileSource} over a SINGLE file/object {@code objectUri}, opened directly (no directory listing).
     * For a remote object this reads by key (a GET-class lookup), which lets a credential with only GET (no LIST)
     * permission serve the object.
     *
     * <p>The {@code container + key} model drops the URI query string. A presigned {@code ?X-Amz-Signature=...} URL
     * therefore does not authenticate through this path. Presigned-URL query auth is not supported here.
     */
    public static FileSource openObject(URI objectUri, Properties properties) {
        Objects.requireNonNull(objectUri, "objectUri");
        Objects.requireNonNull(properties, "properties");
        if (isLocal(objectUri)) {
            return LocalFileSource.file(toLocalPath(objectUri));
        }
        URI container = objectUri.resolve(".");
        String key = objectKey(objectUri);
        return StorageFileSource.object(container, key, properties);
    }

    private static boolean isLocal(URI baseUri) {
        String scheme = baseUri.getScheme();
        return scheme == null || "file".equals(scheme);
    }

    /**
     * The object key of a remote URI: its final path segment, percent-decoded (e.g. {@code %20} back to a space) to the
     * real key the Storage backend expects.
     */
    private static String objectKey(URI objectUri) {
        String path = objectUri.getPath();
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("object URI has no path segment: " + objectUri);
        }
        return path.substring(path.lastIndexOf('/') + 1);
    }

    private static Path toLocalPath(URI baseUri) {
        if ("file".equals(baseUri.getScheme())) {
            return Path.of(baseUri);
        }
        String path = baseUri.getPath();
        return Path.of(path != null ? path : baseUri.toString());
    }
}
