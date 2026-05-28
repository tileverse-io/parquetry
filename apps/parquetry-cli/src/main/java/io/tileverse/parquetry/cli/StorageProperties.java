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
package io.tileverse.parquetry.cli;

import java.util.Properties;

/**
 * Maps connection-flag values to the {@code storage.*} parameter keys consumed by {@code StorageFactory.open(URI,
 * Properties)}. Shared by the source ({@link StorageOptions}) and destination ({@link DstStorageOptions}) mixins, which
 * expose the same eight values under different flag names.
 */
final class StorageProperties {

    // StorageFactory parameter keys
    private static final String KEY_PROVIDER = "storage.provider";
    private static final String KEY_S3_REGION = "storage.s3.region";
    private static final String KEY_S3_ACCESS_KEY = "storage.s3.aws-access-key-id";
    private static final String KEY_S3_SECRET_KEY = "storage.s3.aws-secret-access-key";
    private static final String KEY_S3_PATH_STYLE = "storage.s3.force-path-style";
    private static final String KEY_S3_ANONYMOUS = "storage.s3.anonymous";
    private static final String KEY_GCS_PROJECT = "storage.gcs.project-id";
    // GCS uses a "host" parameter to redirect requests to a local server (e.g. fake-gcs-server).
    // S3-compatible custom endpoints are handled differently: the user encodes the endpoint in the
    // argument URI itself (http://host:port/bucket/key) and passes --provider s3.
    private static final String KEY_GCS_HOST = "storage.gcs.host";

    private StorageProperties() {}

    /**
     * Returns a {@link Properties} containing only the keys for which a value was explicitly supplied. A {@code null}
     * String or {@code false} boolean produces no entry, letting the StorageFactory apply its own defaults.
     */
    // The CLI exposes eight independent connection flags; mapping them is a flat translation, not a parameter-object
    // case.
    @SuppressWarnings("java:S107")
    static Properties toProperties(
            String provider,
            String region,
            String accessKey,
            String secretKey,
            boolean pathStyle,
            boolean anonymous,
            String gcsProject,
            String endpoint) {
        Properties props = new Properties();
        if (provider != null) {
            props.setProperty(KEY_PROVIDER, provider);
        }
        if (region != null) {
            props.setProperty(KEY_S3_REGION, region);
        }
        if (accessKey != null) {
            props.setProperty(KEY_S3_ACCESS_KEY, accessKey);
        }
        if (secretKey != null) {
            props.setProperty(KEY_S3_SECRET_KEY, secretKey);
        }
        if (pathStyle) {
            props.setProperty(KEY_S3_PATH_STYLE, "true");
        }
        if (anonymous) {
            props.setProperty(KEY_S3_ANONYMOUS, "true");
        }
        if (gcsProject != null) {
            props.setProperty(KEY_GCS_PROJECT, gcsProject);
        }
        if (endpoint != null) {
            props.setProperty(KEY_GCS_HOST, endpoint);
        }
        return props;
    }
}
