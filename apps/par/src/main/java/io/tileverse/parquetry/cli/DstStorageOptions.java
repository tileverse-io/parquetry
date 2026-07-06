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
package io.tileverse.parquetry.cli;

import java.util.Properties;

import picocli.CommandLine.Option;

/**
 * Storage provider options for the copy DESTINATION, shared by subcommands via {@code @Mixin}. A copy may read from one
 * backend and write to a different one (for example MinIO to Azure), which needs connection flags distinct from those
 * of the source ({@link StorageOptions}). These {@code --dst-} flags map to the same {@code storage.*} parameter keys
 * consumed by {@code StorageFactory.open(URI, Properties)}.
 */
public final class DstStorageOptions {

    /** Usage-help heading that renders the destination flags as their own section, after the source storage section. */
    public static final String HEADING = "%nDestination storage options:%n";

    @Option(
            names = {"--dst-provider"},
            paramLabel = "<id>",
            description = "Force the destination storage provider: s3, gcs, azure, http, file.")
    public String provider;

    @Option(
            names = {"--dst-region"},
            paramLabel = "<region>",
            description = "Destination S3 region.")
    public String region;

    @Option(
            names = {"--dst-access-key"},
            paramLabel = "<key>",
            description = "Destination S3 access key id.")
    public String accessKey;

    @Option(
            names = {"--dst-secret-key"},
            paramLabel = "<key>",
            description = "Destination S3 secret access key.")
    public String secretKey;

    @Option(
            names = {"--dst-path-style"},
            description = "Use S3 path-style addressing for the destination (for MinIO / S3-compatible endpoints).")
    public boolean pathStyle;

    @Option(
            names = {"--dst-anonymous"},
            description = "Access the destination store anonymously (no credentials).")
    public boolean anonymous;

    @Option(
            names = {"--dst-gcs-project"},
            paramLabel = "<project>",
            description = "Destination Google Cloud project id.")
    public String gcsProject;

    @Option(
            names = {"--dst-endpoint"},
            paramLabel = "<url>",
            description = "Destination service endpoint override for S3, GCS, and Azure "
                    + "(e.g. http://localhost:9000 for MinIO, http://localhost:4443 for fake-gcs-server).")
    public String endpoint;

    /**
     * Returns a {@link Properties} containing only the keys for which a flag was explicitly supplied. Absent flags
     * produce no entry, letting the StorageFactory apply its own defaults.
     */
    public Properties toProperties() {
        return StorageProperties.toProperties(
                provider, region, accessKey, secretKey, pathStyle, anonymous, gcsProject, endpoint);
    }
}
