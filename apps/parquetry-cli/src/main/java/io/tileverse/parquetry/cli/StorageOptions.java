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

import picocli.CommandLine.Option;

/**
 * Storage provider options shared by subcommands via {@code @Mixin}. These flags map directly to the {@code storage.*}
 * parameter keys consumed by {@code StorageFactory.open(URI, Properties)}.
 */
public final class StorageOptions {

    /** Usage-help heading that renders these shared flags as their own section, after the command's own options. */
    public static final String HEADING = "%nStorage options:%n";

    @Option(
            names = {"--provider"},
            paramLabel = "<id>",
            description = "Force the storage provider: s3, gcs, http, file.")
    public String provider;

    @Option(
            names = {"--region"},
            paramLabel = "<region>",
            description = "S3 region.")
    public String region;

    @Option(
            names = {"--access-key"},
            paramLabel = "<key>",
            description = "S3 access key id.")
    public String accessKey;

    @Option(
            names = {"--secret-key"},
            paramLabel = "<key>",
            description = "S3 secret access key.")
    public String secretKey;

    @Option(
            names = {"--path-style"},
            description = "Use S3 path-style addressing (for MinIO / S3-compatible endpoints).")
    public boolean pathStyle;

    @Option(
            names = {"--anonymous"},
            description = "Access the store anonymously (no credentials).")
    public boolean anonymous;

    @Option(
            names = {"--gcs-project"},
            paramLabel = "<project>",
            description = "Google Cloud project id.")
    public String gcsProject;

    @Option(
            names = {"--endpoint"},
            paramLabel = "<url>",
            description = "GCS host override, e.g. http://localhost:4443. "
                    + "For S3-compatible endpoints, pass the full https URL as the argument and use --provider s3.")
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
