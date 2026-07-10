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
package io.tileverse.parquetry.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

class StorageOptionsTest {

    /** Minimal command host used only to let picocli inject option values into the mixin. */
    @Command
    static final class Host implements Callable<Integer> {

        @Mixin
        StorageOptions storage;

        @Override
        public Integer call() {
            return 0;
        }
    }

    @Test
    void allFlagsMapToExpectedStorageKeys() {
        Host host = new Host();
        CommandLine cli = new CommandLine(host);
        cli.parseArgs(
                "--provider",
                "s3",
                "--region",
                "us-east-1",
                "--access-key",
                "AK",
                "--secret-key",
                "SK",
                "--path-style",
                "--anonymous",
                "--gcs-project",
                "proj",
                "--endpoint",
                "http://localhost:4443");

        Properties props = host.storage.toProperties();

        assertThat(props).containsEntry("storage.provider", "s3");
        assertThat(props).containsEntry("storage.s3.region", "us-east-1");
        assertThat(props).containsEntry("storage.s3.aws-access-key-id", "AK");
        assertThat(props).containsEntry("storage.s3.aws-secret-access-key", "SK");
        assertThat(props).containsEntry("storage.s3.force-path-style", "true");
        assertThat(props).containsEntry("storage.s3.anonymous", "true");
        assertThat(props).containsEntry("storage.gcs.project-id", "proj");
        assertThat(props).containsEntry("storage.s3.endpoint", "http://localhost:4443");
        assertThat(props).containsEntry("storage.gcs.endpoint", "http://localhost:4443");
        assertThat(props).containsEntry("storage.azure.endpoint", "http://localhost:4443");
        assertThat(props).hasSize(10);
    }

    @Test
    void noFlagsProducesEmptyProperties() {
        Host host = new Host();
        new CommandLine(host).parseArgs();

        Properties props = host.storage.toProperties();

        assertThat(props).isEmpty();
    }
}
