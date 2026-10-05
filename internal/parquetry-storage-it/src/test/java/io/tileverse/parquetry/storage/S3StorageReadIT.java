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
package io.tileverse.parquetry.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The storage read against S3 served by LocalStack. The bucket is created by the {@code awslocal} of the container and
 * the fixture is uploaded through the Storage: the class uses no AWS SDK type.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3StorageReadIT extends AbstractStorageReadIT {

    private static final String BUCKET = "parquetry-it";

    @Container
    @SuppressWarnings("resource")
    static LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.2.0")).withServices("s3");

    @TempDir
    static Path workDir;

    private static Path fixture;

    @BeforeAll
    static void uploadFixture() throws IOException, InterruptedException {
        fixture = extractFixture(workDir);
        createBucket();
        putFixture(bucket(), localstackProperties(), fixture);
    }

    private static void createBucket() throws IOException, InterruptedException {
        ExecResult created = localstack.execInContainer("awslocal", "s3api", "create-bucket", "--bucket", BUCKET);
        assertThat(created.getExitCode())
                .as("awslocal create-bucket: %s", created.getStderr())
                .isZero();
    }

    @Override
    protected URI container() {
        return bucket();
    }

    @Override
    protected Properties storageProperties() {
        return localstackProperties();
    }

    @Override
    protected Path localFixture() {
        return fixture;
    }

    private static URI bucket() {
        return URI.create("s3://" + BUCKET + "/");
    }

    private static Properties localstackProperties() {
        String endpoint = localstack.getEndpoint().toString().replaceAll("/+$", "");
        Properties properties = new Properties();
        properties.setProperty("storage.s3.endpoint", endpoint);
        properties.setProperty("storage.s3.region", localstack.getRegion());
        properties.setProperty("storage.s3.aws-access-key-id", localstack.getAccessKey());
        properties.setProperty("storage.s3.aws-secret-access-key", localstack.getSecretKey());
        properties.setProperty("storage.s3.force-path-style", "true");
        return properties;
    }
}
