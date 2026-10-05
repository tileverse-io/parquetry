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

import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * The storage read against an Apache httpd container serving the fixture over range GETs. With the cloud providers on
 * the class path a plain HTTP URL has several candidate providers; the resolver settles on the HTTP one from the
 * response headers of the server.
 */
@Testcontainers(disabledWithoutDocker = true)
class HttpStorageReadIT extends AbstractStorageReadIT {

    private static final String DOCUMENT_ROOT = "/usr/local/apache2/htdocs/";

    private static final int WORLD_READABLE = 0644;

    @TempDir
    static Path workDir;

    private static Path fixture;

    @SuppressWarnings("resource")
    private static GenericContainer<?> httpd;

    /** Started by hand: the fixture must be inside the container before it starts. */
    @BeforeAll
    static void startHttpd() {
        fixture = extractFixture(workDir);
        MountableFile served = MountableFile.forHostPath(fixture, WORLD_READABLE);
        httpd = new GenericContainer<>(DockerImageName.parse("httpd:alpine"))
                .withExposedPorts(80)
                .withCopyToContainer(served, DOCUMENT_ROOT + KEY)
                .waitingFor(Wait.forHttp("/").forPort(80));
        httpd.start();
    }

    @AfterAll
    static void stopHttpd() {
        if (httpd != null) {
            httpd.stop();
        }
    }

    @Override
    protected URI container() {
        return URI.create("http://" + httpd.getHost() + ":" + httpd.getFirstMappedPort() + "/");
    }

    @Override
    protected Properties storageProperties() {
        return new Properties();
    }

    @Override
    protected Path localFixture() {
        return fixture;
    }
}
