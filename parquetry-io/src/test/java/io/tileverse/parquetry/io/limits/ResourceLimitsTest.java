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
package io.tileverse.parquetry.io.limits;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ResourceLimitsTest {

    @Test
    void defaultReportsPositiveMachineFacts() {
        ResourceLimits limits = ResourceLimits.getDefault();
        assertThat(limits.availableMemoryBytes()).as("memory").isPositive();
        assertThat(limits.usableDiskBytes(Path.of(System.getProperty("java.io.tmpdir"))))
                .as("disk")
                .isPositive();
        assertThat(limits.availableProcessors()).as("cores").isPositive();
        assertThat(limits.spillDir()).as("spill dir").isNotNull();
    }

    @Test
    void aFixedFakeReportsItsValues() {
        Path dir = Path.of("/tmp/x");
        ResourceLimits fake = ResourceLimits.fixed(2L << 30, 9L << 30, 4, dir);
        assertThat(fake.availableMemoryBytes()).isEqualTo(2L << 30);
        assertThat(fake.usableDiskBytes(Path.of("/anything"))).isEqualTo(9L << 30);
        assertThat(fake.availableProcessors()).isEqualTo(4);
        assertThat(fake.spillDir()).isEqualTo(dir);
    }

    private static final String KEY = "parquetry.io.maxOffHeap";

    @ParameterizedTest
    @CsvSource({
        "512m, 536870912",
        "2g, 2147483648",
        "1024k, 1048576",
        "1048576, 1048576",
        "512M, 536870912",
    })
    void resolveSizeBytesParsesUnitsAndCase(String raw, long expectedBytes) {
        assertThat(DefaultResourceLimits.resolveSizeBytes(raw, null, KEY)).isEqualTo(expectedBytes);
    }

    @Test
    void resolveSizeBytesPrefersThePropertyOverTheEnv() {
        assertThat(DefaultResourceLimits.resolveSizeBytes("512m", "1g", KEY)).isEqualTo(512L << 20);
    }

    @Test
    void resolveSizeBytesFallsBackToTheEnvWhenThePropertyIsAbsent() {
        assertThat(DefaultResourceLimits.resolveSizeBytes(null, "256m", KEY)).isEqualTo(256L << 20);
    }

    @Test
    void anUnsetSizeOverrideReturnsTheProbeSentinel() {
        assertThat(DefaultResourceLimits.resolveSizeBytes(null, null, KEY)).isZero();
        assertThat(DefaultResourceLimits.resolveSizeBytes("   ", null, KEY)).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc", "-5", "0", "g"})
    void anInvalidSizeOverrideFailsNamingTheKey(String raw) {
        assertThatThrownBy(() -> DefaultResourceLimits.resolveSizeBytes(raw, null, KEY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(KEY);
    }

    @Test
    void resolveSpillDirDefaultsToTheTempDirectoryWhenUnset() {
        assertThat(DefaultResourceLimits.resolveSpillDir(null, null))
                .isEqualTo(Path.of(System.getProperty("java.io.tmpdir")));
    }

    @Test
    void resolveSpillDirUsesTheOverride() {
        assertThat(DefaultResourceLimits.resolveSpillDir("/var/spill", null)).isEqualTo(Path.of("/var/spill"));
    }
}
