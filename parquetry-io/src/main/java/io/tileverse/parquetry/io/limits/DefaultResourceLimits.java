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
package io.tileverse.parquetry.io.limits;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import com.sun.management.OperatingSystemMXBean;

final class DefaultResourceLimits implements ResourceLimits {

    static final DefaultResourceLimits INSTANCE = create();

    private static final String MEMORY_PROPERTY = "parquetry.io.maxOffHeap";
    private static final String MEMORY_ENV = "PARQUETRY_IO_MAX_OFFHEAP";
    private static final String DISK_PROPERTY = "parquetry.io.maxSpill";
    private static final String DISK_ENV = "PARQUETRY_IO_MAX_SPILL";
    private static final String SPILL_DIR_PROPERTY = "parquetry.io.spillDir";
    private static final String SPILL_DIR_ENV = "PARQUETRY_IO_SPILL_DIR";

    private final long memoryOverride;
    private final long diskOverride;
    private final Path spillDir;

    private DefaultResourceLimits(long memoryOverride, long diskOverride, Path spillDir) {
        this.memoryOverride = memoryOverride;
        this.diskOverride = diskOverride;
        this.spillDir = spillDir;
    }

    static DefaultResourceLimits create() {
        long memory = resolveSizeBytes(System.getProperty(MEMORY_PROPERTY), System.getenv(MEMORY_ENV), MEMORY_PROPERTY);
        long disk = resolveSizeBytes(System.getProperty(DISK_PROPERTY), System.getenv(DISK_ENV), DISK_PROPERTY);
        Path dir = resolveSpillDir(System.getProperty(SPILL_DIR_PROPERTY), System.getenv(SPILL_DIR_ENV));
        return new DefaultResourceLimits(memory, disk, dir);
    }

    @Override
    public long availableMemoryBytes() {
        if (memoryOverride > 0) {
            return memoryOverride;
        }
        return containerMemoryBytes();
    }

    @Override
    public long usableDiskBytes(Path dir) {
        if (diskOverride > 0) {
            return diskOverride;
        }
        try {
            return Math.max(1L, Files.getFileStore(dir).getUsableSpace());
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read usable disk for " + dir, e);
        }
    }

    @Override
    public int availableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

    @Override
    public Path spillDir() {
        return spillDir;
    }

    /**
     * The container memory limit, as the JVM already detects it. With container support on (the default), the platform
     * bean honors the cgroup v1/v2 limit and reports physical RAM when none is set; reading {@code /sys/fs/cgroup}
     * ourselves would only duplicate that, less correctly. Falls back to the heap basis on a JVM without the bean.
     */
    private static long containerMemoryBytes() {
        OperatingSystemMXBean operatingSystem = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        if (operatingSystem != null) {
            long total = operatingSystem.getTotalMemorySize();
            if (total > 0) {
                return total;
            }
        }
        return Runtime.getRuntime().maxMemory();
    }

    /**
     * Resolves an explicit byte-size override from its already-read property and environment values, the property
     * winning. A blank or absent override returns {@code 0}, the sentinel for "probe the machine instead"; a present
     * but unparseable or non-positive value fails naming {@code key}. Pure: the ambient reads happen in the caller.
     */
    static long resolveSizeBytes(String propertyValue, String envValue, String key) {
        String raw = propertyValue != null ? propertyValue : envValue;
        if (raw == null || raw.isBlank()) {
            return 0L;
        }
        return parsePositiveSize(key, raw.trim());
    }

    private static long parsePositiveSize(String property, String raw) {
        long size;
        try {
            size = parseSize(raw);
        } catch (NumberFormatException _) {
            throw invalidByteSize(property, raw);
        }
        if (size <= 0) {
            throw invalidByteSize(property, raw);
        }
        return size;
    }

    private static IllegalArgumentException invalidByteSize(String property, String raw) {
        return new IllegalArgumentException("Invalid byte size for " + property + ": '" + raw + "'");
    }

    private static long parseSize(String raw) {
        String lower = raw.toLowerCase(Locale.ROOT);
        long multiplier = 1L;
        if (lower.endsWith("k")) {
            multiplier = 1L << 10;
        } else if (lower.endsWith("m")) {
            multiplier = 1L << 20;
        } else if (lower.endsWith("g")) {
            multiplier = 1L << 30;
        }
        String digits = multiplier == 1L ? lower : lower.substring(0, lower.length() - 1);
        return Long.parseLong(digits.trim()) * multiplier;
    }

    /**
     * Resolves the spill directory from its already-read property and environment values, the property winning,
     * defaulting to the temp directory when neither is set. Pure: the ambient reads happen in the caller.
     */
    static Path resolveSpillDir(String propertyValue, String envValue) {
        String raw = propertyValue != null ? propertyValue : envValue;
        if (raw == null || raw.isBlank()) {
            return Path.of(System.getProperty("java.io.tmpdir"));
        }
        return Path.of(raw.trim());
    }
}
