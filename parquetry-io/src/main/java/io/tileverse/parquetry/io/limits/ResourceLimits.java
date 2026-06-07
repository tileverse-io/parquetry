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
package io.tileverse.parquetry.io.limits;

import java.nio.file.Path;

/**
 * The raw machine facts the read path sizes its native and disk budgets from: how much memory and disk the container
 * permits, how many processors it has, and where to spill. This is an injectable port - the default reads the running
 * container, a test supplies a {@link #fixed fixed} instance for a deterministic bound independent of the host. Sizing
 * policy (the fractions applied to these facts) lives where the caps are derived, not here.
 */
public interface ResourceLimits {

    /** Native (off-heap) memory the container permits, in bytes. */
    long availableMemoryBytes();

    /** Usable disk on the file store backing {@code dir}, in bytes. */
    long usableDiskBytes(Path dir);

    /** Processor count available to this process. */
    int availableProcessors();

    /** The directory spill files are written under. */
    Path spillDir();

    /** The process-wide default: reads the container limits and honors env/system-property overrides. */
    static ResourceLimits getDefault() {
        return DefaultResourceLimits.INSTANCE;
    }

    /** A fixed instance for tests; {@code usableDiskBytes} ignores its argument and returns {@code disk}. */
    static ResourceLimits fixed(long memory, long disk, int processors, Path spillDir) {
        return new ResourceLimits() {
            @Override
            public long availableMemoryBytes() {
                return memory;
            }

            @Override
            public long usableDiskBytes(Path dir) {
                return disk;
            }

            @Override
            public int availableProcessors() {
                return processors;
            }

            @Override
            public Path spillDir() {
                return spillDir;
            }
        };
    }
}
