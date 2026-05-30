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
/**
 * parquetry's read-IO SPIs, covering both positional reads and file discovery, with no domain knowledge.
 *
 * <p>{@link io.tileverse.parquetry.io.ByteRangeSource} is the single positional read dependency of the format and core
 * modules. It ships with a pure-JDK default, keeping the reader free of any third-party runtime dependency. Cloud and
 * shared-pool sources are provided by separate adapter modules.
 *
 * <p>{@link io.tileverse.parquetry.io.FileSource} discovers the files under a root, each openable as a
 * {@link io.tileverse.parquetry.io.ByteRangeSource}. A pure-JDK {@link io.tileverse.parquetry.io.LocalFileSource}
 * covers local directories and single files; Storage-backed sources are provided by separate adapter modules.
 */
package io.tileverse.parquetry.io;
