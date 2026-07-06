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
package io.tileverse.parquetry.dataset;

import java.net.URI;
import java.util.Map;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * One file in a {@link FilePlan}: where it is, its row count, its statistics, its partition tuple, and how to open it.
 */
public interface PlannedFile {

    URI location();

    /** Row count from the manifest (Iceberg) or footer (pure-parquet); {@code -1} when unknown. */
    long recordCount();

    FileStatistics statistics();

    /** Partition column values for this file; empty when unpartitioned. */
    Map<String, Object> partitionValues();

    /** Opens a fresh byte source over this file; the caller owns and closes it. */
    ByteRangeSource open();
}
