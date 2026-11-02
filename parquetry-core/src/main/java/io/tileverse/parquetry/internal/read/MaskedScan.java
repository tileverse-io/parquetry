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
package io.tileverse.parquetry.internal.read;

import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Immutable per-read inputs that switch {@link ParallelDecodeCoordinator} onto the masked scan decode path. They apply
 * to each row group of the read; the rows covered by each row group's masked walk come from the read's
 * {@link RowGroupPlans}.
 *
 * @param stages the conditions of the predicate in evaluation order
 * @param outputSchema the schema exposed by the emitted batches (the caller's projection)
 */
public record MaskedScan(
        @NonNull ScanStages stages, @NonNull ParquetSchema outputSchema) {}
