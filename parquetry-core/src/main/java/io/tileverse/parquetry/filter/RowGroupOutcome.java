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
package io.tileverse.parquetry.filter;

/**
 * The end-of-pipeline outcome for a single row group:
 *
 * <ul>
 *   <li>{@link #ELIMINATED} - no I/O for this row group; some tier proved no row can match.
 *   <li>{@link #PARTIAL} - a strict subset of rows survives; record-level evaluation still runs.
 *   <li>{@link #FULL} - every row survives the a-priori tiers; record-level evaluation still applies.
 *   <li>{@link #MATCHED} - statistics proved every row matches the predicate; no decode and no record-level evaluation
 *       are needed.
 * </ul>
 */
public enum RowGroupOutcome {
    ELIMINATED,
    PARTIAL,
    FULL,
    MATCHED
}
