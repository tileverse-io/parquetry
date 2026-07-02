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
package io.tileverse.parquetry.internal.read;

/**
 * Decides, in strict file order on the consumer thread, whether the decode coordinator may drop the survivor row group
 * at a consume position before fetching or decoding it. The decision is delegated to the read's spatial probe against
 * the group's geometry bounds; the coordinator stays unaware of geometry. A dropped group performs no fetch and no
 * decode. Dropping is monotone with respect to the probe's accumulating paint state (a group is dropped only when its
 * whole bounds fall in already-covered space), and an over-decode under speculation stays correct.
 */
@FunctionalInterface
public interface RowGroupGate {

    /** Whether the survivor row group at {@code consumePosition} (0-based, file order) may be skipped untouched. */
    boolean skip(int consumePosition);
}
