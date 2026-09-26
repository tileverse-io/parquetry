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

/**
 * Plans one survivor row group right before its fetch plan is built, on the consumer thread and in file order of
 * planning: from the group's geometry bounds and the read's spatial probe it decides whether the group is dropped
 * untouched or kept, and which of its row ranges are read. Called at most once per row group; the caller memoizes.
 */
@FunctionalInterface
public interface RowGroupPlanner {

    /** The narrowing of the survivor row group at {@code consumePosition}, zero-based in file order. */
    RowGroupNarrowing plan(int consumePosition);
}
