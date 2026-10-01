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
package io.tileverse.parquetry.internal.write;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.schema.ColumnPath;

import lombok.NonNull;

/**
 * The page value limit in force for each leaf of one write. A leaf named by {@link WriteOptions#pageValueLimits()}
 * takes that limit; a covering leaf takes {@link WriteOptions#coveringPageValueLimit()} when one is set; any other leaf
 * takes {@link WriteOptions#pageValueLimit()}.
 *
 * <p>Resolved once per file, because the covering leaves are known only after the covering of the write is planned.
 */
public final class PageValueLimits {

    private final int sharedLimit;
    private final Map<String, Integer> byPath;

    private PageValueLimits(int sharedLimit, Map<String, Integer> byPath) {
        this.sharedLimit = sharedLimit;
        this.byPath = byPath;
    }

    /** The limits of a write with no covering leaves to narrow. */
    public static PageValueLimits of(@NonNull WriteOptions options) {
        return of(options, List.of());
    }

    /**
     * The limits of a write whose covering is planned over {@code coveringLeaves}, empty when the write has no
     * covering.
     */
    public static PageValueLimits of(@NonNull WriteOptions options, @NonNull Collection<ColumnPath> coveringLeaves) {
        Map<String, Integer> byPath = new HashMap<>(options.pageValueLimits());
        OptionalInt coveringLimit = options.coveringPageValueLimit();
        if (coveringLimit.isPresent()) {
            for (ColumnPath leaf : coveringLeaves) {
                byPath.putIfAbsent(leaf.dot(), coveringLimit.getAsInt());
            }
        }
        return new PageValueLimits(options.pageValueLimit(), Map.copyOf(byPath));
    }

    /** The limit for {@code leaf}, falling back to the limit shared by all columns. */
    public int forLeaf(ColumnPath leaf) {
        return byPath.getOrDefault(leaf.dot(), sharedLimit);
    }
}
