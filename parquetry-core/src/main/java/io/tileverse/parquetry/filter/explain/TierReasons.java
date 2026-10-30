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
package io.tileverse.parquetry.filter.explain;

/** The {@link PruningDecision#reason()} reported on each row group by a tier turned off by the read. */
public final class TierReasons {

    /** The reason reported by {@code tier} when the read turns it off, for example "stats filter disabled". */
    public static String disabled(Tier tier) {
        return switch (tier) {
            case STATS -> "stats filter disabled";
            case SPATIAL -> "spatial filter disabled";
            case COLUMN_INDEX -> "column index filter disabled";
            case BLOOM_FILTER -> "bloom filter disabled";
            case RECORD_LEVEL -> "record-level filter disabled";
        };
    }

    private TierReasons() {}
}
