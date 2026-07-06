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
package io.tileverse.parquetry.filter.explain;

/**
 * Shared {@link PruningDecision#reason()} literals for tiers that were turned off. The pipeline emits these when a tier
 * is disabled; the renderers match against them to mark a tier as off in the tier-configuration header. Keeping the
 * literals in one place lets both sides agree without fragile inline string comparisons.
 */
public final class TierReasons {

    /** Reason emitted when the STATS tier is turned off. */
    public static final String STATS_FILTER_DISABLED = "stats filter disabled";

    /** Reason emitted when the DICTIONARY tier is turned off. */
    public static final String DICTIONARY_FILTER_DISABLED = "dictionary filter disabled";

    private TierReasons() {}
}
