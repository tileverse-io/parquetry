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
package io.tileverse.parquetry.filter.explain;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Plan-derived view of which pruning tiers were turned off, used to render the tier-configuration header. A tier counts
 * as off when every decision it produced across the file's row groups is a {@link PruningDecision.NotApplied} whose
 * reason equals the tier's disabled literal from {@link TierReasons}. A tier with no decisions anywhere counts as on,
 * since "off" is a positive claim the plan must show evidence for.
 */
final class TierConfiguration {

    static final List<Tier> ORDER =
            List.of(Tier.STATS, Tier.SPATIAL, Tier.DICTIONARY, Tier.COLUMN_INDEX, Tier.BLOOM_FILTER);

    private static final Map<Tier, String> DISABLED_REASONS = disabledReasons();

    private final Map<Tier, Boolean> offByTier;

    private TierConfiguration(Map<Tier, Boolean> offByTier) {
        this.offByTier = offByTier;
    }

    static TierConfiguration from(ExplainPlan plan) {
        Map<Tier, Boolean> offByTier = new EnumMap<>(Tier.class);
        for (Tier tier : ORDER) {
            offByTier.put(tier, isTurnedOff(tier, plan));
        }
        return new TierConfiguration(offByTier);
    }

    boolean isOff(Tier tier) {
        return offByTier.getOrDefault(tier, Boolean.FALSE);
    }

    private static boolean isTurnedOff(Tier tier, ExplainPlan plan) {
        String disabledReason = DISABLED_REASONS.get(tier);
        if (disabledReason == null) {
            return false;
        }
        boolean sawDecision = false;
        for (RowGroupPlan rowGroup : plan.rowGroups()) {
            for (PruningDecision decision : rowGroup.tiers()) {
                if (decision.tier() != tier) {
                    continue;
                }
                sawDecision = true;
                if (!matchesDisabled(decision, disabledReason)) {
                    return false;
                }
            }
        }
        return sawDecision;
    }

    private static boolean matchesDisabled(PruningDecision decision, String disabledReason) {
        return decision instanceof PruningDecision.NotApplied notApplied && disabledReason.equals(notApplied.reason());
    }

    private static Map<Tier, String> disabledReasons() {
        Map<Tier, String> reasons = new EnumMap<>(Tier.class);
        reasons.put(Tier.STATS, TierReasons.STATS_FILTER_DISABLED);
        reasons.put(Tier.DICTIONARY, TierReasons.DICTIONARY_FILTER_DISABLED);
        return reasons;
    }
}
