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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.RowRanges;

import lombok.NonNull;

/**
 * The per-row-group plans of one read, memoized by survivor position: the single source read by the prefetcher and the
 * decode coordinator for whether a row group is dropped, which mask its fetch and its decode use, and how many rows its
 * masked scan walks. Without a planner the plans are the filter pipeline's masks and nothing is dropped.
 *
 * <p>Confined to the consumer thread, like the chunk views of the survivors that it plans over. The planner is asked at
 * most once per row group, on the first question about that position, which is the point where the row group's fetch
 * plan is built.
 */
public final class RowGroupPlans {

    /** Builds the mask that narrows a survivor to the row ranges decided by its plan. */
    @FunctionalInterface
    public interface NarrowedMaskFactory {

        /** The mask narrowing {@code survivor} to {@code rows}, or empty when its scanned leaves cannot be masked. */
        Optional<RowMask> maskFor(RowGroupSurvivor survivor, RowRanges rows);
    }

    /** The memo of a plans object with no planner, which never asks a position for its narrowing. */
    private static final RowGroupNarrowing[] NO_NARROWINGS = new RowGroupNarrowing[0];

    private final List<RowGroupSurvivor> survivors;
    private final List<Optional<RowMask>> pipelineMasks;
    private final boolean pageNarrowedFetch;
    private final RowGroupPlanner planner;
    private final NarrowedMaskFactory maskFactory;

    /** The narrowing memo, one slot per survivor; a null slot means the planner has not been asked yet. */
    private final RowGroupNarrowing[] narrowings;

    /** The decode-mask memo, pre-filled with the pipeline masks; {@link #maskResolved} says which slots are final. */
    private final List<Optional<RowMask>> decodeMasks;

    private final boolean[] maskResolved;

    private RowGroupPlans(
            List<RowGroupSurvivor> survivors,
            List<Optional<RowMask>> pipelineMasks,
            boolean pageNarrowedFetch,
            RowGroupPlanner planner,
            NarrowedMaskFactory maskFactory) {
        if (pipelineMasks.size() != survivors.size()) {
            throw new IllegalArgumentException("Expected one pipeline mask per survivor, got " + pipelineMasks.size()
                    + " masks for " + survivors.size() + " survivors");
        }
        this.survivors = List.copyOf(survivors);
        this.pipelineMasks = List.copyOf(pipelineMasks);
        this.pageNarrowedFetch = pageNarrowedFetch;
        this.planner = planner;
        this.maskFactory = maskFactory;
        this.narrowings = planner == null ? NO_NARROWINGS : new RowGroupNarrowing[survivors.size()];
        this.decodeMasks = new ArrayList<>(this.pipelineMasks);
        this.maskResolved = new boolean[survivors.size()];
    }

    /** Plans that read every survivor with the filter pipeline's own mask and drop nothing. */
    public static RowGroupPlans unplanned(
            @NonNull List<RowGroupSurvivor> survivors,
            @NonNull List<Optional<RowMask>> pipelineMasks,
            boolean pageNarrowedFetch) {
        return new RowGroupPlans(survivors, pipelineMasks, pageNarrowedFetch, null, null);
    }

    /** Plans that ask {@code planner} once per survivor and build a narrowed mask through {@code maskFactory}. */
    public static RowGroupPlans planned(
            @NonNull List<RowGroupSurvivor> survivors,
            @NonNull List<Optional<RowMask>> pipelineMasks,
            boolean pageNarrowedFetch,
            @NonNull RowGroupPlanner planner,
            @NonNull NarrowedMaskFactory maskFactory) {
        return new RowGroupPlans(survivors, pipelineMasks, pageNarrowedFetch, planner, maskFactory);
    }

    public int size() {
        return survivors.size();
    }

    /** Whether the survivor at {@code position} performs no fetch and no decode. */
    public boolean dropped(int position) {
        return narrowing(position) instanceof RowGroupNarrowing.Dropped;
    }

    /**
     * The mask received by the survivor's column readers: the plan's narrowing when it has one, else the pipeline's.
     */
    public Optional<RowMask> decodeMask(int position) {
        if (!maskResolved[position]) {
            decodeMasks.set(position, resolveDecodeMask(position));
            maskResolved[position] = true;
        }
        return decodeMasks.get(position);
    }

    /** The mask used by the fetch: the decode mask when page-narrowed fetch is on, else none (whole chunks). */
    public Optional<RowMask> fetchMask(int position) {
        if (!pageNarrowedFetch) {
            return Optional.empty();
        }
        return decodeMask(position);
    }

    /** The rows walked by a masked scan of the survivor: the decode mask's surviving rows, else the survivor's rows. */
    public long rowsToScan(int position) {
        Optional<RowMask> mask = decodeMask(position);
        if (mask.isPresent()) {
            return mask.orElseThrow().survivingRows().totalRows();
        }
        return survivors.get(position).numRows();
    }

    private RowGroupNarrowing narrowing(int position) {
        if (planner == null) {
            return RowGroupNarrowing.whole();
        }
        RowGroupNarrowing memo = narrowings[position];
        if (memo == null) {
            memo = planner.plan(position);
            narrowings[position] = memo;
        }
        return memo;
    }

    private Optional<RowMask> resolveDecodeMask(int position) {
        Optional<RowMask> pipelineMask = pipelineMasks.get(position);
        if (!(narrowing(position) instanceof RowGroupNarrowing.Kept(Optional<RowRanges> rows)) || rows.isEmpty()) {
            return pipelineMask;
        }
        Optional<RowMask> narrowed = maskFactory.maskFor(survivors.get(position), rows.orElseThrow());
        if (narrowed.isPresent()) {
            return narrowed;
        }
        return pipelineMask;
    }
}
