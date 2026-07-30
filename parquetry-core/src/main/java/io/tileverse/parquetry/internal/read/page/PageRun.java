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
package io.tileverse.parquetry.internal.read.page;

import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.format.PageLocation;

/**
 * A maximal byte-contiguous stretch of surviving data pages within one column chunk: the absolute file byte range to
 * fetch, plus the data-page ordinal of the run's first page, which seeds {@link PageCursor} row alignment when the
 * chunk arrives as separate runs instead of one contiguous segment.
 */
public record PageRun(long fileOffset, int length, int firstPageOrdinal) {

    public PageRun {
        if (fileOffset < 0) {
            throw new IllegalArgumentException("fileOffset must be >= 0, got " + fileOffset);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be > 0, got " + length);
        }
        if (firstPageOrdinal < 0) {
            throw new IllegalArgumentException("firstPageOrdinal must be >= 0, got " + firstPageOrdinal);
        }
    }

    /**
     * Computes the surviving-page runs for one column. Surviving pages join the same run only while their byte ranges
     * abut; a writer that leaves bytes between consecutive pages splits the run, which stays correct (every surviving
     * page's bytes are still covered, just across more ranges).
     */
    public static List<PageRun> runsFor(PageSelection selection, List<PageLocation> pageLocations) {
        List<PageRun> runs = new ArrayList<>();
        long runStart = -1;
        long runEnd = -1;
        int runFirstOrdinal = -1;
        for (int ordinal = 0; ordinal < pageLocations.size(); ordinal++) {
            if (!selection.isSurviving(ordinal)) {
                continue;
            }
            PageLocation location = pageLocations.get(ordinal);
            long pageStart = location.offset();
            long pageEnd = pageStart + location.compressedPageSize();
            if (runStart >= 0 && pageStart == runEnd) {
                runEnd = pageEnd;
                continue;
            }
            if (runStart >= 0) {
                runs.add(new PageRun(runStart, Math.toIntExact(runEnd - runStart), runFirstOrdinal));
            }
            runStart = pageStart;
            runEnd = pageEnd;
            runFirstOrdinal = ordinal;
        }
        if (runStart >= 0) {
            runs.add(new PageRun(runStart, Math.toIntExact(runEnd - runStart), runFirstOrdinal));
        }
        return runs;
    }
}
