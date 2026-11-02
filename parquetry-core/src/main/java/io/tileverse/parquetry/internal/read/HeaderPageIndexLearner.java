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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.filter.ColumnIndexEvaluator;
import io.tileverse.parquetry.internal.filter.FilterPipeline.PageIndexLookup;
import io.tileverse.parquetry.internal.filter.LearnedPages;
import io.tileverse.parquetry.internal.filter.PageIndex;
import io.tileverse.parquetry.internal.read.page.PageHeaderScan;
import io.tileverse.parquetry.internal.read.page.PageSelection;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

import lombok.NonNull;

/**
 * The learner of a filtered read over a file without a page index. It learns the pages of a row group from the headers
 * of its chunks, and it keeps them only when the predicate of the read drops a page with them.
 *
 * <p>The entries of a row group are kept together or not at all. A row mask needs the pages of each scanned column, and
 * one missing entry would leave the row group without a mask. The chunks must all be of flat columns, and the learner
 * checks their columns before it parses a header. The headers of each chunk must then give an entry passing the
 * validation of {@link PageIndex#learned}. The prunable part of the predicate is evaluated on the entries last. They
 * are kept when it eliminates the row group, or when it leaves a page of some chunk without a surviving row.
 *
 * <p>Fetch threads call {@link #learn}, several at once for the prefetched row groups of a read. The learner holds no
 * mutable state. It reads the schema and the footer through the chunk view of a row group, and nothing memoized by that
 * view. The entries are keyed by the leaf ordinals of the chunk view, as the lookups of later reads are.
 */
public final class HeaderPageIndexLearner implements PageIndexLearner {

    private final Predicate prunablePart;
    private final BitSet learnableRowGroups;
    private final LearnedPageIndexes sink;

    /**
     * @param prunablePart the part of the predicate of the read evaluable by the page tier
     * @param learnableRowGroups the ordinals of the row groups allowed to the learner. A later change to the set does
     *     not reach the learner.
     * @param sink the page indexes of the file, receiving the kept entries
     */
    public HeaderPageIndexLearner(
            @NonNull Predicate prunablePart, @NonNull BitSet learnableRowGroups, @NonNull LearnedPageIndexes sink) {
        this.prunablePart = prunablePart;
        this.learnableRowGroups = (BitSet) learnableRowGroups.clone();
        this.sink = sink;
    }

    @Override
    public boolean learns(int rowGroup) {
        return learnableRowGroups.get(rowGroup);
    }

    @Override
    public void learn(@NonNull RowGroupSurvivor survivor, @NonNull List<WholeChunk> chunks) {
        if (!learns(survivor.index())) {
            return;
        }
        try {
            keepEntriesIfAPageDrops(survivor, chunks);
        } catch (RuntimeException _) {
            // A failure ends the pass and leaves the row group unlearned, and the read goes on. The decode reads the
            // same pages, and it reports what is wrong with them.
        }
    }

    private void keepEntriesIfAPageDrops(RowGroupSurvivor survivor, List<WholeChunk> chunks) {
        Optional<List<Candidate>> candidates = candidatesOf(survivor, chunks);
        if (candidates.isEmpty()) {
            return;
        }
        List<Candidate> learned = candidates.orElseThrow();
        if (dropsAPage(learned, survivor.numRows())) {
            keep(survivor.index(), learned);
        }
    }

    /**
     * The candidate entry of each chunk, or empty when one chunk gives none. The columns of all the chunks are checked
     * before the headers of any chunk are parsed.
     */
    private static Optional<List<Candidate>> candidatesOf(RowGroupSurvivor survivor, List<WholeChunk> chunks) {
        RowGroupChunks view = survivor.chunks();
        long rowCount = survivor.numRows();
        Optional<List<FlatChunk>> flatChunks = eachOrNone(chunks, chunk -> flatChunkOf(view, chunk));
        if (flatChunks.isEmpty()) {
            return Optional.empty();
        }
        return eachOrNone(flatChunks.orElseThrow(), chunk -> candidateOf(chunk, rowCount));
    }

    /** {@code mapping} applied to each of {@code items}, or empty when it gives nothing for one of them. */
    private static <T, R> Optional<List<R>> eachOrNone(List<T> items, Function<T, Optional<R>> mapping) {
        List<R> mapped = new ArrayList<>(items.size());
        for (T item : items) {
            Optional<R> result = mapping.apply(item);
            if (result.isEmpty()) {
                return Optional.empty();
            }
            mapped.add(result.orElseThrow());
        }
        return Optional.of(mapped);
    }

    /**
     * {@code chunk} with the type and the leaf ordinal of its column, or empty for a column of no primitive type and
     * for a repeated column. The kind is checked first. A path missing from the schema has no kind, and asking its
     * flatness would throw. The type comes from the schema of the chunk view. The entry does not store it, and a later
     * read decodes the entry with the type given by that schema.
     */
    private static Optional<FlatChunk> flatChunkOf(RowGroupChunks view, WholeChunk chunk) {
        ColumnPath path = chunk.path();
        Optional<PrimitiveKind> kind = view.primitiveKind(path);
        if (kind.isEmpty() || !view.isFlat(path)) {
            return Optional.empty();
        }
        Optional<LogicalType> logicalType = view.logicalType(path);
        return Optional.of(new FlatChunk(chunk, view.ordinalOf(path), kind.orElseThrow(), logicalType));
    }

    /**
     * The entry parsed from the page headers of {@code chunk} with its page view. It is empty when the headers do not
     * describe the chunk, and when the pages fail the validation of {@link PageIndex#learned}.
     */
    private static Optional<Candidate> candidateOf(FlatChunk chunk, long rowCount) {
        WholeChunk whole = chunk.whole();
        PrimitiveKind kind = chunk.kind();
        Optional<LogicalType> logicalType = chunk.logicalType();
        Optional<LearnedPages> scanned = PageHeaderScan.scan(whole.bytes(), rowCount, kind, logicalType);
        if (scanned.isEmpty()) {
            return Optional.empty();
        }
        LearnedPages pages = scanned.orElseThrow();
        Optional<PageIndex> view = PageIndex.learned(pages, whole.meta(), rowCount, kind, logicalType);
        return view.map(validated -> new Candidate(whole.path(), chunk.column(), pages, validated));
    }

    /**
     * Whether the prunable part drops a page of a candidate. It drops one when it eliminates the row group, and when it
     * narrows the rows of the row group and leaves a page of some candidate without a surviving row. A narrowing alone
     * may keep each page, because the rows of a page dropped by one column may survive through another.
     */
    private boolean dropsAPage(List<Candidate> candidates, long rowCount) {
        PageIndexLookup lookup = lookupOver(candidates);
        PruningDecision decision = ColumnIndexEvaluator.evaluate(prunablePart, lookup, rowCount);
        return switch (decision) {
            case PruningDecision.Eliminated _ -> true;
            case PruningDecision.NarrowedTo narrowed -> leavesAPageWithoutRows(candidates, narrowed.ranges());
            case PruningDecision.PassedAll _, PruningDecision.Inconclusive _, PruningDecision.NotApplied _ -> false;
        };
    }

    /**
     * The page views of the candidates, by column. The views are in memory, and the warm of the lookup has nothing to
     * read ahead.
     */
    private static PageIndexLookup lookupOver(List<Candidate> candidates) {
        Map<ColumnPath, PageIndex> views = HashMap.newHashMap(candidates.size());
        for (Candidate candidate : candidates) {
            views.put(candidate.path(), candidate.view());
        }
        return path -> Optional.ofNullable(views.get(path));
    }

    private static boolean leavesAPageWithoutRows(List<Candidate> candidates, RowRanges survivingRows) {
        for (Candidate candidate : candidates) {
            PageSelection selection = PageSelection.forColumn(candidate.view(), survivingRows);
            if (selection.survivingPageCount() < selection.pageCount()) {
                return true;
            }
        }
        return false;
    }

    /** Hands the entries of row group {@code rowGroup} to the sink. An entry already stored for a chunk stays. */
    private void keep(int rowGroup, List<Candidate> candidates) {
        for (Candidate candidate : candidates) {
            sink.putIfAbsent(rowGroup, candidate.column(), candidate.pages());
        }
    }

    /**
     * A chunk of a flat column, with the type and the leaf ordinal of its column.
     *
     * @param whole the chunk, fetched whole
     * @param column the leaf ordinal of the column in the chunk view
     * @param kind the physical type of the column
     * @param logicalType the logical type of the column in the schema of the chunk view
     */
    private record FlatChunk(
            @NonNull WholeChunk whole,
            int column,
            @NonNull PrimitiveKind kind,
            @NonNull Optional<LogicalType> logicalType) {}

    /**
     * The entry learned for one chunk, with its page view.
     *
     * @param path the leaf column of the chunk
     * @param column the leaf ordinal of the column, keying the entry in the sink
     * @param pages the entry, as the sink keeps it
     * @param view the pages of the entry, validated against the chunk
     */
    private record Candidate(
            @NonNull ColumnPath path,
            int column,
            @NonNull LearnedPages pages,
            @NonNull PageIndex view) {}
}
