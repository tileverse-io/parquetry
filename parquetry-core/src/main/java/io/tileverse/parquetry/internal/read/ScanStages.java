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
import java.util.HashSet;
import java.util.List;
import java.util.SequencedMap;
import java.util.Set;
import java.util.TreeMap;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * The conditions of a masked scan in evaluation order. Each stage evaluates the rows left by the stages before it, and
 * the conditions cheapest to read come first. The file schema alone sets the order, and it holds for the whole read.
 *
 * <p>A condition is one child of an {@link Predicate.And}. The conditions split into up to three stages, and a stage
 * without conditions drops out:
 *
 * <ol>
 *   <li>Conditions reading fixed-width leaves only.
 *   <li>Conditions reading a {@code BYTE_ARRAY} leaf.
 *   <li>The {@link Predicate.Spatial} and {@link Predicate.GeometryFilterPredicate} tests, and the {@code Not} of each.
 * </ol>
 *
 * <p>A condition reading a row position or a repeated leaf joins the first stage. Positions exist only in the batch of
 * the first stage, and the rows of a repeated leaf are known only from its decoded levels. The first stage reads at
 * least one physical leaf. When its conditions read none, the stage after it merges with it.
 *
 * <p>An {@code Or} holding a geometry test takes its stage from its leaves, like any condition other than a geometry
 * test. A predicate other than an {@code And} is one stage.
 *
 * @param stages the stages in evaluation order
 */
public record ScanStages(@NonNull List<Stage> stages) {

    public ScanStages {
        stages = List.copyOf(stages);
    }

    /**
     * The conditions evaluated together over one batch.
     *
     * @param predicate the conditions of the stage
     * @param leaves the physical leaves read by the conditions
     * @param ownLeaves the leaves first read by this stage; the first stage owns all its leaves
     * @param schema the file schema projected to {@code leaves}, the shape of the stage's batch
     */
    public record Stage(
            @NonNull Predicate predicate,
            @NonNull Set<ColumnPath> leaves,
            @NonNull Set<ColumnPath> ownLeaves,
            @NonNull ParquetSchema schema) {

        public Stage {
            leaves = Set.copyOf(leaves);
            ownLeaves = Set.copyOf(ownLeaves);
        }
    }

    /**
     * Splits {@code normalized} into its stages. A single stage keeps {@code normalized} and {@code filterLeaves} as
     * given.
     *
     * @param filterLeaves the physical leaves of the scan, the stand-in leaf of a synthesized-only predicate included
     */
    public static ScanStages of(Predicate normalized, Set<ColumnPath> filterLeaves, ParquetSchema fileSchema) {
        List<List<Predicate>> conditionsByStage = splitConditions(normalized, filterLeaves, fileSchema);
        if (conditionsByStage.size() == 1) {
            Stage whole = new Stage(normalized, filterLeaves, filterLeaves, fileSchema.project(filterLeaves));
            return new ScanStages(List.of(whole));
        }
        return new ScanStages(stagesOf(conditionsByStage, filterLeaves, fileSchema));
    }

    private static List<List<Predicate>> splitConditions(
            Predicate normalized, Set<ColumnPath> filterLeaves, ParquetSchema fileSchema) {
        List<List<Predicate>> conditionsByStage = groupByPlace(normalized, fileSchema);
        boolean firstReadsNoLeaf =
                leavesOf(conditionsByStage.getFirst(), filterLeaves).isEmpty();
        if (firstReadsNoLeaf && conditionsByStage.size() > 1) {
            mergeFirstWithNext(conditionsByStage);
        }
        return conditionsByStage;
    }

    private static List<List<Predicate>> groupByPlace(Predicate normalized, ParquetSchema fileSchema) {
        SequencedMap<Place, List<Predicate>> conditionsByPlace = new TreeMap<>();
        for (Predicate condition : conditionsOf(normalized)) {
            Place place = placeOf(condition, fileSchema);
            conditionsByPlace.computeIfAbsent(place, _ -> new ArrayList<>()).add(condition);
        }
        return new ArrayList<>(conditionsByPlace.values());
    }

    private static List<Predicate> conditionsOf(Predicate normalized) {
        return normalized instanceof Predicate.And(List<Predicate> children) ? children : List.of(normalized);
    }

    /** The place of {@code condition}. A row position or a repeated leaf decides before the kind of the condition. */
    private static Place placeOf(Predicate condition, ParquetSchema fileSchema) {
        Set<ColumnPath> columns = Predicate.columns(condition);
        if (readsRowPosition(condition) || readsRepeatedLeaf(columns, fileSchema)) {
            return Place.FIRST;
        }
        if (isGeometryTest(condition)) {
            return Place.LAST;
        }
        return readsByteArray(columns, fileSchema) ? Place.SECOND : Place.FIRST;
    }

    private static boolean readsRowPosition(Predicate condition) {
        return !Predicate.rowPositionColumns(condition).isEmpty();
    }

    private static boolean readsRepeatedLeaf(Set<ColumnPath> columns, ParquetSchema fileSchema) {
        return columns.stream().anyMatch(column -> fileSchema.maxLevels(column).maxRepetitionLevel() > 0);
    }

    private static boolean isGeometryTest(Predicate condition) {
        return switch (condition) {
            case Predicate.Spatial _ -> true;
            case Predicate.GeometryFilterPredicate _ -> true;
            case Predicate.Not(Predicate.Spatial _) -> true;
            case Predicate.Not(Predicate.GeometryFilterPredicate _) -> true;
            default -> false;
        };
    }

    private static boolean readsByteArray(Set<ColumnPath> columns, ParquetSchema fileSchema) {
        return columns.stream().anyMatch(column -> isByteArray(column, fileSchema));
    }

    private static boolean isByteArray(ColumnPath column, ParquetSchema fileSchema) {
        SchemaNode node = fileSchema.find(column).orElse(null);
        return node instanceof SchemaNode.Primitive leaf && leaf.kind() == PrimitiveKind.BYTE_ARRAY;
    }

    private static void mergeFirstWithNext(List<List<Predicate>> conditionsByStage) {
        List<Predicate> first = conditionsByStage.removeFirst();
        conditionsByStage.getFirst().addAll(0, first);
    }

    private static List<Stage> stagesOf(
            List<List<Predicate>> conditionsByStage, Set<ColumnPath> filterLeaves, ParquetSchema fileSchema) {
        List<Stage> stages = new ArrayList<>(conditionsByStage.size());
        Set<ColumnPath> readEarlier = new HashSet<>();
        for (List<Predicate> conditions : conditionsByStage) {
            Set<ColumnPath> leaves = leavesOf(conditions, filterLeaves);
            Set<ColumnPath> ownLeaves = new HashSet<>(leaves);
            ownLeaves.removeAll(readEarlier);
            readEarlier.addAll(leaves);
            stages.add(new Stage(predicateOf(conditions), leaves, ownLeaves, fileSchema.project(leaves)));
        }
        return stages;
    }

    /** The physical leaves read by {@code conditions}. A column outside {@code filterLeaves} is a row position. */
    private static Set<ColumnPath> leavesOf(List<Predicate> conditions, Set<ColumnPath> filterLeaves) {
        Set<ColumnPath> leaves = new HashSet<>();
        for (Predicate condition : conditions) {
            leaves.addAll(Predicate.columns(condition));
        }
        leaves.retainAll(filterLeaves);
        return leaves;
    }

    private static Predicate predicateOf(List<Predicate> conditions) {
        return conditions.size() == 1 ? conditions.getFirst() : new Predicate.And(conditions);
    }

    /** The places of a condition, in evaluation order. */
    private enum Place {
        FIRST,
        SECOND,
        LAST
    }
}
