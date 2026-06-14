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
package io.tileverse.parquetry.record;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Flattens a repeated (list/map) leaf to its element values for {@link ParquetRecord#multiValue(ColumnPath)}.
 *
 * <p>The leaf path is the physical column path, including the synthetic LIST {@code list}/{@code element} and MAP
 * {@code key_value}/{@code key}/{@code value} levels the file stores. The schema tree classifies each segment: a
 * {@code repeated} group opens a repeated boundary at which the record holds a list or map; the values inside it are
 * read element by element and the remaining path is resolved on each element record.
 *
 * <p>A single-valued path (no repeated boundary) flattens to the singleton of {@link ParquetRecord#get(ColumnPath)}, or
 * to the empty list when that value is null. An empty, null, or absent repeated ancestor flattens to the empty list.
 */
final class MultiValues {

    private MultiValues() {}

    static List<Object> flatten(ParquetRecord rec, ColumnPath leafPath) {
        List<Object> out = new ArrayList<>();
        SchemaNode.Group root = rec.schema().root();
        collect(rec, root, leafPath, 0, out);
        return out;
    }

    /**
     * The size of the repeated leaf's universe INCLUDING null elements. {@link #flatten} drops nulls because callers
     * read element values; quantified ALL/ONE evaluation needs the full count, where a null is a present non-matching
     * member.
     */
    static int flattenSize(ParquetRecord rec, ColumnPath leafPath) {
        int[] count = {0};
        SchemaNode.Group root = rec.schema().root();
        countAll(rec, root, leafPath, 0, count);
        return count[0];
    }

    /**
     * Counts every element of the slice {@code [from, end)} of {@code path}, null elements included, mirroring the
     * descent in {@link #collect} but tallying rather than appending values.
     */
    private static void countAll(ParquetRecord rec, SchemaNode.Group group, ColumnPath path, int from, int[] count) {
        Boundary boundary = firstRepeatedBoundary(group, path, from);
        if (boundary == null) {
            countLeafValue(rec, subPath(path, from, path.numParts()), count);
            return;
        }
        ColumnPath containerPath = subPath(path, from, boundary.index());
        if (isMapWrapper(boundary.repeatedGroup())) {
            countFromMap(rec, containerPath, path, boundary.index(), count);
            return;
        }
        countFromList(rec, boundary.repeatedGroup(), containerPath, path, boundary.index(), count);
    }

    private static void countLeafValue(ParquetRecord rec, ColumnPath leafSubPath, int[] count) {
        if (rec.get(leafSubPath) != null) {
            count[0]++;
        }
    }

    private static void countFromList(
            ParquetRecord rec,
            SchemaNode.Group repeatedGroup,
            ColumnPath containerPath,
            ColumnPath path,
            int boundary,
            int[] count) {
        Object container = rec.get(containerPath);
        if (!(container instanceof List<?> elements)) {
            return;
        }
        int tailFrom = boundary + 2;
        SchemaNode.Group elementGroup = elementGroupOrNull(repeatedGroup);
        for (Object element : elements) {
            countListElement(element, elementGroup, path, tailFrom, count);
        }
    }

    private static void countListElement(
            Object element, SchemaNode.Group elementGroup, ColumnPath path, int tailFrom, int[] count) {
        boolean primitiveEntry = tailFrom >= path.numParts();
        if (primitiveEntry) {
            count[0]++;
            return;
        }
        if (element instanceof ParquetRecord rec) {
            countAll(rec, elementGroup, path, tailFrom, count);
        }
    }

    private static void countFromMap(
            ParquetRecord rec, ColumnPath containerPath, ColumnPath path, int boundary, int[] count) {
        Map<?, ?> map = rec.readMap(containerPath);
        if (map == null) {
            return;
        }
        String addressed = path.part(boundary + 1);
        boolean keys = "key".equals(addressed);
        count[0] += (keys ? map.keySet() : map.values()).size();
    }

    /**
     * Collects element values for the slice {@code [from, end)} of {@code path}, resolved within {@code group} on
     * {@code rec}. Plain struct segments are folded into a single multi-segment read; the first repeated boundary
     * splits the path: the list or map is read at the boundary, then each element re-enters this method for the tail.
     */
    private static void collect(
            ParquetRecord rec, SchemaNode.Group group, ColumnPath path, int from, List<Object> out) {
        Boundary boundary = firstRepeatedBoundary(group, path, from);
        if (boundary == null) {
            addLeafValue(rec, subPath(path, from, path.numParts()), out);
            return;
        }
        ColumnPath containerPath = subPath(path, from, boundary.index());
        if (isMapWrapper(boundary.repeatedGroup())) {
            collectFromMap(rec, containerPath, path, boundary.index(), out);
            return;
        }
        collectFromList(rec, boundary.repeatedGroup(), containerPath, path, boundary.index(), out);
    }

    /** The first repeated group along a path: the index of its {@code repeated} segment and the group node itself. */
    private record Boundary(int index, SchemaNode.Group repeatedGroup) {}

    private static void addLeafValue(ParquetRecord rec, ColumnPath leafSubPath, List<Object> out) {
        Object value = rec.get(leafSubPath);
        if (value != null) {
            out.add(value);
        }
    }

    /**
     * Descends a LIST boundary: reads the list at {@code containerPath}, then resolves the tail (the path beyond the
     * synthetic {@code list} and {@code element} levels) on each non-null element. An element with no remaining tail is
     * a primitive list entry collected directly; an element with a tail is a struct record the remaining path resolves
     * on.
     */
    private static void collectFromList(
            ParquetRecord rec,
            SchemaNode.Group repeatedGroup,
            ColumnPath containerPath,
            ColumnPath path,
            int boundary,
            List<Object> out) {
        Object container = rec.get(containerPath);
        if (!(container instanceof List<?> elements)) {
            return;
        }
        int tailFrom = boundary + 2;
        SchemaNode.Group elementGroup = elementGroupOrNull(repeatedGroup);
        for (Object element : elements) {
            collectListElement(element, elementGroup, path, tailFrom, out);
        }
    }

    private static void collectListElement(
            Object element, SchemaNode.Group elementGroup, ColumnPath path, int tailFrom, List<Object> out) {
        if (element == null) {
            return;
        }
        if (tailFrom >= path.numParts()) {
            out.add(element);
            return;
        }
        if (element instanceof ParquetRecord rec) {
            collect(rec, elementGroup, path, tailFrom, out);
        }
    }

    /**
     * Descends a MAP boundary: reads the map at {@code containerPath}, then collects from its keys or values per the
     * segment after the synthetic {@code key_value} level.
     *
     * <p>Scope: keys and values are collected directly and not descended into. A path targeting a struct field inside a
     * map value (e.g. {@code tags.key_value.value.code}) is not handled here yet; only LIST is fully descended.
     */
    private static void collectFromMap(
            ParquetRecord rec, ColumnPath containerPath, ColumnPath path, int boundary, List<Object> out) {
        Map<?, ?> map = rec.readMap(containerPath);
        if (map == null) {
            return;
        }
        String addressed = path.part(boundary + 1);
        boolean keys = "key".equals(addressed);
        for (Object entryValue : keys ? map.keySet() : map.values()) {
            if (entryValue != null) {
                out.add(entryValue);
            }
        }
    }

    /** The first {@code repeated} group in {@code path} at or after {@code from}, or {@code null} when none remains. */
    private static Boundary firstRepeatedBoundary(SchemaNode.Group group, ColumnPath path, int from) {
        SchemaNode.Group current = group;
        for (int i = from; i < path.numParts(); i++) {
            SchemaNode child = childNamed(current, path.part(i));
            if (child == null) {
                return null;
            }
            if (child instanceof SchemaNode.Group childGroup && child.repetition() == Repetition.REPEATED) {
                return new Boundary(i, childGroup);
            }
            if (child instanceof SchemaNode.Group childGroup) {
                current = childGroup;
            }
        }
        return null;
    }

    private static boolean isMapWrapper(SchemaNode.Group repeatedGroup) {
        for (SchemaNode child : repeatedGroup.children()) {
            String name = child.name();
            if ("key".equals(name) || "value".equals(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The element struct group inside a LIST {@code repeated} wrapper, or {@code null} for a primitive-element list.
     */
    private static SchemaNode.Group elementGroupOrNull(SchemaNode.Group repeatedGroup) {
        if (repeatedGroup.children().size() == 1
                && repeatedGroup.children().get(0) instanceof SchemaNode.Group elementGroup) {
            return elementGroup;
        }
        return null;
    }

    private static SchemaNode childNamed(SchemaNode.Group group, String name) {
        for (SchemaNode child : group.children()) {
            if (child.name().equals(name)) {
                return child;
            }
        }
        return null;
    }

    private static ColumnPath subPath(ColumnPath path, int from, int toExclusive) {
        List<String> parts = new ArrayList<>(toExclusive - from);
        for (int i = from; i < toExclusive; i++) {
            parts.add(path.part(i));
        }
        return ColumnPath.of(parts);
    }
}
