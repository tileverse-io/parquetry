/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.read;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.read.ValueBuilder.ChildBinding;
import io.tileverse.parquetry.read.ValueBuilder.LeafValueBuilder;
import io.tileverse.parquetry.read.ValueBuilder.ListValueBuilder;
import io.tileverse.parquetry.read.ValueBuilder.MapValueBuilder;
import io.tileverse.parquetry.read.ValueBuilder.StructValueBuilder;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.Repetition;

/**
 * Dremel record assembler emitting one logical row at a time as a {@link RowAccessor}.
 *
 * <p>Built once per row-group reader. The constructor walks the projected schema and pairs each shape it finds with the
 * right {@link ValueBuilder} subtype:
 *
 * <ul>
 *   <li>{@link Field.Primitive} with no repeated ancestor: {@link LeafValueBuilder}.
 *   <li>{@link Field.Primitive} with {@code REPEATED} repetition: {@link ListValueBuilder} (legacy repeated primitive).
 *   <li>{@link Field.Group} annotated {@code LIST} (or legacy repeated group): {@link ListValueBuilder}.
 *   <li>{@link Field.Group} annotated {@code MAP}: {@link MapValueBuilder}.
 *   <li>{@link Field.Group} non-repeated, non-LIST/MAP: {@link StructValueBuilder} (handles the "optional group of
 *       scalars" shape and extends to arbitrary depth).
 * </ul>
 *
 * <p>{@link #next()} drives every root builder once. Each builder consumes whatever triples the row demands. Builders
 * that produce composite values store them at the topmost group's {@link ColumnPath} (e.g. {@code mylist} for a list,
 * {@code mymap} for a map), so consumers see the shape Parquet promised regardless of the leaf names underneath.
 */
final class RecordAssembler {

    private final List<RootBuilder> roots;
    private final List<ColumnReader> allReaders;

    RecordAssembler(ParquetSchema schema, List<ColumnReader> readers) {
        this.allReaders = List.copyOf(readers);
        Map<ColumnPath, ColumnReader> readersByPath = new HashMap<>();
        for (ColumnReader reader : readers) {
            readersByPath.put(reader.columnPath(), reader);
        }
        this.roots = buildRoots(schema, readersByPath);
    }

    boolean hasNext() {
        for (ColumnReader reader : allReaders) {
            if (reader.hasNext()) {
                return true;
            }
        }
        return false;
    }

    RowAccessor next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more rows to assemble");
        }
        MutableRowAccessor row = new MutableRowAccessor();
        for (RootBuilder root : roots) {
            Object value = root.builder.read();
            if (value != null) {
                row.values.put(root.path, value);
            }
        }
        return row;
    }

    // --- builder tree construction ---

    /**
     * Top-level roots are flattened: a non-repeated {@link Field.Group} that is <em>not</em> LIST/MAP-annotated has no
     * record-accessor representation of its own; instead its descendant leaves register one root each at their full
     * column paths. This preserves the contract that {@code record.get(ColumnPath.of("roll_num", "min"))} returns the
     * int directly. Sub-records only exist inside list / map elements (handled by {@link StructValueBuilder} when built
     * from {@code buildAnnotatedList} / {@code buildLegacyRepeatedGroup}).
     */
    private static List<RootBuilder> buildRoots(ParquetSchema schema, Map<ColumnPath, ColumnReader> readersByPath) {
        List<RootBuilder> built = new ArrayList<>();
        Context ctx = new Context(readersByPath, /*maxDef*/ 0, /*maxRep*/ 0);
        Field.Group root = schema.root();
        for (Field child : root.children()) {
            collectRoots(child, List.of(), ctx, built);
        }
        return built;
    }

    private static void collectRoots(Field field, List<String> ancestorPath, Context ctx, List<RootBuilder> out) {
        switch (field) {
            case Field.Primitive p -> {
                ValueBuilder builder = buildPrimitive(p, ancestorPath, ctx);
                if (builder != null) {
                    out.add(new RootBuilder(appendPath(ancestorPath, p.name()), builder));
                }
            }
            case Field.Group g -> collectGroupRoot(g, ancestorPath, ctx, out);
        }
    }

    private static void collectGroupRoot(Field.Group g, List<String> ancestorPath, Context ctx, List<RootBuilder> out) {
        Optional<LogicalType> annotation = g.logicalType();
        boolean isList = annotation.isPresent() && annotation.get() instanceof LogicalType.ListType;
        boolean isMap = annotation.isPresent() && annotation.get() instanceof LogicalType.MapType;
        boolean isRepeated = g.repetition() == Repetition.REPEATED;
        // LIST/MAP-annotated and legacy repeated groups become single roots that hold a List / Map / etc.
        if (isList || isMap || isRepeated) {
            ValueBuilder builder = buildGroup(g, ancestorPath, ctx);
            if (builder != null) {
                out.add(new RootBuilder(appendPath(ancestorPath, g.name()), builder));
            }
            return;
        }
        // Non-repeated nested struct: descend and register each child as its own root at its full path.
        List<String> childPath = append(ancestorPath, g.name());
        Context inner = new Context(ctx.readersByPath, ctx.maxDef + repetitionDef(g), ctx.maxRep);
        for (Field child : g.children()) {
            collectRoots(child, childPath, inner, out);
        }
    }

    /**
     * Returns the {@link ValueBuilder} for {@code field} at the given ancestor path, or {@code null} when no descendant
     * leaf is included in the projection (so the whole subtree drops out of the row accessor).
     */
    private static ValueBuilder buildField(Field field, List<String> ancestorPath, Context ctx) {
        return switch (field) {
            case Field.Primitive p -> buildPrimitive(p, ancestorPath, ctx);
            case Field.Group g -> buildGroup(g, ancestorPath, ctx);
        };
    }

    private static ValueBuilder buildPrimitive(Field.Primitive leaf, List<String> ancestorPath, Context ctx) {
        ColumnPath path = appendPath(ancestorPath, leaf.name());
        ColumnReader reader = ctx.readersByPath.get(path);
        if (reader == null) {
            return null; // not projected
        }
        if (leaf.repetition() == Repetition.REPEATED) {
            // Legacy repeated primitive: synthesize a list around the leaf.
            int repLevel = ctx.maxRep + 1;
            int listDefLevel = ctx.maxDef;
            return new ListValueBuilder(repLevel, listDefLevel, new LeafValueBuilder(reader), reader, List.of(reader));
        }
        return new LeafValueBuilder(reader);
    }

    private static ValueBuilder buildGroup(Field.Group group, List<String> ancestorPath, Context ctx) {
        List<String> groupPath = append(ancestorPath, group.name());
        Optional<LogicalType> annotation = group.logicalType();

        // Annotated LIST: { repeated group <name> { <element-or-fields> } }
        if (annotation.isPresent() && annotation.get() instanceof LogicalType.ListType) {
            return buildAnnotatedList(group, groupPath, ctx);
        }
        // Annotated MAP: { repeated group key_value { required K key; <Q> V value; } }
        if (annotation.isPresent() && annotation.get() instanceof LogicalType.MapType) {
            return buildMap(group, groupPath, ctx);
        }
        // Legacy repeated group without LIST/MAP annotation: list of struct.
        if (group.repetition() == Repetition.REPEATED) {
            return buildLegacyRepeatedGroup(group, groupPath, ancestorPath, ctx);
        }
        // Non-repeated nested group: struct.
        return buildStruct(group, groupPath, ctx);
    }

    private static ValueBuilder buildAnnotatedList(Field.Group listGroup, List<String> listPath, Context ctx) {
        int listDefLevel = ctx.maxDef + repetitionDef(listGroup);
        int listRepBase = ctx.maxRep + repetitionRep(listGroup);
        Field.Group repeatedGroup = expectRepeatedSingleton(listGroup);
        int repLevel = listRepBase + 1; // the repeated group inside contributes +1 to rep
        int innerDef = listDefLevel + 1; // and +1 to def (REPEATED contributes to def)
        List<String> repeatedPath = append(listPath, repeatedGroup.name());
        Context innerCtx = new Context(ctx.readersByPath, innerDef, repLevel);
        // The 3-level LIST has one child inside the repeated group.
        List<Field> elementFields = repeatedGroup.children();
        if (elementFields.size() != 1) {
            // Some legacy files have a multi-field repeated group annotated as LIST; treat as list-of-struct.
            return buildLegacyAnnotatedListMultiField(
                    listGroup, listPath, repeatedGroup, repeatedPath, innerCtx, repLevel, listDefLevel);
        }
        Field elementField = elementFields.get(0);
        List<String> elementPath = append(repeatedPath, elementField.name());
        ValueBuilder elementBuilder = buildField(elementField, repeatedPath, innerCtx);
        if (elementBuilder == null) {
            return null; // element not projected
        }
        List<ColumnReader> elementLeaves = collectLeaves(elementField, elementPath, ctx.readersByPath);
        if (elementLeaves.isEmpty()) {
            return null;
        }
        return new ListValueBuilder(repLevel, listDefLevel, elementBuilder, elementLeaves.get(0), elementLeaves);
    }

    /** Fallback for old multi-field repeated-group LIST shapes (rare; standard 3-level shape has a single element). */
    private static ValueBuilder buildLegacyAnnotatedListMultiField(
            Field.Group listGroup,
            List<String> listPath,
            Field.Group repeatedGroup,
            List<String> repeatedPath,
            Context innerCtx,
            int repLevel,
            int listDefLevel) {
        ValueBuilder elementBuilder = buildStructFromChildren(repeatedGroup, repeatedPath, innerCtx);
        if (elementBuilder == null) {
            return null;
        }
        List<ColumnReader> elementLeaves = collectGroupLeaves(repeatedGroup, repeatedPath, innerCtx.readersByPath);
        if (elementLeaves.isEmpty()) {
            return null;
        }
        return new ListValueBuilder(repLevel, listDefLevel, elementBuilder, elementLeaves.get(0), elementLeaves);
    }

    private static ValueBuilder buildMap(Field.Group mapGroup, List<String> mapPath, Context ctx) {
        int mapDefLevel = ctx.maxDef + repetitionDef(mapGroup);
        int mapRepBase = ctx.maxRep + repetitionRep(mapGroup);
        Field.Group keyValueGroup = expectRepeatedSingleton(mapGroup);
        int repLevel = mapRepBase + 1;
        int innerDef = mapDefLevel + 1;
        List<String> kvPath = append(mapPath, keyValueGroup.name());
        Context kvCtx = new Context(ctx.readersByPath, innerDef, repLevel);
        if (keyValueGroup.children().size() != 2) {
            throw new IllegalStateException(
                    "MAP group at " + columnPath(mapPath) + " must contain exactly two fields (key, value); found "
                            + keyValueGroup.children().size());
        }
        Field keyField = keyValueGroup.children().get(0);
        Field valueField = keyValueGroup.children().get(1);
        ValueBuilder keyBuilder = buildField(keyField, kvPath, kvCtx);
        ValueBuilder valueBuilder = buildField(valueField, kvPath, kvCtx);
        if (keyBuilder == null && valueBuilder == null) {
            return null;
        }
        List<ColumnReader> entryLeaves = collectGroupLeaves(keyValueGroup, kvPath, ctx.readersByPath);
        if (entryLeaves.isEmpty()) {
            return null;
        }
        return new MapValueBuilder(repLevel, mapDefLevel, keyBuilder, valueBuilder, entryLeaves.get(0), entryLeaves);
    }

    private static ValueBuilder buildLegacyRepeatedGroup(
            Field.Group group, List<String> groupPath, List<String> ancestorPath, Context ctx) {
        int listDefLevel = ctx.maxDef; // group itself is REPEATED, so the "list" sits at the parent's def level
        int repLevel = ctx.maxRep + 1;
        int innerDef = ctx.maxDef + 1; // REPEATED contributes +1
        Context innerCtx = new Context(ctx.readersByPath, innerDef, repLevel);
        ValueBuilder elementBuilder = buildStructFromChildren(group, groupPath, innerCtx);
        if (elementBuilder == null) {
            return null;
        }
        List<ColumnReader> elementLeaves = collectGroupLeaves(group, groupPath, ctx.readersByPath);
        if (elementLeaves.isEmpty()) {
            return null;
        }
        return new ListValueBuilder(repLevel, listDefLevel, elementBuilder, elementLeaves.get(0), elementLeaves);
    }

    private static ValueBuilder buildStruct(Field.Group group, List<String> groupPath, Context ctx) {
        int newMaxDef = ctx.maxDef + repetitionDef(group);
        Context nested = new Context(ctx.readersByPath, newMaxDef, ctx.maxRep);
        return buildStructFromChildren(group, groupPath, nested);
    }

    private static ValueBuilder buildStructFromChildren(Field.Group group, List<String> groupPath, Context ctx) {
        List<ChildBinding> children = new ArrayList<>();
        for (Field child : group.children()) {
            ValueBuilder childBuilder = buildField(child, groupPath, ctx);
            if (childBuilder != null) {
                children.add(new ChildBinding(ColumnPath.of(child.name()), childBuilder));
            }
        }
        if (children.isEmpty()) {
            return null;
        }
        List<ColumnReader> structLeaves = collectGroupLeaves(group, groupPath, ctx.readersByPath);
        return new StructValueBuilder(columnPath(groupPath), ctx.maxDef, children, structLeaves);
    }

    // --- helpers ---

    private static Field.Group expectRepeatedSingleton(Field.Group annotated) {
        if (annotated.children().size() != 1) {
            throw new IllegalStateException(
                    "Annotated LIST / MAP group at " + annotated.name() + " must contain exactly one repeated child");
        }
        Field child = annotated.children().get(0);
        if (!(child instanceof Field.Group g) || g.repetition() != Repetition.REPEATED) {
            throw new IllegalStateException(
                    "Annotated LIST / MAP group at " + annotated.name() + " must contain a single REPEATED group");
        }
        return g;
    }

    private static int repetitionDef(Field field) {
        // OPTIONAL and REPEATED both contribute +1 to the definition level chain.
        return switch (field.repetition()) {
            case REQUIRED -> 0;
            case OPTIONAL, REPEATED -> 1;
        };
    }

    private static int repetitionRep(Field field) {
        return field.repetition() == Repetition.REPEATED ? 1 : 0;
    }

    private static List<ColumnReader> collectLeaves(
            Field field, List<String> fieldPath, Map<ColumnPath, ColumnReader> readersByPath) {
        List<ColumnReader> leaves = new ArrayList<>();
        collectInto(field, fieldPath, readersByPath, leaves);
        return leaves;
    }

    private static List<ColumnReader> collectGroupLeaves(
            Field.Group group, List<String> groupPath, Map<ColumnPath, ColumnReader> readersByPath) {
        List<ColumnReader> leaves = new ArrayList<>();
        for (Field child : group.children()) {
            collectInto(child, append(groupPath, child.name()), readersByPath, leaves);
        }
        return leaves;
    }

    private static void collectInto(
            Field field,
            List<String> fieldPath,
            Map<ColumnPath, ColumnReader> readersByPath,
            Collection<ColumnReader> out) {
        switch (field) {
            case Field.Primitive _ -> {
                ColumnReader reader = readersByPath.get(columnPath(fieldPath));
                if (reader != null) {
                    out.add(reader);
                }
            }
            case Field.Group g -> {
                for (Field child : g.children()) {
                    collectInto(child, append(fieldPath, child.name()), readersByPath, out);
                }
            }
        }
    }

    private static List<String> append(List<String> prefix, String segment) {
        List<String> next = new ArrayList<>(prefix.size() + 1);
        next.addAll(prefix);
        next.add(segment);
        return next;
    }

    private static ColumnPath appendPath(List<String> prefix, String segment) {
        return columnPath(append(prefix, segment));
    }

    private static ColumnPath columnPath(List<String> parts) {
        return new ColumnPath(parts);
    }

    /** Bound projected leaf readers + the running max def / max rep at the current ancestor depth. */
    private record Context(Map<ColumnPath, ColumnReader> readersByPath, int maxDef, int maxRep) {}

    /** A top-level projected column and the builder that materializes it. */
    private record RootBuilder(ColumnPath path, ValueBuilder builder) {}
}
