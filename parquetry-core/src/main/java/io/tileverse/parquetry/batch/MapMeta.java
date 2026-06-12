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
package io.tileverse.parquetry.batch;

import java.util.Set;
import java.util.function.ToIntFunction;

import io.tileverse.parquetry.batch.ListMeta.StructMeta;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The level constants and key/value resolution for one map group, computed once and reused on the navigation path. A
 * map is a repeated {@code key_value} group whose two children (key and value) read exactly like struct fields; the
 * entry record reuses {@link StructMeta} over those two children.
 *
 * <p>The level arithmetic mirrors a list's, with {@code key_value} as the repeated child: a row's entries live in the
 * structure leaf's window; the map is null per the vector's validity; the map is empty when the window's first entry
 * has definition below {@link #entryDefLevel()}; otherwise every entry with repetition at most {@link #entryRepLevel()}
 * opens one key-value pair.
 *
 * @param groupDefLevel the definition level at which the map container is present
 * @param entryRepLevel an entry opens a key-value pair when its repetition level is at most this
 * @param entryDefLevel an opened pair is real (not a phantom null/empty marker) when its definition level is at least
 *     this
 * @param structureLeaf the leaf whose level streams define this group's per-row entry windows: the first present
 *     descendant under the key, else under the value
 * @param entry the {@code key_value} group's two fields (key, value), resolved like a struct's fields
 */
public record MapMeta(
        int groupDefLevel, int entryRepLevel, int entryDefLevel, ColumnPath structureLeaf, StructMeta entry) {

    /**
     * Builds the metadata for the map group at {@code groupPath}, resolving the key and value fields against the leaf
     * paths present in {@code presentLeaves}.
     */
    public static MapMeta of(
            ParquetSchema schema,
            SchemaNode.Group mapGroup,
            ColumnPath groupPath,
            Set<ColumnPath> presentLeaves,
            ToIntFunction<ColumnPath> leafOrdinal) {
        SchemaNode.Group keyValue = keyValueGroup(mapGroup);
        ColumnPath keyValuePath = childPath(groupPath, keyValue.name());

        LevelMaxima groupLevels = schema.maxLevels(groupPath);
        LevelMaxima entryLevels = schema.maxLevels(keyValuePath);
        int groupDefLevel = groupLevels.maxDefinitionLevel();
        int entryRepLevel = entryLevels.maxRepetitionLevel();
        int entryDefLevel = entryLevels.maxDefinitionLevel();

        StructMeta entry = ListMeta.structMeta(schema, keyValue, keyValuePath, presentLeaves, leafOrdinal);
        return new MapMeta(groupDefLevel, entryRepLevel, entryDefLevel, entry.structureLeaf(), entry);
    }

    private static SchemaNode.Group keyValueGroup(SchemaNode.Group mapGroup) {
        SchemaNode keyValue = mapGroup.children().get(0);
        if (!(keyValue instanceof SchemaNode.Group group)) {
            throw new IllegalArgumentException("map group's first child must be the key_value group");
        }
        return group;
    }

    private static ColumnPath childPath(ColumnPath parent, String name) {
        return ColumnPath.parse(parent.dot() + "." + name);
    }
}
