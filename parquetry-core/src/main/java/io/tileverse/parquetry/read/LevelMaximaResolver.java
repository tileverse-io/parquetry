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

import java.util.Objects;
import java.util.Optional;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Computes max repetition and definition levels for a leaf column by walking the file schema.
 *
 * <p>Used by both {@link RowGroupReader} (when wiring column readers for projected columns) and by
 * {@link RealColumnFetcher} (when packaging a {@code FetchedColumnChunk}). Both code paths agreed on a single formula
 * before this helper was extracted; centralizing the walk avoids drift between the two.
 *
 * <p>Definition-level rule: each OPTIONAL or REPEATED ancestor (including the leaf) contributes one. Repetition-level
 * rule: each REPEATED ancestor contributes one. REQUIRED nodes contribute neither.
 */
final class LevelMaximaResolver {

    private LevelMaximaResolver() {}

    /**
     * Walks {@code fileSchema} from the root down to {@code path}, summing definition and repetition contributions.
     *
     * @throws IllegalArgumentException if {@code path} cannot be resolved against the schema
     */
    public static LevelMaxima resolve(ParquetSchema fileSchema, ColumnPath path) {
        Objects.requireNonNull(fileSchema, "fileSchema");
        Objects.requireNonNull(path, "path");
        int maxRep = 0;
        int maxDef = 0;
        Field current = fileSchema.root();
        for (String part : path.parts()) {
            if (!(current instanceof Field.Group group)) {
                throw new IllegalArgumentException("Path traversal hit a non-group at " + path.dot());
            }
            Field child = findChildByName(group, part)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "ParquetSchema missing child '" + part + "' along path " + path.dot()));
            switch (child.repetition()) {
                case REQUIRED -> {
                    // No contribution.
                }
                case OPTIONAL -> maxDef++;
                case REPEATED -> {
                    maxDef++;
                    maxRep++;
                }
            }
            current = child;
        }
        return new LevelMaxima(maxRep, maxDef);
    }

    private static Optional<Field> findChildByName(Field.Group group, String name) {
        for (Field child : group.children()) {
            if (child.name().equals(name)) {
                return Optional.of(child);
            }
        }
        return Optional.empty();
    }

    /** Resolved max-rep / max-def pair for a single leaf column. */
    public record LevelMaxima(int maxRepetitionLevel, int maxDefinitionLevel) {
        public LevelMaxima {
            if (maxRepetitionLevel < 0) {
                throw new IllegalArgumentException("maxRepetitionLevel must be >= 0, got " + maxRepetitionLevel);
            }
            if (maxDefinitionLevel < 0) {
                throw new IllegalArgumentException("maxDefinitionLevel must be >= 0, got " + maxDefinitionLevel);
            }
        }
    }
}
