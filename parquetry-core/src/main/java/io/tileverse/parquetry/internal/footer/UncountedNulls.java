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
package io.tileverse.parquetry.internal.footer;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.SchemaElement;

/**
 * Finds, from the footer alone, the leaf columns with a null count not kept by this reader. DuckDB 1.5 counts a null
 * struct of a list twice in the null count of each leaf under the struct. A filter trusting that count skips row groups
 * holding values. DuckDB 2.0 counts it once, and DuckDB 1.4 and earlier record no null count for such a leaf.
 */
final class UncountedNulls {

    private static final Pattern DUCKDB_RELEASE = Pattern.compile("DuckDB version v(\\d{1,9})\\.(\\d{1,9})");
    private static final int AFFECTED_MAJOR = 1;
    private static final int AFFECTED_MINOR = 5;

    private UncountedNulls() {}

    /**
     * The ordinals of the leaf columns with a null count not kept: in a file written by DuckDB 1.5, the leaves under a
     * struct nested in a repeated field. The group wrapping a list or a map is no struct: DuckDB 1.5 counts a nested
     * list right. Ordinals follow the schema's leaf order, as in a {@link LeafIndex}. Empty for a file from another
     * writer.
     */
    static BitSet leavesOf(FileMetaData footer) {
        if (!countsNullStructsTwice(footer.createdBy())) {
            return new BitSet();
        }
        return new SchemaWalk(footer.schema()).leavesUnderStructsInRepeatedFields();
    }

    /** Whether {@code createdBy} names a DuckDB 1.5 release. */
    static boolean countsNullStructsTwice(Optional<String> createdBy) {
        Matcher release = DUCKDB_RELEASE.matcher(createdBy.orElse("").trim());
        if (!release.lookingAt()) {
            return false;
        }
        int major = Integer.parseInt(release.group(1));
        int minor = Integer.parseInt(release.group(2));
        return major == AFFECTED_MAJOR && minor == AFFECTED_MINOR;
    }

    /** A depth-first walk of the schema elements, numbering the leaves in their order. */
    private static final class SchemaWalk {

        private final List<SchemaElement> schema;
        private final BitSet marked = new BitSet();
        private int nextElement;
        private int nextLeaf;

        SchemaWalk(List<SchemaElement> schema) {
            this.schema = schema;
        }

        BitSet leavesUnderStructsInRepeatedFields() {
            if (!schema.isEmpty()) {
                nextElement = 1;
                visitChildren(schema.get(0), false, false);
            }
            return marked;
        }

        private void visitChildren(SchemaElement group, boolean inRepeatedField, boolean inStruct) {
            int children = group.numChildren().orElse(0);
            for (int child = 0; child < children && nextElement < schema.size(); child++) {
                visit(schema.get(nextElement++), inRepeatedField, inStruct);
            }
        }

        private void visit(SchemaElement element, boolean inRepeatedField, boolean inStruct) {
            if (element.numChildren().isEmpty()) {
                markLeaf(inStruct);
                return;
            }
            boolean repeated = isRepeated(element);
            boolean structInRepeatedField = inRepeatedField && !repeated && !wrapsARepeatedField(element);
            visitChildren(element, inRepeatedField || repeated, inStruct || structInRepeatedField);
        }

        /** Whether {@code group} wraps a list or a map: its only child, next in the walk, is repeated. */
        private boolean wrapsARepeatedField(SchemaElement group) {
            boolean oneChild = group.numChildren().orElse(0) == 1;
            return oneChild && nextElement < schema.size() && isRepeated(schema.get(nextElement));
        }

        private static boolean isRepeated(SchemaElement element) {
            return element.repetitionType().orElse(null) == FieldRepetitionType.REPEATED;
        }

        private void markLeaf(boolean inStruct) {
            if (inStruct) {
                marked.set(nextLeaf);
            }
            nextLeaf++;
        }
    }
}
