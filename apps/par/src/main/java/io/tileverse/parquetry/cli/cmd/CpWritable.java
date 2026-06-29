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
package io.tileverse.parquetry.cli.cmd;

import io.tileverse.parquetry.cli.UnsupportedSchemaException;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Preflight for {@code cp}: checks the writer can reproduce a schema before the destination is opened, giving a clean
 * error instead of a deep write failure mid-copy.
 *
 * <p>The copy pumps columnar batches straight from the read into the writer, which reproduces struct, list, map and
 * Variant faithfully. INT96 is the writer's one remaining gap, hence the only column kind rejected here.
 */
public final class CpWritable {

    private CpWritable() {}

    /** Throws {@link UnsupportedSchemaException} for the first INT96 leaf anywhere in {@code schema}. */
    public static void requireWritable(ParquetSchema schema) {
        for (SchemaNode child : schema.root().children()) {
            checkNode(child, ColumnPath.of(child.name()));
        }
    }

    private static void checkNode(SchemaNode node, ColumnPath path) {
        if (node instanceof SchemaNode.Primitive primitive) {
            checkPrimitive(primitive, path);
        } else if (node instanceof SchemaNode.Group group) {
            checkGroup(group, path);
        }
    }

    private static void checkPrimitive(SchemaNode.Primitive primitive, ColumnPath path) {
        if (primitive.kind() == PrimitiveKind.INT96) {
            throw new UnsupportedSchemaException("cp cannot write INT96 column '" + path.dot() + "'");
        }
    }

    private static void checkGroup(SchemaNode.Group group, ColumnPath path) {
        for (SchemaNode child : group.children()) {
            ColumnPath childPath = ColumnPath.parse(path.dot() + "." + child.name());
            checkNode(child, childPath);
        }
    }
}
