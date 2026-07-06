/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.cli.render;

import java.io.PrintWriter;
import java.util.List;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/** Renders a {@link ParquetSchema} as an indented message-type tree (text) or a flat leaf list (json). */
public final class SchemaRenderer {

    private SchemaRenderer() {}

    /** Writes an indented Parquet message-type tree, one field per line. */
    public static void writeText(PrintWriter out, ParquetSchema schema) {
        out.append("message ").append(schema.root().name()).append(" {\n");
        for (SchemaNode child : schema.root().children()) {
            appendNode(out, child, 1);
        }
        out.append("}\n");
        // append() never triggers the auto-flush PrintWriter; flush explicitly because a native image does not
        // flush the writer on exit.
        out.flush();
    }

    private static void appendNode(PrintWriter out, SchemaNode node, int depth) {
        out.append("  ".repeat(depth));
        out.append(node.repetition().name().toLowerCase()).append(' ');
        if (node instanceof SchemaNode.Primitive prim) {
            out.append(prim.kind().name()).append(' ').append(prim.name());
            prim.logicalType()
                    .ifPresent(lt -> out.append(" (")
                            .append(lt.getClass().getSimpleName())
                            .append(')'));
            out.append(";\n");
            return;
        }
        SchemaNode.Group group = (SchemaNode.Group) node;
        out.append("group ").append(group.name()).append(" {\n");
        for (SchemaNode child : group.children()) {
            appendNode(out, child, depth + 1);
        }
        out.append("  ".repeat(depth)).append("}\n");
    }

    /** Writes a JSON object with the message name and a flat array of leaf column descriptors. */
    public static void writeJson(PrintWriter out, ParquetSchema schema) {
        List<ColumnPath> leaves = schema.leafColumns();
        Json.writeTo(out, gen -> {
            gen.writeStartObject();
            gen.writeStringProperty("message", schema.root().name());
            gen.writeArrayPropertyStart("columns");
            for (ColumnPath leaf : leaves) {
                SchemaNode.Primitive prim =
                        (SchemaNode.Primitive) schema.find(leaf).orElseThrow();
                gen.writeStartObject();
                gen.writeStringProperty("name", leaf.dot());
                gen.writeStringProperty("kind", prim.kind().name());
                gen.writeStringProperty("repetition", prim.repetition().name());
                prim.logicalType()
                        .ifPresent(lt -> gen.writeStringProperty(
                                "logicalType", lt.getClass().getSimpleName()));
                gen.writeEndObject();
            }
            gen.writeEndArray();
            gen.writeEndObject();
        });
    }
}
