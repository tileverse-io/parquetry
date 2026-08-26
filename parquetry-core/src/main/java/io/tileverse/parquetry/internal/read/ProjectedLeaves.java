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
import java.util.List;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/** Pairs a row group's fetched column chunks with the file-schema leaves a column reader decodes them against. */
final class ProjectedLeaves {

    private ProjectedLeaves() {}

    /**
     * Resolves each fetched chunk to its file-schema leaf. Uses {@link ParquetSchema#find(ColumnPath)} to walk the
     * schema tree and cast to {@link SchemaNode.Primitive}.
     */
    static List<ProjectedLeaf> resolve(List<FetchedColumnChunk> chunks, ParquetSchema fileSchema) {
        List<ProjectedLeaf> result = new ArrayList<>(chunks.size());
        for (FetchedColumnChunk chunk : chunks) {
            ColumnPath path = chunk.path();
            SchemaNode field = fileSchema
                    .find(path)
                    .orElseThrow(() -> new IllegalStateException(
                            "Projected chunk path " + path.dot() + " not found in file schema"));
            if (!(field instanceof SchemaNode.Primitive primitive)) {
                throw new IllegalStateException(
                        "Projected column " + path.dot() + " is not a primitive leaf in the file schema");
            }
            result.add(new ProjectedLeaf(path, chunk, primitive));
        }
        return result;
    }

    /** One fetched column chunk and the file-schema leaf it decodes as. */
    record ProjectedLeaf(ColumnPath path, FetchedColumnChunk chunk, SchemaNode.Primitive leaf) {}
}
