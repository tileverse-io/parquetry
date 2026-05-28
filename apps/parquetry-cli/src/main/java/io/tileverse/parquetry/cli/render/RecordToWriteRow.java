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
package io.tileverse.parquetry.cli.render;

import io.tileverse.parquetry.cli.UnsupportedSchemaException;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/** Adapts a {@link ParquetRecord} to a {@link WriteRow}, and guards the flat-primitive schema limit. */
public final class RecordToWriteRow {

    private RecordToWriteRow() {}

    /** Passes leaf values straight through; nulls stay null. */
    public static WriteRow adapt(ParquetRecord record) {
        return path -> record.isNull(path) ? null : record.get(path);
    }

    /**
     * Verifies the writer can reproduce {@code schema}: top-level fields must be non-repeated primitives, and no leaf
     * may be INT96 or a Variant logical type. Throws {@link UnsupportedSchemaException} otherwise.
     */
    public static void requireWritable(ParquetSchema schema) {
        for (SchemaNode child : schema.root().children()) {
            if (!(child instanceof SchemaNode.Primitive prim)) {
                throw new UnsupportedSchemaException(
                        "cp supports flat primitive schemas only; column '" + child.name() + "' is a group");
            }
            if (prim.repetition() == Repetition.REPEATED) {
                throw new UnsupportedSchemaException(
                        "cp supports flat primitive schemas only; column '" + prim.name() + "' is repeated");
            }
            if (prim.kind() == PrimitiveKind.INT96) {
                throw new UnsupportedSchemaException("cp cannot write INT96 column '" + prim.name() + "'");
            }
            if (prim.logicalType()
                    .filter(lt -> lt instanceof LogicalType.Variant)
                    .isPresent()) {
                throw new UnsupportedSchemaException("cp cannot write Variant column '" + prim.name() + "'");
            }
        }
    }
}
