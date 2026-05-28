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

import java.io.PrintWriter;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

import tools.jackson.core.JsonGenerator;

/**
 * Streams projected records to stdout in one of four formats. Stateful: call {@link #begin}, then {@link #row} per
 * record, then {@link #end}.
 */
public final class RecordRenderer {

    public enum Mode {
        JSONL,
        CSV,
        TSV,
        TEXT
    }

    private final Mode mode;
    private final ParquetSchema schema;
    private final List<ColumnPath> leaves;
    private final PrintWriter out;

    public RecordRenderer(Mode mode, ParquetSchema schema, List<ColumnPath> leaves, PrintWriter out) {
        this.mode = mode;
        this.schema = schema;
        this.leaves = leaves;
        this.out = out;
    }

    public void begin() {
        if (mode == Mode.CSV || mode == Mode.TSV) {
            out.println(String.join(
                    delimiter(), leaves.stream().map(ColumnPath::dot).toList()));
        }
    }

    public void row(ParquetRecord record) {
        switch (mode) {
            case JSONL -> jsonRow(record);
            case CSV, TSV -> delimitedRow(record);
            case TEXT -> textRow(record);
        }
    }

    public void end() {
        out.flush();
    }

    private void jsonRow(ParquetRecord record) {
        String line = Json.write(gen -> {
            gen.writeStartObject();
            for (ColumnPath leaf : leaves) {
                writeJsonValue(gen, record, leaf);
            }
            gen.writeEndObject();
        });
        out.println(line);
    }

    private void writeJsonValue(JsonGenerator gen, ParquetRecord record, ColumnPath leaf) {
        if (record.isNull(leaf)) {
            return;
        }
        SchemaNode.Primitive prim = primitive(leaf);
        String name = leaf.dot();
        switch (prim.kind()) {
            case BOOLEAN -> gen.writeBooleanProperty(name, record.getBoolean(leaf));
            case INT32 -> gen.writeNumberProperty(name, record.getInt(leaf));
            case INT64 -> gen.writeNumberProperty(name, record.getLong(leaf));
            case FLOAT -> gen.writeNumberProperty(name, record.getFloat(leaf));
            case DOUBLE -> gen.writeNumberProperty(name, record.getDouble(leaf));
            default -> gen.writeStringProperty(name, binaryAsString(prim, record, leaf));
        }
    }

    private void delimitedRow(ParquetRecord record) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < leaves.size(); i++) {
            if (i > 0) {
                sb.append(delimiter());
            }
            sb.append(scalar(record, leaves.get(i)));
        }
        out.println(sb);
    }

    private void textRow(ParquetRecord record) {
        StringBuilder sb = new StringBuilder();
        for (ColumnPath leaf : leaves) {
            sb.append(leaf.dot()).append('=').append(scalar(record, leaf)).append('\t');
        }
        out.println(sb.toString().stripTrailing());
    }

    private String scalar(ParquetRecord record, ColumnPath leaf) {
        if (record.isNull(leaf)) {
            return "";
        }
        SchemaNode.Primitive prim = primitive(leaf);
        return switch (prim.kind()) {
            case BOOLEAN -> Boolean.toString(record.getBoolean(leaf));
            case INT32 -> Integer.toString(record.getInt(leaf));
            case INT64 -> Long.toString(record.getLong(leaf));
            case FLOAT -> Float.toString(record.getFloat(leaf));
            case DOUBLE -> Double.toString(record.getDouble(leaf));
            default -> binaryAsString(prim, record, leaf);
        };
    }

    private String binaryAsString(SchemaNode.Primitive prim, ParquetRecord record, ColumnPath leaf) {
        if (isStringLike(prim.logicalType())) {
            return record.getString(leaf);
        }
        return Base64.getEncoder().encodeToString(record.getBinary(leaf));
    }

    private static boolean isStringLike(Optional<LogicalType> logicalType) {
        return logicalType
                .map(lt -> lt instanceof LogicalType.StringType
                        || lt instanceof LogicalType.EnumType
                        || lt instanceof LogicalType.JsonType)
                .orElse(false);
    }

    private SchemaNode.Primitive primitive(ColumnPath leaf) {
        return (SchemaNode.Primitive) schema.find(leaf).orElseThrow();
    }

    private String delimiter() {
        return mode == Mode.TSV ? "\t" : ",";
    }
}
