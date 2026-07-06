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
import java.lang.foreign.MemorySegment;
import java.util.List;

import io.tileverse.parquetry.jackson.JsonRecordEncoder;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Streams decoded rows to a {@link PrintWriter} in one of the text output modes. jsonl emits true nested JSON via
 * {@link JsonRecordEncoder}; csv/tsv/text emit one column per projected top-level field, with a compact-JSON cell for
 * any struct/list/map/Variant field.
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
    private final List<SchemaNode> fields;
    private final PrintWriter out;

    public RecordRenderer(Mode mode, ParquetSchema schema, PrintWriter out) {
        this.mode = mode;
        this.schema = schema;
        this.fields = schema.root().children();
        this.out = out;
    }

    public void begin() {
        if (mode == Mode.CSV || mode == Mode.TSV) {
            writeLine(String.join(
                    delimiter(), fields.stream().map(SchemaNode::name).toList()));
        }
    }

    public void row(ParquetRecord row) {
        switch (mode) {
            case JSONL -> jsonRow(row);
            case CSV, TSV -> delimitedRow(row);
            case TEXT -> textRow(row);
        }
    }

    public void end() {
        out.flush();
    }

    private void jsonRow(ParquetRecord row) {
        String line = Json.write(generator -> JsonRecordEncoder.writeObject(generator, schema, row));
        writeLine(line);
    }

    private void delimitedRow(ParquetRecord row) {
        StringBuilder line = new StringBuilder();
        for (int index = 0; index < fields.size(); index++) {
            if (index > 0) {
                line.append(delimiter());
            }
            line.append(cell(row, fields.get(index)));
        }
        writeLine(line.toString());
    }

    private void textRow(ParquetRecord row) {
        StringBuilder line = new StringBuilder();
        for (SchemaNode field : fields) {
            line.append(field.name()).append('=').append(cell(row, field)).append('\t');
        }
        writeLine(line.toString().stripTrailing());
    }

    private void writeLine(String text) {
        out.print(text);
        out.print('\n');
        out.flush();
    }

    private String cell(ParquetRecord row, SchemaNode field) {
        ColumnPath path = ColumnPath.of(field.name());
        Object value = row.get(path);
        if (value == null) {
            return "";
        }
        if (field instanceof SchemaNode.Primitive primitive) {
            return scalar(primitive, value);
        }
        return Json.write(generator -> JsonRecordEncoder.writeValue(generator, field, value));
    }

    private String scalar(SchemaNode.Primitive primitive, Object value) {
        return switch (primitive.kind()) {
            case BOOLEAN -> Boolean.toString((Boolean) value);
            case INT32 -> Integer.toString((Integer) value);
            case INT64 -> Long.toString((Long) value);
            case FLOAT -> Float.toString((Float) value);
            case DOUBLE -> Double.toString((Double) value);
            default -> binaryAsString(primitive, (MemorySegment) value);
        };
    }

    private String binaryAsString(SchemaNode.Primitive primitive, MemorySegment value) {
        return JsonRecordEncoder.binaryScalarToString(primitive, value);
    }

    private String delimiter() {
        return mode == Mode.TSV ? "\t" : ",";
    }
}
