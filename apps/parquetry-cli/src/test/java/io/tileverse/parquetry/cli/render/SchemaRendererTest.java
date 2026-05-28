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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.schema.ParquetSchema;

class SchemaRendererTest {

    @Test
    void textTreeListsEveryLeafWithKind() {
        ParquetSchema schema = Fixtures.citiesSchema();
        String text = render(out -> SchemaRenderer.writeText(out, schema));
        assertThat(text).contains("id", "INT32", "name", "BYTE_ARRAY", "pop", "INT64", "capital", "BOOLEAN");
        assertThat(text).contains("required", "optional");
    }

    @Test
    void jsonHasOneEntryPerLeaf() {
        ParquetSchema schema = Fixtures.citiesSchema();
        String json = render(out -> SchemaRenderer.writeJson(out, schema));
        assertThat(json).contains("\"name\":\"id\"", "\"kind\":\"INT32\"");
    }

    private static String render(Consumer<PrintWriter> body) {
        StringWriter sink = new StringWriter();
        PrintWriter out = new PrintWriter(sink);
        body.accept(out);
        out.flush();
        return sink.toString();
    }
}
