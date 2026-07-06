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
import java.io.StringWriter;
import java.util.function.Consumer;

import tools.jackson.core.JsonGenerator;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.StreamWriteFeature;
import tools.jackson.core.json.JsonFactory;

/** Thin reflection-free JSON helpers built on Jackson streaming. */
public final class Json {

    // AUTO_CLOSE_TARGET is disabled so streaming a document onto the command's stdout writer flushes the generator
    // without closing the writer underneath the caller.
    private static final JsonFactory FACTORY =
            JsonFactory.builder().disable(StreamWriteFeature.AUTO_CLOSE_TARGET).build();

    private Json() {}

    /** Renders a JSON document to a String via the supplied generator callback. */
    public static String write(Consumer<JsonGenerator> body) {
        StringWriter sw = new StringWriter();
        try (JsonGenerator gen = FACTORY.createGenerator(ObjectWriteContext.empty(), sw)) {
            body.accept(gen);
        }
        return sw.toString();
    }

    /** Streams a JSON document straight to {@code out}, terminated by a newline. */
    public static void writeTo(PrintWriter out, Consumer<JsonGenerator> body) {
        try (JsonGenerator gen = FACTORY.createGenerator(ObjectWriteContext.empty(), out)) {
            body.accept(gen);
        }
        out.println();
        // picocli's auto-flush PrintWriter only flushes on println/printf; flush explicitly because a native image
        // does not flush the writer on exit.
        out.flush();
    }
}
