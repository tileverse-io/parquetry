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
package io.tileverse.parquetry.cli;

import java.util.List;

import picocli.CommandLine.Option;

/** Options shared by every subcommand via {@code @Mixin}. */
public final class GlobalOptions {

    public enum Format {
        TEXT,
        JSON,
        JSONL,
        CSV,
        TSV
    }

    @Option(
            names = {"--columns"},
            split = ",",
            paramLabel = "<col>",
            description = "Leaf columns to keep (comma-separated). Default: all.")
    public List<String> columns = List.of();

    @Option(
            names = {"--filter"},
            paramLabel = "<expr>",
            description = "Predicate expression, e.g. \"pop > 1000 AND name = 'x'\".")
    public String filter;

    @Option(
            names = {"--limit"},
            paramLabel = "<n>",
            description = "Cap rows emitted/written.")
    public Long limit;

    @Option(
            names = {"-o", "--format"},
            description = "Output format. Valid: ${COMPLETION-CANDIDATES}.")
    public Format format;

    @Option(
            names = {"-v", "--verbose"},
            description = "Print stack traces on error.")
    public boolean verbose;
}
