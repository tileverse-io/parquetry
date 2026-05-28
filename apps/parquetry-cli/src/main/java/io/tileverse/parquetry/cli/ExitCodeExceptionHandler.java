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

import io.tileverse.parquetry.cli.expr.FilterParseException;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.schema.ParquetSchemaException;

import picocli.CommandLine;
import picocli.CommandLine.IExecutionExceptionHandler;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;

/** Maps parquetry exceptions to {@link CliExitCode} values and prints a one-line cause (stack only when -v). */
final class ExitCodeExceptionHandler implements IExecutionExceptionHandler {

    @Override
    public int handleExecutionException(Exception ex, CommandLine cmd, ParseResult parseResult) {
        if (isVerbose(parseResult)) {
            ex.printStackTrace(cmd.getErr());
        } else {
            cmd.getErr().println("par: " + ex.getMessage());
        }
        return mapToExitCode(ex);
    }

    private static boolean isVerbose(ParseResult parseResult) {
        ParseResult sub = parseResult;
        while (sub != null) {
            if (sub.hasMatchedOption("-v")) {
                return true;
            }
            sub = sub.subcommand();
        }
        return false;
    }

    static int mapToExitCode(Exception ex) {
        if (ex instanceof ParameterException) {
            return CliExitCode.USAGE;
        }
        if (ex instanceof FilterParseException) {
            return CliExitCode.FILTER;
        }
        if (ex instanceof UnsupportedSchemaException) {
            return CliExitCode.FORMAT;
        }
        if (ex instanceof ParquetSchemaException) {
            return CliExitCode.SCHEMA;
        }
        if (ex instanceof ParquetFormatException || ex instanceof ParquetWriteException) {
            return CliExitCode.FORMAT;
        }
        return CliExitCode.GENERIC;
    }
}
