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
package io.tileverse.parquetry.cli;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.cli.expr.FilterParseException;

import picocli.CommandLine.ParameterException;

class ExitCodeExceptionHandlerTest {

    @Test
    void mapsEachKnownExceptionToItsCode() {
        assertThat(ExitCodeExceptionHandler.mapToExitCode(new UnsupportedSchemaException("x")))
                .isEqualTo(CliExitCode.FORMAT);
        assertThat(ExitCodeExceptionHandler.mapToExitCode(new FilterParseException("x")))
                .isEqualTo(CliExitCode.FILTER);
        assertThat(ExitCodeExceptionHandler.mapToExitCode(new IllegalArgumentException("x")))
                .isEqualTo(CliExitCode.GENERIC);
        assertThat(ExitCodeExceptionHandler.mapToExitCode(new ParameterException(Par.newCommandLine(), "x")))
                .isEqualTo(CliExitCode.USAGE);
    }

    @Test
    void describePointsFilterErrorsAtFilterHelp() {
        assertThat(ExitCodeExceptionHandler.describe(new FilterParseException("unsupported in --filter: ILIKE")))
                .isEqualTo("unsupported in --filter: ILIKE (run with --filter-help to list the supported predicates)");
    }

    @Test
    void describeLeavesOtherMessagesUnchanged() {
        assertThat(ExitCodeExceptionHandler.describe(new IllegalArgumentException("boom")))
                .isEqualTo("boom");
    }
}
