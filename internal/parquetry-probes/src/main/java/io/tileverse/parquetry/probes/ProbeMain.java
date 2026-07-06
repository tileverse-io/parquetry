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
package io.tileverse.parquetry.probes;

/**
 * Entry point of the shaded {@code probes.jar}: dispatches to a read-comparison probe by name. Each probe reads its
 * settings from {@code parquetry.probe.*} system properties (file, concurrency, warmup, measure, engines, scenarios).
 *
 * <pre>{@code
 * java -Dparquetry.probe.file=buildings.parquet -Dparquetry.probe.concurrency=8 -jar probes.jar read
 * }</pre>
 */
public final class ProbeMain {

    private ProbeMain() {}

    public static void main(String[] args) throws Exception {
        String which = args.length > 0 ? args[0] : "";
        switch (which) {
            case "read", "ReadComparison" -> ReadComparisonProbe.main(args);
            case "columnar", "ColumnarReadComparison" -> ColumnarReadComparisonProbe.main(args);
            default -> printUsage();
        }
    }

    private static void printUsage() {
        IO.println("Usage: java [JVM flags] -Dparquetry.probe.* -jar probes.jar <probe>");
        IO.println("  probes:");
        IO.println("    read      row-path comparison (ReadComparisonProbe)");
        IO.println("    columnar  columnar-path comparison (ColumnarReadComparisonProbe)");
        IO.println("  settings (system properties): parquetry.probe.file (required), .concurrency, .warmup,");
        IO.println("    .measure, .engines, .scenarios, .subtype");
    }
}
