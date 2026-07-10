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
package io.tileverse.parquetry.probes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The subset of DuckDB's profile JSON the probe reports: the engine scan latency and the peak off-heap buffer memory.
 */
record DuckDbProfile(double latencySeconds, long peakBufferBytes) {

    static DuckDbProfile read(Path profile) {
        try {
            String json = Files.readString(profile);
            return new DuckDbProfile(
                    extractNumber(json, "latency"), (long) extractNumber(json, "system_peak_buffer_memory"));
        } catch (IOException _) {
            return new DuckDbProfile(0.0, 0L);
        }
    }

    /** Reads the numeric value of the first top-level {@code "key": <number>} occurrence. */
    private static double extractNumber(String json, String key) {
        String needle = "\"" + key + "\":";
        int at = json.indexOf(needle);
        if (at < 0) {
            return 0.0;
        }
        int start = at + needle.length();
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
            start++;
        }
        int end = start;
        while (end < json.length() && "0123456789+-.eE".indexOf(json.charAt(end)) >= 0) {
            end++;
        }
        String number = json.substring(start, end);
        return number.isEmpty() ? 0.0 : Double.parseDouble(number);
    }
}
