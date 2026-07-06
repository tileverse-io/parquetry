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
package io.tileverse.parquetry.dataset;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses the Hive {@code key=value} partition segments of a file's relative path into an ordered map. The listing is
 * the discovery mechanism; this only reads the path. A file with no {@code key=value} segments has no partitions.
 */
public final class HivePartitionResolver {

    private HivePartitionResolver() {}

    /**
     * The {@code key=value} path segments of {@code relativePath}, in order, URL-decoded; empty when there are none.
     */
    public static Map<String, String> partitionValues(String relativePath) {
        Map<String, String> values = new LinkedHashMap<>();
        for (String part : relativePath.split("/")) {
            int eq = part.indexOf('=');
            boolean hasKeyAndValue = eq > 0 && eq < part.length() - 1;
            if (hasKeyAndValue) {
                String key = part.substring(0, eq);
                String value = URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8);
                values.put(key, value);
            }
        }
        return values;
    }
}
