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
package io.tileverse.stac;

import java.io.InputStream;
import java.util.Optional;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Reads STAC JSON into a Jackson tree, and reads the document fragments shared by the STAC readers. Holds the one
 * {@link JsonMapper} the STAC readers use, keeping the model package free of any parquetry JSON helper.
 */
final class StacJson {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private StacJson() {}

    /**
     * Parses {@code json} into a tree.
     *
     * @throws StacFormatException if the text is not valid JSON
     */
    static JsonNode parse(InputStream json) {
        try {
            return MAPPER.readTree(json);
        } catch (JacksonException malformed) {
            throw new StacFormatException("malformed STAC JSON", malformed);
        }
    }

    /**
     * Parses {@code json} into a tree.
     *
     * @throws StacFormatException if the text is not valid JSON
     */
    static JsonNode parse(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (JacksonException malformed) {
            throw new StacFormatException("malformed STAC JSON", malformed);
        }
    }

    /** Reads the {@code extent} member of a STAC Collection object, wherever that object was read from. */
    static Optional<StacExtent> readExtent(JsonNode root) {
        JsonNode extent = root.get("extent");
        if (extent == null || !extent.isObject()) {
            return Optional.empty();
        }
        Optional<double[]> bbox = readSpatialBbox(extent.get("spatial"));
        Optional<String[]> interval = readTemporalInterval(extent.get("temporal"));
        return Optional.of(new StacExtent(bbox, interval));
    }

    private static Optional<double[]> readSpatialBbox(JsonNode spatial) {
        if (spatial == null || !spatial.isObject()) {
            return Optional.empty();
        }
        JsonNode bboxes = spatial.get("bbox");
        if (bboxes == null || !bboxes.isArray() || bboxes.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(readBbox(bboxes.get(0)));
    }

    private static Optional<String[]> readTemporalInterval(JsonNode temporal) {
        if (temporal == null || !temporal.isObject()) {
            return Optional.empty();
        }
        JsonNode intervals = temporal.get("interval");
        if (intervals == null || !intervals.isArray() || intervals.isEmpty()) {
            return Optional.empty();
        }
        JsonNode first = intervals.get(0);
        if (first == null || !first.isArray()) {
            return Optional.empty();
        }
        String[] interval = new String[first.size()];
        for (int i = 0; i < first.size(); i++) {
            JsonNode bound = first.get(i);
            interval[i] = bound == null || bound.isNull() ? null : bound.stringValue();
        }
        return Optional.of(interval);
    }

    // A null return is the model's signal that the item declared no bbox; an empty array would be read as a real bound.
    @SuppressWarnings("java:S1168")
    static double[] readBbox(JsonNode bbox) {
        if (bbox == null || !bbox.isArray() || bbox.isEmpty()) {
            return null;
        }
        double[] values = new double[bbox.size()];
        for (int i = 0; i < bbox.size(); i++) {
            values[i] = bbox.get(i).doubleValue();
        }
        return values;
    }
}
