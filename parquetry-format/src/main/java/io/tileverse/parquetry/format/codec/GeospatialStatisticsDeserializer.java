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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.GeospatialStatistics;

/**
 * Deserializer for the Thrift {@code GeospatialStatistics} struct.
 *
 * <pre>
 * struct GeospatialStatistics {
 *   1: optional BoundingBox bbox
 *   2: optional list&lt;i32&gt; geospatial_types
 * }
 * </pre>
 */
final class GeospatialStatisticsDeserializer {

    private GeospatialStatisticsDeserializer() {}

    static GeospatialStatistics read(CompactProtocolReader r) throws IOException {
        Optional<BoundingBox> bbox = Optional.empty();
        Optional<List<Integer>> geospatialTypes = Optional.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> bbox = Optional.of(BoundingBoxDeserializer.read(r));
                case 2 -> geospatialTypes = Optional.of(readI32List(r));
                default -> r.skipField(fh.type());
            }
        }
        return new GeospatialStatistics(bbox, geospatialTypes);
    }

    private static List<Integer> readI32List(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<Integer> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(r.readI32());
        }
        return result;
    }
}
