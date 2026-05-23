/*
 * Copyright (c) 2026 Tileverse.io
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
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.GeospatialStatistics;

/** Serializer mirror of {@link GeospatialStatisticsDeserializer}. */
final class GeospatialStatisticsSerializer {

    private GeospatialStatisticsSerializer() {}

    static void serialize(CompactProtocolWriter w, GeospatialStatistics gs) throws IOException {
        w.writeStructBegin();
        Optional<BoundingBox> bbox = gs.bbox();
        if (bbox.isPresent()) {
            w.writeFieldBegin((short) 1, CompactType.STRUCT);
            BoundingBoxSerializer.serialize(w, bbox.get());
        }
        Optional<List<Integer>> geospatialTypes = gs.geospatialTypes();
        if (geospatialTypes.isPresent()) {
            w.writeListField((short) 2, CompactType.I32, geospatialTypes.get(), CompactProtocolWriter::writeI32);
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
