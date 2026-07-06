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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;

/**
 * Deserializer for the Thrift {@code OffsetIndex} struct.
 *
 * <pre>
 * struct OffsetIndex {
 *   1: required list&lt;PageLocation&gt; page_locations
 *   2: optional list&lt;i64&gt; unencoded_byte_array_data_bytes
 * }
 * </pre>
 */
final class OffsetIndexDeserializer {

    private OffsetIndexDeserializer() {}

    static OffsetIndex read(CompactProtocolReader r) throws IOException {
        List<PageLocation> pageLocations = new ArrayList<>();
        Optional<List<Long>> unencodedByteArrayDataBytes = Optional.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> pageLocations = readPageLocationList(r);
                case 2 -> unencodedByteArrayDataBytes = Optional.of(readLongList(r));
                default -> r.skipField(fh.type());
            }
        }
        return new OffsetIndex(pageLocations, unencodedByteArrayDataBytes);
    }

    private static List<PageLocation> readPageLocationList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<PageLocation> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(PageLocationDeserializer.read(r));
        }
        return result;
    }

    private static List<Long> readLongList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<Long> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(r.readI64());
        }
        return result;
    }
}
