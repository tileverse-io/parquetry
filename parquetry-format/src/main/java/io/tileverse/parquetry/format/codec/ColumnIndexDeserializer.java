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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.enums.BoundaryOrder;

/**
 * Deserializer for the Thrift {@code ColumnIndex} struct.
 *
 * <pre>
 * struct ColumnIndex {
 *   1: required list&lt;bool&gt; null_pages
 *   2: required list&lt;binary&gt; min_values
 *   3: required list&lt;binary&gt; max_values
 *   4: required BoundaryOrder boundary_order
 *   5: optional list&lt;i64&gt; null_counts
 *   6: optional list&lt;i64&gt; repetition_level_histograms
 *   7: optional list&lt;i64&gt; definition_level_histograms
 * }
 * </pre>
 */
final class ColumnIndexDeserializer {

    private ColumnIndexDeserializer() {}

    static ColumnIndex read(CompactProtocolReader r) throws IOException {
        List<Boolean> nullPages = new ArrayList<>();
        List<byte[]> minValues = new ArrayList<>();
        List<byte[]> maxValues = new ArrayList<>();
        BoundaryOrder boundaryOrder = BoundaryOrder.UNORDERED;
        Optional<List<Long>> nullCounts = Optional.empty();
        Optional<List<Long>> repetitionLevelHistograms = Optional.empty();
        Optional<List<Long>> definitionLevelHistograms = Optional.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> nullPages = readBoolList(r);
                case 2 -> minValues = readBinaryList(r);
                case 3 -> maxValues = readBinaryList(r);
                case 4 -> boundaryOrder = BoundaryOrder.values()[r.readI32()];
                case 5 -> nullCounts = Optional.of(readLongList(r));
                case 6 -> repetitionLevelHistograms = Optional.of(readLongList(r));
                case 7 -> definitionLevelHistograms = Optional.of(readLongList(r));
                default -> r.skipField(fh.type());
            }
        }
        return new ColumnIndex(
                nullPages,
                minValues,
                maxValues,
                boundaryOrder,
                nullCounts,
                repetitionLevelHistograms,
                definitionLevelHistograms);
    }

    /**
     * Reads a list of booleans. In Thrift Compact Protocol, boolean values in a list are encoded
     * as individual bytes: 0x01 for true, 0x02 for false.
     */
    private static List<Boolean> readBoolList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<Boolean> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(r.readBool());
        }
        return result;
    }

    private static List<byte[]> readBinaryList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<byte[]> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(r.readBinary());
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
