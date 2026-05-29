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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.ColumnOrder;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.SchemaElement;

/**
 * Deserializer for the Thrift {@code FileMetaData} struct - the top-level footer record.
 *
 * <pre>
 * struct FileMetaData {
 *   1: required i32 version
 *   2: required list&lt;SchemaElement&gt; schema
 *   3: required i64 num_rows
 *   4: required list&lt;RowGroup&gt; row_groups
 *   5: optional list&lt;KeyValue&gt; key_value_metadata
 *   6: optional string created_by
 *   7: optional list&lt;ColumnOrder&gt; column_orders
 *   8: optional EncryptionAlgorithm encryption_algorithm  (skipped - deferred)
 *   9: optional binary footer_signing_key_metadata        (skipped - deferred)
 * }
 * </pre>
 */
final class FileMetaDataDeserializer {

    private FileMetaDataDeserializer() {}

    static FileMetaData read(CompactProtocolReader r) throws IOException {
        int version = 0;
        List<SchemaElement> schema = new ArrayList<>();
        long numRows = 0;
        List<RowGroup> rowGroups = new ArrayList<>();
        List<KeyValue> keyValueMetadata = new ArrayList<>();
        Optional<String> createdBy = Optional.empty();
        Optional<List<ColumnOrder>> columnOrders = Optional.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> version = r.readI32();
                case 2 -> schema = readSchemaList(r);
                case 3 -> numRows = r.readI64();
                case 4 -> rowGroups = readRowGroupList(r);
                case 5 -> keyValueMetadata = readKeyValueList(r);
                case 6 -> createdBy = Optional.of(r.readString());
                case 7 -> columnOrders = Optional.of(readColumnOrderList(r));
                // Fields 8 (EncryptionAlgorithm) and 9 (footer signing key) are deferred - skip
                default -> r.skipField(fh.type());
            }
        }
        return new FileMetaData(
                version,
                schema,
                numRows,
                rowGroups,
                keyValueMetadata,
                createdBy,
                columnOrders,
                Optional.empty(),
                MemorySegment.NULL);
    }

    private static List<SchemaElement> readSchemaList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<SchemaElement> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(SchemaElementDeserializer.read(r));
        }
        return result;
    }

    private static List<RowGroup> readRowGroupList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<RowGroup> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(RowGroupDeserializer.read(r));
        }
        return result;
    }

    private static List<KeyValue> readKeyValueList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<KeyValue> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(KeyValueDeserializer.read(r));
        }
        return result;
    }

    private static List<ColumnOrder> readColumnOrderList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<ColumnOrder> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(ColumnOrderDeserializer.read(r));
        }
        return result;
    }
}
