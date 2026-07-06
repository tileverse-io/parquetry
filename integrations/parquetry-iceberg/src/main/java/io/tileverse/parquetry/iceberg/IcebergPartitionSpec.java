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
package io.tileverse.parquetry.iceberg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The partition model of an Iceberg table, merged across every partition spec the table has ever used. Keying by
 * partition-field-id (globally unique across specs for a given source and transform) lets the reader decode any data
 * file's partition tuple without tracking which spec wrote it.
 */
final class IcebergPartitionSpec {

    /**
     * One partition field of an Iceberg partition spec.
     *
     * @param partitionFieldId the partition-field-id Iceberg assigns (globally unique across a table's specs)
     * @param sourceFieldId the schema field id this partition derives from
     * @param name the partition field name
     * @param transform the transform name, for example {@code identity} or {@code day}
     */
    record PartitionField(int partitionFieldId, int sourceFieldId, String name, String transform) {
        PartitionField {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(transform, "transform");
        }

        boolean isIdentity() {
            return "identity".equals(transform);
        }
    }

    private final Map<Integer, PartitionField> byPartitionFieldId;
    private final Map<Integer, IcebergField> identitySourceFields;

    private IcebergPartitionSpec(
            Map<Integer, PartitionField> byPartitionFieldId, Map<Integer, IcebergField> identitySourceFields) {
        this.byPartitionFieldId = Map.copyOf(byPartitionFieldId);
        this.identitySourceFields = Map.copyOf(identitySourceFields);
    }

    /** Builds the merged spec from every partition field across all specs and the table's primitive fields. */
    static IcebergPartitionSpec of(List<PartitionField> partitionFields, List<IcebergField> tableFields) {
        Map<Integer, PartitionField> byId = new HashMap<>();
        Map<Integer, IcebergField> identities = new HashMap<>();
        Map<Integer, IcebergField> fieldsById = new HashMap<>();
        for (IcebergField field : tableFields) {
            fieldsById.put(field.fieldId(), field);
        }
        for (PartitionField partitionField : partitionFields) {
            byId.put(partitionField.partitionFieldId(), partitionField);
            if (partitionField.isIdentity()) {
                IcebergField source = fieldsById.get(partitionField.sourceFieldId());
                if (source != null) {
                    identities.put(partitionField.sourceFieldId(), source);
                }
            }
        }
        return new IcebergPartitionSpec(byId, identities);
    }

    boolean isEmpty() {
        return byPartitionFieldId.isEmpty();
    }

    Optional<PartitionField> byPartitionFieldId(int id) {
        return Optional.ofNullable(byPartitionFieldId.get(id));
    }

    /** Identity-partition source-field-id -> its table field; the columns reconstruction may need to rebuild. */
    Map<Integer, IcebergField> identitySourceFields() {
        return identitySourceFields;
    }
}
