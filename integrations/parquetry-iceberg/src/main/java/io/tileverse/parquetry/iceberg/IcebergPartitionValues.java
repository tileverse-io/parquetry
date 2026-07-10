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
package io.tileverse.parquetry.iceberg;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.iceberg.IcebergPartitionSpec.PartitionField;

/**
 * Converts a data file's raw partition tuple into reconstruction constants. Only identity partitions yield a constant:
 * a transform partition keeps its source column physically in the data file and needs no reconstruction. Each identity
 * value is converted to a parquetry {@link Value} by its source field's Iceberg type. A null partition value yields no
 * entry (the column reads as a real null); an unsupported source type fails fast.
 */
final class IcebergPartitionValues {

    private IcebergPartitionValues() {}

    /**
     * Identity-partition source-field-id -> its constant value, for the given raw tuple keyed by partition-field-id.
     */
    static Map<Integer, Value> constantsFor(IcebergPartitionSpec spec, Map<Integer, Object> partitionValues) {
        Map<Integer, Value> constants = new HashMap<>();
        for (Map.Entry<Integer, Object> entry : partitionValues.entrySet()) {
            PartitionField partitionField =
                    spec.byPartitionFieldId(entry.getKey()).orElse(null);
            if (partitionField == null || !partitionField.isIdentity()) {
                continue;
            }
            Object raw = entry.getValue();
            if (raw == null) {
                continue;
            }
            IcebergField source = spec.identitySourceFields().get(partitionField.sourceFieldId());
            if (source == null) {
                // The partition's source column is no longer in the current schema (dropped by evolution). It is not
                // a presented field, nothing references it, and it cannot be reconstructed; skip it rather than fail.
                continue;
            }
            constants.put(partitionField.sourceFieldId(), toValue(source, raw));
        }
        return constants;
    }

    private static Value toValue(IcebergField source, Object raw) {
        return switch (source.type()) {
            case "int" -> new Value.IntVal(((Number) raw).intValue());
            case "long" -> new Value.LongVal(((Number) raw).longValue());
            case "float" -> new Value.FloatVal(((Number) raw).floatValue());
            case "double" -> new Value.DoubleVal(((Number) raw).doubleValue());
            case "boolean" -> new Value.BoolVal((Boolean) raw);
            case "date" -> new Value.DateVal(asLocalDate(raw));
            case "string" -> new Value.StringVal(raw.toString());
            default ->
                throw new IcebergFormatException(
                        "cannot reconstruct identity partition column %s of unsupported type %s"
                                .formatted(source.name(), source.type()));
        };
    }

    /**
     * A date partition value decodes through the Avro logical type to a {@link LocalDate}; accept that, and fall back
     * to raw epoch days when a manifest stores the value without the date logical type.
     */
    private static LocalDate asLocalDate(Object raw) {
        if (raw instanceof LocalDate date) {
            return date;
        }
        return LocalDate.ofEpochDay(((Number) raw).longValue());
    }
}
