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
package io.tileverse.parquetry.iceberg;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import io.tileverse.parquetry.avro.AvroDataFileReader;
import io.tileverse.parquetry.avro.AvroRecord;
import io.tileverse.parquetry.avro.AvroSchema.Field;
import io.tileverse.parquetry.avro.AvroSchema.Record;
import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Reads an Iceberg snapshot's manifest list and its data manifests (Avro) into data-file references. Delete manifests
 * and delete-content entries fail fast: merge-on-read is not yet supported.
 *
 * <p>This reader holds no table schema. It keeps each file's bounds as raw, field-id-keyed bytes and leaves decoding
 * them to {@link IcebergFileStats}, which has the schema; see {@link DataFileRef}.
 */
final class IcebergManifests {

    private static final int CONTENT_DATA = 0;
    private static final int STATUS_DELETED = 2;

    private IcebergManifests() {}

    /**
     * A surviving data file: its absolute location, the row count the manifest records, and the raw per-column
     * statistics a later pruning step consumes.
     *
     * <p>The bound maps and the null-count map mirror the manifest's on-disk shape, each an Iceberg
     * {@code map<field-id, value>}: the keys are field ids and a missing key means the manifest recorded no such
     * statistic for that column. The keys are field ids rather than column names because a field id is all the manifest
     * records; resolving a column's name and type needs the table schema, which this reader does not hold.
     * {@link IcebergFileStats#from} performs that join.
     *
     * <p>A lower/upper bound value is the raw bound bytes as a {@link MemorySegment}, a zero-copy view of the Avro
     * record. Decoding a bound needs its column's Iceberg type, which lives in the schema and not here. The bytes
     * therefore stay undecoded until the schema-aware layer reads them best-effort, skipping any bound it cannot decode
     * rather than failing the read. A null-value count is the number of nulls in that column.
     */
    public record DataFileRef(
            String location,
            long recordCount,
            Map<Integer, MemorySegment> lowerBounds,
            Map<Integer, MemorySegment> upperBounds,
            Map<Integer, Long> nullValueCounts) {

        public DataFileRef {
            Objects.requireNonNull(location, "location");
            lowerBounds = Map.copyOf(lowerBounds);
            upperBounds = Map.copyOf(upperBounds);
            nullValueCounts = Map.copyOf(nullValueCounts);
        }
    }

    /** Reads every surviving data file referenced by {@code manifestListLocation}. */
    public static List<DataFileRef> readDataFiles(String manifestListLocation, IcebergFileIO io) {
        List<String> manifestLocations = readManifestLocations(manifestListLocation, io);
        List<DataFileRef> dataFiles = new ArrayList<>();
        // Each manifest is an independent I/O (one object-store read). Reading them serially stalls on round-trip
        // latency once a table has many manifests on cloud storage; parallelize across virtual threads here when the
        // cloud IcebergFileIO lands. The per-manifest result lists keep this order-preserving when that happens.
        for (String manifestLocation : manifestLocations) {
            dataFiles.addAll(readManifest(manifestLocation, io));
        }
        return dataFiles;
    }

    private static List<String> readManifestLocations(String manifestListLocation, IcebergFileIO io) {
        List<String> manifests = new ArrayList<>();
        try (ByteRangeSource source = io.open(manifestListLocation);
                AvroDataFileReader reader = AvroDataFileReader.open(source);
                Stream<AvroRecord> entries = reader.records()) {
            entries.forEach(entry -> {
                requireDataManifest(entry);
                manifests.add((String) entry.get("manifest_path"));
            });
        }
        return manifests;
    }

    /**
     * The manifest-list {@code content} field (0=data, 1=deletes) became required only in v2; v1 manifest lists omit it
     * and reference data manifests implicitly. Treat absent/null as data; only a present delete value fails fast.
     */
    private static void requireDataManifest(AvroRecord entry) {
        Object content = safeGet(entry, "content");
        if (content != null && intValue(content) != CONTENT_DATA) {
            throw new IcebergFormatException(
                    "merge-on-read is not supported: manifest list references a delete manifest");
        }
    }

    private static List<DataFileRef> readManifest(String manifestLocation, IcebergFileIO io) {
        List<DataFileRef> dataFiles = new ArrayList<>();
        try (ByteRangeSource source = io.open(manifestLocation);
                AvroDataFileReader reader = AvroDataFileReader.open(source);
                Stream<AvroRecord> entries = reader.records()) {

            entries.forEach(entry -> {
                int status = requiredNumber(entry.get("status"), "status").intValue();
                if (status == STATUS_DELETED) {
                    return;
                }
                AvroRecord dataFile = (AvroRecord) entry.get("data_file");
                requireDataContent(dataFile);
                String location = (String) dataFile.get("file_path");
                long recordCount = requiredNumber(dataFile.get("record_count"), "record_count")
                        .longValue();
                Map<Integer, MemorySegment> lowerBounds =
                        readIntKeyedMap(safeGet(dataFile, "lower_bounds"), MemorySegment.class::cast);
                Map<Integer, MemorySegment> upperBounds =
                        readIntKeyedMap(safeGet(dataFile, "upper_bounds"), MemorySegment.class::cast);
                Map<Integer, Long> nullValueCounts =
                        readIntKeyedMap(safeGet(dataFile, "null_value_counts"), value -> ((Number) value).longValue());
                dataFiles.add(new DataFileRef(location, recordCount, lowerBounds, upperBounds, nullValueCounts));
            });
        }
        return dataFiles;
    }

    /** The data_file record has a {@code content} field (0=data) in v2+; treat a non-data value as merge-on-read. */
    private static void requireDataContent(AvroRecord dataFile) {
        Object content = safeGet(dataFile, "content");
        if (content != null && intValue(content) != CONTENT_DATA) {
            throw new IcebergFormatException("merge-on-read is not supported: a manifest entry has delete content");
        }
    }

    /**
     * Iceberg encodes a field-id keyed statistic as an Avro array of {@code {key: int, value: ...}} records. A null
     * field (the statistic is absent from this manifest) yields an empty map.
     */
    private static <V> Map<Integer, V> readIntKeyedMap(Object field, Function<Object, V> valueMapper) {
        Map<Integer, V> map = new LinkedHashMap<>();
        if (field instanceof List<?> entries) {
            for (Object element : entries) {
                AvroRecord pair = (AvroRecord) element;
                int key = ((Number) pair.get("key")).intValue();
                map.put(key, valueMapper.apply(pair.get("value")));
            }
        }
        return map;
    }

    private static Object safeGet(AvroRecord avroRecord, String fieldName) {
        Record schema = avroRecord.schema();
        List<Field> fields = schema.fields();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (field.name().equals(fieldName)) {
                return avroRecord.get(i);
            }
        }
        return null;
    }

    private static Number requiredNumber(Object value, String field) {
        if (value == null) {
            throw new IcebergFormatException("manifest entry has no " + field);
        }
        return (Number) value;
    }

    private static int intValue(Object value) {
        return ((Number) value).intValue();
    }
}
