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
 * Reads an Iceberg snapshot's manifest list and its manifests (Avro) into the data files and merge-on-read delete files
 * the snapshot references. Each entry's data sequence number is resolved by inheritance (an entry that records no
 * sequence number inherits its manifest's), which the delete planner needs to decide which deletes apply to a data
 * file.
 *
 * <p>This reader holds no table schema. It keeps each data file's bounds as raw, field-id-keyed bytes and leaves
 * decoding them to {@link IcebergFileStats}, which has the schema; see {@link DataFileRef}.
 */
final class IcebergManifests {

    private static final int CONTENT_DATA = 0;
    private static final int CONTENT_POSITION_DELETES = 1;
    private static final int CONTENT_EQUALITY_DELETES = 2;
    private static final int STATUS_DELETED = 2;
    private static final String FILE_PATH = "file_path";
    private static final String RECORD_COUNT = "record_count";

    private IcebergManifests() {}

    /**
     * A surviving data file: its location, the row count the manifest records, its data sequence number, the raw
     * per-column statistics a later pruning step consumes, and the partition tuple.
     *
     * <p>The bound maps, the null-count map, and the partition tuple mirror the manifest's on-disk shape, each an
     * Iceberg {@code map<field-id, value>}: a missing key means the manifest recorded no such value for that column.
     * The keys are field ids rather than column names because a field id is all the manifest records; resolving a
     * column's name and type needs the table schema, which this reader does not hold ({@link IcebergFileStats#from}
     * performs that join).
     *
     * <p>A lower/upper bound value stays the raw bound bytes as a {@link MemorySegment} (a zero-copy view of the Avro
     * record) until the schema-aware layer decodes it best-effort by the column's Iceberg type, skipping any bound it
     * cannot decode rather than failing the read.
     *
     * @param location the absolute data-file location
     * @param recordCount the number of rows the manifest records for this file
     * @param dataSequenceNumber the file's data sequence number, inherited from its manifest when the entry omits it
     * @param lowerBounds per-column lower bounds keyed by field id; a missing key means no recorded bound
     * @param upperBounds per-column upper bounds keyed by field id; a missing key means no recorded bound
     * @param nullValueCounts per-column null counts keyed by field id
     * @param partitionValues the partition tuple keyed by Iceberg partition-field-id; each value is the raw Avro value
     *     (for an identity partition, the source column's value) and a null tuple slot is omitted
     */
    public record DataFileRef(
            String location,
            long recordCount,
            long dataSequenceNumber,
            Map<Integer, MemorySegment> lowerBounds,
            Map<Integer, MemorySegment> upperBounds,
            Map<Integer, Long> nullValueCounts,
            Map<Integer, Object> partitionValues) {

        public DataFileRef {
            Objects.requireNonNull(location, "location");
            lowerBounds = Map.copyOf(lowerBounds);
            upperBounds = Map.copyOf(upperBounds);
            nullValueCounts = Map.copyOf(nullValueCounts);
            partitionValues = Map.copyOf(partitionValues);
        }
    }

    /**
     * A merge-on-read delete file, either positional or equality.
     *
     * <p>A positional delete file is a Parquet file of {@code (file_path, pos)} rows; {@code pos} is an absolute row
     * position within the data file named by {@code file_path}. The delete planner applies it to a data file by
     * matching that path and by the sequence rule (a positional delete applies to data files at or before its own
     * sequence). Its {@code equalityFieldIds} list is empty.
     *
     * <p>An equality delete file is a Parquet file whose columns are the equality-id fields named by
     * {@code equalityFieldIds}; each row is a tuple of values to delete. The delete planner applies it to a data file
     * by partition match and the strict sequence rule (an equality delete applies to data files strictly before its own
     * sequence). It has no {@code referencedDataFile}.
     *
     * @param location the absolute delete-file location
     * @param content the Iceberg file content code (1 for position deletes, 2 for equality deletes)
     * @param dataSequenceNumber the delete file's data sequence number, inherited from its manifest when omitted
     * @param referencedDataFile the single data file a positional delete is scoped to, or {@code null} when the delete
     *     file lists its targets in its own {@code file_path} column instead (the writer may record either)
     * @param recordCount the number of delete rows the manifest records
     * @param equalityFieldIds the Iceberg field ids of the equality fields for an equality delete file, in delete-file
     *     column order; empty for a positional delete file
     * @param partitionValues the delete file's partition tuple keyed by Iceberg partition-field-id, mirroring
     *     {@link DataFileRef#partitionValues()}; an equality delete applies only to data files with a matching
     *     partition
     */
    public record DeleteFileRef(
            String location,
            int content,
            long dataSequenceNumber,
            String referencedDataFile,
            long recordCount,
            List<Integer> equalityFieldIds,
            Map<Integer, Object> partitionValues) {

        public DeleteFileRef {
            Objects.requireNonNull(location, "location");
            equalityFieldIds = List.copyOf(equalityFieldIds);
            partitionValues = Map.copyOf(partitionValues);
        }

        /** Whether this delete file deletes by equality tuples rather than by absolute row position. */
        public boolean isEqualityDelete() {
            return content == CONTENT_EQUALITY_DELETES;
        }
    }

    /** The data files and merge-on-read delete files a single snapshot references. */
    public record Snapshot(List<DataFileRef> dataFiles, List<DeleteFileRef> deleteFiles) {

        public Snapshot {
            dataFiles = List.copyOf(dataFiles);
            deleteFiles = List.copyOf(deleteFiles);
        }
    }

    /** Reads the data files and delete files referenced by {@code manifestListLocation}. */
    public static Snapshot readSnapshot(String manifestListLocation, IcebergFileIO io) {
        List<ManifestFileRef> manifests = readManifestFiles(manifestListLocation, io);
        List<DataFileRef> dataFiles = new ArrayList<>();
        List<DeleteFileRef> deleteFiles = new ArrayList<>();
        // Each manifest is an independent I/O (one object-store read). Reading them serially stalls on round-trip
        // latency once a table has many manifests on cloud storage; parallelize across virtual threads here when the
        // cloud IcebergFileIO lands. The per-manifest appends keep this order-preserving when that happens.
        for (ManifestFileRef manifest : manifests) {
            readManifestEntries(manifest, io, dataFiles, deleteFiles);
        }
        return new Snapshot(dataFiles, deleteFiles);
    }

    /** Reads every surviving data file referenced by {@code manifestListLocation}, ignoring any delete files. */
    public static List<DataFileRef> readDataFiles(String manifestListLocation, IcebergFileIO io) {
        return readSnapshot(manifestListLocation, io).dataFiles();
    }

    /** One manifest the manifest list references, with the sequence number its ADDED entries inherit. */
    private record ManifestFileRef(String location, long sequenceNumber) {}

    private static List<ManifestFileRef> readManifestFiles(String manifestListLocation, IcebergFileIO io) {
        List<ManifestFileRef> manifests = new ArrayList<>();
        try (ByteRangeSource source = io.open(manifestListLocation);
                AvroDataFileReader reader = AvroDataFileReader.open(source);
                Stream<AvroRecord> entries = reader.records()) {
            entries.forEach(entry -> {
                String path = (String) entry.get("manifest_path");
                long sequenceNumber = longOrDefault(safeGet(entry, "sequence_number"), 0L);
                manifests.add(new ManifestFileRef(path, sequenceNumber));
            });
        }
        return manifests;
    }

    private static void readManifestEntries(
            ManifestFileRef manifest, IcebergFileIO io, List<DataFileRef> dataFiles, List<DeleteFileRef> deleteFiles) {
        try (ByteRangeSource source = io.open(manifest.location());
                AvroDataFileReader reader = AvroDataFileReader.open(source);
                Stream<AvroRecord> entries = reader.records()) {
            entries.forEach(entry -> {
                int status = requiredNumber(entry.get("status"), "status").intValue();
                if (status == STATUS_DELETED) {
                    return;
                }
                long sequenceNumber = inheritedSequenceNumber(entry, manifest);
                AvroRecord dataFile = (AvroRecord) entry.get("data_file");
                addClassified(dataFile, sequenceNumber, dataFiles, deleteFiles);
            });
        }
    }

    private static void addClassified(
            AvroRecord dataFile, long sequenceNumber, List<DataFileRef> dataFiles, List<DeleteFileRef> deleteFiles) {
        int content = intOrDefault(safeGet(dataFile, "content"), CONTENT_DATA);
        switch (content) {
            case CONTENT_DATA -> dataFiles.add(readDataFile(dataFile, sequenceNumber));
            case CONTENT_POSITION_DELETES, CONTENT_EQUALITY_DELETES ->
                deleteFiles.add(readDeleteFile(dataFile, content, sequenceNumber));
            default -> throw new IcebergFormatException("unknown manifest entry content " + content);
        }
    }

    /**
     * The entry's data sequence number. An entry that records no sequence number (a freshly ADDED entry) inherits its
     * manifest's sequence number from the manifest list; an EXISTING entry that records one keeps it.
     */
    private static long inheritedSequenceNumber(AvroRecord entry, ManifestFileRef manifest) {
        Object explicit = safeGet(entry, "sequence_number");
        if (explicit != null) {
            return ((Number) explicit).longValue();
        }
        return manifest.sequenceNumber();
    }

    private static DataFileRef readDataFile(AvroRecord dataFile, long sequenceNumber) {
        String location = (String) dataFile.get(FILE_PATH);
        long recordCount =
                requiredNumber(dataFile.get(RECORD_COUNT), RECORD_COUNT).longValue();
        Map<Integer, MemorySegment> lowerBounds =
                readIntKeyedMap(safeGet(dataFile, "lower_bounds"), MemorySegment.class::cast);
        Map<Integer, MemorySegment> upperBounds =
                readIntKeyedMap(safeGet(dataFile, "upper_bounds"), MemorySegment.class::cast);
        Map<Integer, Long> nullValueCounts =
                readIntKeyedMap(safeGet(dataFile, "null_value_counts"), value -> ((Number) value).longValue());
        Object partition = safeGet(dataFile, "partition");
        Map<Integer, Object> partitionValues =
                partition instanceof AvroRecord partitionRecord ? readPartitionTuple(partitionRecord) : Map.of();
        return new DataFileRef(
                location, recordCount, sequenceNumber, lowerBounds, upperBounds, nullValueCounts, partitionValues);
    }

    /**
     * Reads a delete-file entry, either positional (content 1) or equality (content 2). A deletion vector (v3) is also
     * content 1 but locates a Puffin blob through {@code content_offset}; that form is not yet supported and fails fast
     * rather than reading a non-existent Parquet file.
     */
    private static DeleteFileRef readDeleteFile(AvroRecord dataFile, int content, long sequenceNumber) {
        if (content == CONTENT_POSITION_DELETES && safeGet(dataFile, "content_offset") != null) {
            throw new IcebergFormatException("deletion vectors are not yet supported: " + dataFile.get(FILE_PATH));
        }
        String location = (String) dataFile.get(FILE_PATH);
        long recordCount =
                requiredNumber(dataFile.get(RECORD_COUNT), RECORD_COUNT).longValue();
        String referencedDataFile = (String) safeGet(dataFile, "referenced_data_file");
        List<Integer> equalityFieldIds = readEqualityFieldIds(dataFile);
        Object partition = safeGet(dataFile, "partition");
        Map<Integer, Object> partitionValues =
                partition instanceof AvroRecord partitionRecord ? readPartitionTuple(partitionRecord) : Map.of();
        return new DeleteFileRef(
                location, content, sequenceNumber, referencedDataFile, recordCount, equalityFieldIds, partitionValues);
    }

    /**
     * The Iceberg field ids of an equality delete file's equality fields, from the manifest's {@code equality_ids}
     * array, in delete-file column order. A positional delete entry records no such array and yields an empty list.
     */
    private static List<Integer> readEqualityFieldIds(AvroRecord dataFile) {
        Object equalityIds = safeGet(dataFile, "equality_ids");
        if (!(equalityIds instanceof List<?> ids)) {
            return List.of();
        }
        List<Integer> fieldIds = new ArrayList<>(ids.size());
        for (Object id : ids) {
            fieldIds.add(((Number) id).intValue());
        }
        return fieldIds;
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

    /**
     * Reads a data file's partition tuple from the manifest's {@code partition} struct record, keyed by Iceberg
     * partition-field-id. Each tuple slot is an Avro field whose {@code field-id} attribute is the partition-field-id;
     * when that attribute is absent, fall back to the field's position offset by Iceberg's partition-field-id base
     * (1000) following spec order. A null slot is omitted.
     */
    private static Map<Integer, Object> readPartitionTuple(AvroRecord partitionRecord) {
        Map<Integer, Object> tuple = new LinkedHashMap<>();
        List<Field> fields = partitionRecord.schema().fields();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            Object value = partitionRecord.get(i);
            if (value == null) {
                continue;
            }
            int key = field.fieldId().orElse(1000 + i);
            tuple.put(key, value);
        }
        return tuple;
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

    private static int intOrDefault(Object value, int fallback) {
        return value == null ? fallback : ((Number) value).intValue();
    }

    private static long longOrDefault(Object value, long fallback) {
        return value == null ? fallback : ((Number) value).longValue();
    }
}
