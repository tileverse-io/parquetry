/*
 * Copyright 2026 Tileverse.io
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
package rowlineagegen;

import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Produces a data file that PHYSICALLY materializes the reserved {@code _row_id} column (field id
 * 2147483540) as a nullable column with a MIX of stored and null cells, then reads it back through
 * Iceberg's own coalesce path to prove per-row resolution: a stored cell wins, a null cell resolves
 * to the file's {@code first_row_id} base plus the within-file position.
 *
 * <p>Commit 1 appends a normal data file (ids 0..4, no materialized {@code _row_id}); its rows read
 * back with synthesized ids 0..4. Commit 2 appends a data file written against a schema that adds the
 * optional {@code _row_id} field, with a MERGE-with-insert shape: odd within-file positions store a
 * value (1001, 1003) that differs from the synthesized formula, even positions store null. Against
 * the file's {@code first_row_id} base 5 the read oracle yields ids 5, 1001, 7, 1003, 9.
 */
public final class MaterializedRowLineageFixtureGenerator {

    private static final Schema TABLE_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "value", Types.DoubleType.get()));

    private static final int ROWS_PER_FILE = 5;

    private static final long MATERIALIZED_ROW_ID_BASE = 1000L;

    public static void main(String[] args) throws IOException {
        Path warehouse = resolveWarehouse(args);
        deleteRecursively(warehouse);
        Files.createDirectories(warehouse);
        Table table = createTable(warehouse);
        appendSynthesizedFile(table, "data/data-file-0.parquet", 0L, ROWS_PER_FILE);
        appendMaterializedFile(table, "data/data-file-1-materialized.parquet", ROWS_PER_FILE, ROWS_PER_FILE);
        reportLiveRows(table);
        readLineageOracle(table);
        System.out.println("WAREHOUSE=" + warehouse.resolve("events"));
    }

    private static Path resolveWarehouse(String[] args) {
        if (args.length > 0) {
            return Path.of(args[0]);
        }
        return Path.of("work", "warehouse").toAbsolutePath();
    }

    private static Table createTable(Path warehouse) {
        HadoopTables tables = new HadoopTables(new Configuration());
        String location = warehouse.toUri().toString() + "events";
        return tables.create(
                TABLE_SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(
                        "format-version", "3",
                        "write.parquet.compression-codec", "uncompressed",
                        "write.metadata.compression-codec", "none"),
                location);
    }

    private static void appendSynthesizedFile(Table table, String relativePath, long firstId, int rowCount)
            throws IOException {
        List<Record> rows = tableRows(table.schema(), firstId, rowCount);
        OutputFile out = localOutput(table, relativePath);
        FileWriterFactory<Record> factory =
                new GenericFileWriterFactory.Builder(table).build();
        DataWriter<Record> writer = factory.newDataWriter(encrypt(out), table.spec(), null);
        try (Closeable toClose = writer) {
            writer.write(rows);
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
    }

    private static List<Record> tableRows(Schema schema, long firstId, int rowCount) {
        List<Record> rows = new ArrayList<>();
        for (long id = firstId; id < firstId + rowCount; id++) {
            Record record = GenericRecord.create(schema);
            record.setField("id", id);
            record.setField("value", id * 1.5);
            rows.add(record);
        }
        return rows;
    }

    /**
     * Writes a data file against a schema that adds the reserved {@code _row_id} field as an optional
     * column with a mix of stored and null cells, then appends it. The DataFile is committed with an
     * explicit metrics record count to avoid a reader-side schema surprise.
     */
    private static void appendMaterializedFile(Table table, String relativePath, long firstId, int rowCount)
            throws IOException {
        Schema writeSchema = schemaWithRowId(table.schema());
        List<Record> rows = materializedRows(writeSchema, firstId, rowCount);
        OutputFile out = localOutput(table, relativePath);
        Metrics metrics = writeParquet(writeSchema, encrypt(out), rows);
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(out.location())
                .withFileSizeInBytes(fileSize(out))
                .withFormat(FileFormat.PARQUET)
                .withMetrics(metrics)
                .build();
        table.newAppend().appendFile(dataFile).commit();
    }

    private static Schema schemaWithRowId(Schema tableSchema) {
        List<NestedField> fields = new ArrayList<>(tableSchema.columns());
        fields.add(MetadataColumns.ROW_ID);
        return new Schema(fields);
    }

    /**
     * A MERGE-with-insert shape: the physical {@code _row_id} column has a mix of stored and null
     * cells. Odd within-file positions store a value (which must win on read); even positions store
     * null (which resolves on read to the file's {@code first_row_id} base plus the position). This
     * exercises true per-row coalesce rather than an all-stored column.
     */
    private static List<Record> materializedRows(Schema writeSchema, long firstId, int rowCount) {
        List<Record> rows = new ArrayList<>();
        for (int offset = 0; offset < rowCount; offset++) {
            long id = firstId + offset;
            Record record = GenericRecord.create(writeSchema);
            record.setField("id", id);
            record.setField("value", id * 1.5);
            record.setField(MetadataColumns.ROW_ID.name(), storedRowIdOrNull(offset));
            rows.add(record);
        }
        return rows;
    }

    private static Long storedRowIdOrNull(int offset) {
        boolean stored = offset % 2 == 1;
        if (stored) {
            return MATERIALIZED_ROW_ID_BASE + offset;
        }
        return null;
    }

    private static Metrics writeParquet(Schema writeSchema, EncryptedOutputFile out, List<Record> rows)
            throws IOException {
        FileAppender<Record> appender = Parquet.write(out.encryptingOutputFile())
                .schema(writeSchema)
                .createWriterFunc(messageType -> GenericParquetWriter.create(writeSchema, messageType))
                .overwrite()
                .build();
        try (Closeable toClose = appender) {
            appender.addAll(rows);
        }
        return appender.metrics();
    }

    private static void reportLiveRows(Table table) throws IOException {
        List<Long> ids = new ArrayList<>();
        try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
            for (Record record : reader) {
                ids.add((Long) record.getField("id"));
            }
        }
        ids.sort(Comparator.naturalOrder());
        System.out.println("ORACLE-LIVE-IDS=" + ids);
    }

    /**
     * Reads each data file with a projection that INCLUDES the reserved {@code _row_id} and {@code
     * _last_updated_sequence_number} fields, using the same {@code idToConstant} base that Iceberg's
     * planner assigns. This is the read path that resolves the coalesce (stored physical column wins
     * over the synthesized {@code first_row_id + position}); the generic {@code IcebergGenerics}
     * reader does not project these metadata columns.
     */
    private static void readLineageOracle(Table table) throws IOException {
        Schema projection = schemaWithLineageColumns(table.schema());
        System.out.println("id\t_row_id\t_last_updated_sequence_number");
        for (FileScanTask task : scanTasksInFirstRowIdOrder(table)) {
            readFileLineage(table, task, projection);
        }
    }

    private static Schema schemaWithLineageColumns(Schema tableSchema) {
        List<NestedField> fields = new ArrayList<>(tableSchema.columns());
        fields.add(MetadataColumns.ROW_ID);
        fields.add(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);
        return new Schema(fields);
    }

    private static List<FileScanTask> scanTasksInFirstRowIdOrder(Table table) throws IOException {
        List<FileScanTask> tasks = new ArrayList<>();
        TableScan scan = table.newScan();
        try (CloseableIterable<FileScanTask> planned = scan.planFiles()) {
            planned.forEach(tasks::add);
        }
        tasks.sort(Comparator.comparingLong(task -> task.file().firstRowId()));
        return tasks;
    }

    private static void readFileLineage(Table table, FileScanTask task, Schema projection) throws IOException {
        DataFile file = task.file();
        System.out.println("ORACLE-FILE path=" + shortName(file.location()) + " first_row_id=" + file.firstRowId()
                + " data_sequence_number=" + file.dataSequenceNumber() + " record_count=" + file.recordCount());
        Map<Integer, Object> idToConstant = lineageConstants(file);
        CloseableIterable<Record> records = Parquet.read(table.io().newInputFile(file.location()))
                .project(projection)
                .createReaderFunc(messageType ->
                        org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(projection, messageType, idToConstant))
                .build();
        try (CloseableIterable<Record> toClose = records) {
            for (Record record : toClose) {
                printLineageRow(record);
            }
        }
    }

    private static Map<Integer, Object> lineageConstants(DataFile file) {
        Map<Integer, Object> idToConstant = new HashMap<>();
        idToConstant.put(MetadataColumns.ROW_ID.fieldId(), file.firstRowId());
        idToConstant.put(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(), file.dataSequenceNumber());
        return idToConstant;
    }

    private static void printLineageRow(Record record) {
        Object id = record.getField("id");
        Object rowId = record.getField(MetadataColumns.ROW_ID.name());
        Object lastUpdated = record.getField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());
        System.out.println("ORACLE-ROW id=" + id + " _row_id=" + rowId + " _last_updated_sequence_number=" + lastUpdated);
    }

    private static String shortName(String location) {
        int slash = location.lastIndexOf('/');
        return slash < 0 ? location : location.substring(slash + 1);
    }

    private static long fileSize(OutputFile out) {
        return out.toInputFile().getLength();
    }

    private static OutputFile localOutput(Table table, String relativePath) throws IOException {
        String tablePath = table.location().replace("file:", "");
        Path file = Path.of(tablePath, relativePath);
        Files.createDirectories(file.getParent());
        return org.apache.iceberg.Files.localOutput(file.toFile());
    }

    private static EncryptedOutputFile encrypt(OutputFile out) {
        return EncryptedFiles.encryptedOutput(out, EncryptionKeyMetadata.EMPTY);
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        List<Path> paths = new ArrayList<>();
        Files.walk(root).forEach(paths::add);
        paths.sort(Comparator.reverseOrder());
        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
    }

    private MaterializedRowLineageFixtureGenerator() {}
}
