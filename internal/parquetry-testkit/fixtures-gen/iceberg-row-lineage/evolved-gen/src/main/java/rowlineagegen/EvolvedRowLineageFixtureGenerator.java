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
 * Produces a format-version=3 table whose two data files sit on opposite sides of a schema evolution,
 * which makes one of them reconcile on read while the other passes through.
 *
 * <p>Commit 1 appends the legacy file under the original schema {@code events(id, value)}, with no
 * materialized lineage column: every row's lineage is synthesized from the file's {@code first_row_id}
 * base plus the within-file position, and from the file's data sequence number. The schema then
 * evolves: {@code value} is renamed to {@code amount} and a {@code label} column is added, which
 * leaves the legacy file short of one table field and gives another its old name. Commit 2
 * appends a file written against the evolved schema that PHYSICALLY materializes BOTH reserved
 * lineage columns, {@code _row_id} (field id 2147483540) and {@code _last_updated_sequence_number}
 * (field id 2147483539), each as a nullable column with its own mix of stored and null cells.
 *
 * <p>The two mixes are deliberately staggered: {@code _row_id} stores on odd within-file positions
 * and {@code _last_updated_sequence_number} on even ones. A reader that confuses the two columns
 * therefore disagrees with the oracle on every row.
 *
 * <p>The user {@code id} runs 200..209 while {@code _row_id} runs 0..9, keeping the two apart.
 */
public final class EvolvedRowLineageFixtureGenerator {

    private static final Schema ORIGINAL_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "value", Types.DoubleType.get()));

    private static final int ROWS_PER_FILE = 5;

    private static final long FIRST_ID = 200L;

    private static final long MATERIALIZED_ROW_ID_BASE = 1000L;

    private static final long MATERIALIZED_SEQUENCE_BASE = 7000L;

    public static void main(String[] args) throws IOException {
        Path warehouse = resolveWarehouse(args);
        deleteRecursively(warehouse);
        Files.createDirectories(warehouse);
        Table table = createTable(warehouse);
        appendLegacyFile(table, "data/data-file-legacy.parquet", FIRST_ID, ROWS_PER_FILE);
        evolveSchema(table);
        appendMaterializedFile(table, "data/data-file-evolved.parquet", FIRST_ID + ROWS_PER_FILE, ROWS_PER_FILE);
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
                ORIGINAL_SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(
                        "format-version", "3",
                        "write.parquet.compression-codec", "uncompressed",
                        "write.metadata.compression-codec", "none"),
                location);
    }

    /**
     * Renames {@code value} to {@code amount} and adds {@code label}. The rename keeps field id 2, which
     * leaves the legacy file matching the table by id while disagreeing by name; the added column has no
     * counterpart in that file at all. A schema update writes a new metadata document without a
     * snapshot, which leaves the data sequence numbers of the two commits at 1 and 2.
     */
    private static void evolveSchema(Table table) {
        table.updateSchema()
                .renameColumn("value", "amount")
                .addColumn("label", Types.StringType.get())
                .commit();
        table.refresh();
    }

    private static void appendLegacyFile(Table table, String relativePath, long firstId, int rowCount)
            throws IOException {
        List<Record> rows = legacyRows(table.schema(), firstId, rowCount);
        OutputFile out = localOutput(table, relativePath);
        FileWriterFactory<Record> factory =
                new GenericFileWriterFactory.Builder(table).build();
        DataWriter<Record> writer = factory.newDataWriter(encrypt(out), table.spec(), null);
        try (Closeable toClose = writer) {
            writer.write(rows);
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
    }

    private static List<Record> legacyRows(Schema schema, long firstId, int rowCount) {
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
     * Writes a data file against the evolved schema plus both reserved lineage fields, then appends it.
     * The DataFile is committed with an explicit metrics record count to avoid a reader-side schema
     * surprise.
     */
    private static void appendMaterializedFile(Table table, String relativePath, long firstId, int rowCount)
            throws IOException {
        Schema writeSchema = schemaWithLineageColumns(table.schema());
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

    private static Schema schemaWithLineageColumns(Schema tableSchema) {
        List<NestedField> fields = new ArrayList<>(tableSchema.columns());
        fields.add(MetadataColumns.ROW_ID);
        fields.add(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);
        return new Schema(fields);
    }

    /**
     * A MERGE-with-insert shape on both lineage columns, staggered against each other. Odd within-file
     * positions store a {@code _row_id}; even positions store a {@code _last_updated_sequence_number}.
     * A stored cell must win on read, and a null cell must resolve to the file's base, which is the
     * {@code first_row_id} plus the position for one column and the data sequence number for the other.
     */
    private static List<Record> materializedRows(Schema writeSchema, long firstId, int rowCount) {
        List<Record> rows = new ArrayList<>();
        for (int offset = 0; offset < rowCount; offset++) {
            long id = firstId + offset;
            Record record = GenericRecord.create(writeSchema);
            record.setField("id", id);
            record.setField("amount", id * 1.5);
            record.setField("label", "row-" + id);
            record.setField(MetadataColumns.ROW_ID.name(), storedRowIdOrNull(offset));
            record.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), storedSequenceOrNull(offset));
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

    private static Long storedSequenceOrNull(int offset) {
        boolean stored = offset % 2 == 0;
        if (stored) {
            return MATERIALIZED_SEQUENCE_BASE + offset;
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
        System.out.println("ORACLE-SCHEMA=" + table.schema());
    }

    /**
     * Reads each data file with a projection that INCLUDES both reserved lineage fields, using the same
     * {@code idToConstant} base that Iceberg's planner assigns. This is the read path that resolves the
     * coalesce: a stored physical cell wins over the synthesized base.
     */
    private static void readLineageOracle(Table table) throws IOException {
        Schema projection = schemaWithLineageColumns(table.schema());
        System.out.println("id\t_row_id\t_last_updated_sequence_number");
        for (FileScanTask task : scanTasksInFirstRowIdOrder(table)) {
            readFileLineage(table, task, projection);
        }
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
                .createReaderFunc(messageType -> org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                        projection, messageType, idToConstant))
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
        System.out.println(
                "ORACLE-ROW id=" + id + " _row_id=" + rowId + " _last_updated_sequence_number=" + lastUpdated);
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
        try (java.util.stream.Stream<Path> walk = Files.walk(root)) {
            walk.forEach(paths::add);
        }
        paths.sort(Comparator.reverseOrder());
        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
    }

    private EvolvedRowLineageFixtureGenerator() {}
}
