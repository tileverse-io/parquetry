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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Builds an Iceberg format-version=3 table with row lineage enabled and three data files across two
 * commits, then verifies the synthesized row lineage columns against Iceberg's own scan planner.
 *
 * <p>The table {@code events(id, value)} is unpartitioned. The first commit appends TWO files (A and
 * B) at once into a SINGLE manifest at data sequence number 1; the second commit appends ONE file
 * (C) into a second manifest at data sequence number 2. File A gets base {@code first_row_id} 0; file
 * B, the second entry in the same manifest, gets base 5 (the manifest base plus A's record count -
 * the cumulative within-manifest inheritance a reader must implement); file C gets base 10. No file
 * materializes a physical {@code _row_id} column. Every row's synthesized {@code _row_id} is its file
 * base plus its within-file 0-based position, and its synthesized {@code _last_updated_sequence_number}
 * is the data sequence number of the commit that appended it.
 *
 * <p>The user {@code id} runs 100..114 while {@code _row_id} runs 0..14, keeping the two columns
 * distinct. The authoritative per-row expected values come from Iceberg's scan planner
 * ({@code TableScan.planFiles} -> {@code DataFile.firstRowId()} and {@code dataSequenceNumber()});
 * the 1.11.0 generic reader does not project the metadata columns.
 */
public final class FreshRowLineageFixtureGenerator {

    private static final Schema TABLE_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "value", Types.DoubleType.get()));

    private static final int ROWS_PER_FILE = 5;

    private static final long FIRST_ID = 100L;

    public static void main(String[] args) throws IOException {
        Path warehouse = resolveWarehouse(args);
        deleteRecursively(warehouse);
        Files.createDirectories(warehouse);
        Table table = createTable(warehouse);
        appendTwoFilesInOneCommit(table);
        appendOneFileInOneCommit(table);
        reportRowLineageProperties(table);
        verifyWithOracle(table);
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

    /**
     * The first commit appends TWO data files at once. Both land in a SINGLE manifest; the second
     * entry's row-id base is then the manifest base plus the first entry's record count (the
     * cumulative within-manifest inheritance the reader must implement). Both files commit at data
     * sequence number 1.
     */
    private static void appendTwoFilesInOneCommit(Table table) throws IOException {
        DataFile fileA = writeDataFile(table, "data/data-file-a.parquet", FIRST_ID, ROWS_PER_FILE);
        DataFile fileB = writeDataFile(table, "data/data-file-b.parquet", FIRST_ID + ROWS_PER_FILE, ROWS_PER_FILE);
        table.newAppend().appendFile(fileA).appendFile(fileB).commit();
    }

    /** The second commit appends ONE data file into its own manifest, at data sequence number 2. */
    private static void appendOneFileInOneCommit(Table table) throws IOException {
        DataFile fileC = writeDataFile(table, "data/data-file-c.parquet", FIRST_ID + 2 * ROWS_PER_FILE, ROWS_PER_FILE);
        table.newAppend().appendFile(fileC).commit();
    }

    private static DataFile writeDataFile(Table table, String relativePath, long firstId, int rowCount)
            throws IOException {
        List<Record> rows = dataRows(table.schema(), firstId, rowCount);
        OutputFile out = localOutput(table, relativePath);
        FileWriterFactory<Record> factory =
                new GenericFileWriterFactory.Builder(table).build();
        DataWriter<Record> writer = factory.newDataWriter(encrypt(out), table.spec(), null);
        try (Closeable toClose = writer) {
            writer.write(rows);
        }
        return writer.toDataFile();
    }

    private static List<Record> dataRows(Schema schema, long firstId, int rowCount) {
        List<Record> rows = new ArrayList<>();
        for (long id = firstId; id < firstId + rowCount; id++) {
            rows.add(row(schema, id, id * 1.5));
        }
        return rows;
    }

    private static Record row(Schema schema, long id, double value) {
        Record record = GenericRecord.create(schema);
        record.setField("id", id);
        record.setField("value", value);
        return record;
    }

    /**
     * The v3 table has row lineage on by default. In 1.11.0 that shows up as a top-level {@code
     * next-row-id} in the metadata and a per-snapshot {@code first-row-id} base, both printed by the
     * scan oracle below; there is no explicit table property to set.
     */
    private static void reportRowLineageProperties(Table table) {
        System.out.println("TABLE-PROPERTIES=" + table.properties());
    }

    /**
     * Iceberg's own scan planner assigns each data file its {@code firstRowId()} base and its {@code
     * dataSequenceNumber()}. The 1.11.0 generic reader ({@code IcebergGenerics}) does not project the
     * synthesized {@code _row_id} / {@code _last_updated_sequence_number} metadata columns (row
     * lineage synthesis on read is an engine concern). The authoritative per-row expected values are
     * derived here from those file-level bases: {@code _row_id = firstRowId + within-file 0-based
     * position} and {@code _last_updated_sequence_number = dataSequenceNumber}.
     *
     * <p>The user {@code id} is offset from {@code _row_id} by {@code FIRST_ID}: rows are written in
     * id order across the whole dataset, and the row at global position p has {@code _row_id == p}
     * and {@code id == FIRST_ID + p}. This keeps {@code _row_id != id}, defeating a reader test that
     * confuses the two columns.
     */
    private static void verifyWithOracle(Table table) throws IOException {
        List<FileScanTask> tasks = scanTasksInFirstRowIdOrder(table);
        System.out.println("id\t_row_id\t_last_updated_sequence_number");
        for (FileScanTask task : tasks) {
            printFileOracle(task);
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

    private static void printFileOracle(FileScanTask task) {
        DataFile file = task.file();
        long firstRowId = file.firstRowId();
        long dataSequenceNumber = file.dataSequenceNumber();
        long recordCount = file.recordCount();
        System.out.println("ORACLE-FILE path=" + file.location() + " first_row_id=" + firstRowId
                + " data_sequence_number=" + dataSequenceNumber + " record_count=" + recordCount);
        for (long position = 0; position < recordCount; position++) {
            long rowId = firstRowId + position;
            long id = FIRST_ID + rowId;
            System.out.println(
                    "ORACLE-ROW id=" + id + " _row_id=" + rowId + " _last_updated_sequence_number=" + dataSequenceNumber);
        }
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

    private FreshRowLineageFixtureGenerator() {}
}
