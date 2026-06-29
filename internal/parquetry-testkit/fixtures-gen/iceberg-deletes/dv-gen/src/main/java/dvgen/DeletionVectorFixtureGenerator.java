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
package dvgen;

import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Builds an Iceberg format-version=3 merge-on-read DELETION-VECTOR table directly through the
 * Iceberg Java API (no Spark, no Flink) and verifies the live-row set with Iceberg's own reader.
 *
 * <p>The table {@code events(id, category, value)} is unpartitioned. One data file of 100 rows is
 * appended (data sequence number 1), then one deletion vector for that data file is written to a
 * Puffin file with {@link BaseDVFileWriter} and committed through {@code newRowDelta} (delete
 * sequence number 2). The deleted positions are a contiguous run, scattered singletons, and the
 * tail; the absolute row position equals {@code id} because the rows are written in id order.
 */
public final class DeletionVectorFixtureGenerator {

    private static final Schema TABLE_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "category", Types.StringType.get()),
            NestedField.optional(3, "value", Types.DoubleType.get()));

    private static final int ROW_COUNT = 100;

    private static final long[] DELETED_POSITIONS = {10L, 11L, 12L, 13L, 14L, 50L, 73L, 97L, 98L, 99L};

    public static void main(String[] args) throws IOException {
        Path warehouse = resolveWarehouse(args);
        deleteRecursively(warehouse);
        Files.createDirectories(warehouse);
        Table table = createTable(warehouse);
        DataFile dataFile = appendData(table);
        commitDeletionVector(table, dataFile);
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
                        "write.delete.mode", "merge-on-read",
                        "write.update.mode", "merge-on-read",
                        "write.merge.mode", "merge-on-read",
                        "write.parquet.compression-codec", "uncompressed",
                        "write.metadata.compression-codec", "none"),
                location);
    }

    private static DataFile appendData(Table table) throws IOException {
        List<Record> rows = dataRows(table.schema());
        OutputFile out = localOutput(table, "data/data-events.parquet");
        FileWriterFactory<Record> factory =
                new GenericFileWriterFactory.Builder(table).build();
        DataWriter<Record> writer = factory.newDataWriter(encrypt(out), table.spec(), null);
        try (Closeable toClose = writer) {
            writer.write(rows);
        }
        DataFile dataFile = writer.toDataFile();
        table.newAppend().appendFile(dataFile).commit();
        return dataFile;
    }

    /**
     * One hundred rows in id order; {@code value = id * 1.5} and {@code category} cycles over a, b, c
     * by {@code id % 3}. Writing in id order makes the absolute row position equal to the id, which
     * is what the deletion vector references.
     */
    private static List<Record> dataRows(Schema schema) {
        List<Record> rows = new ArrayList<>();
        for (long id = 0; id < ROW_COUNT; id++) {
            rows.add(row(schema, id, categoryFor(id), id * 1.5));
        }
        return rows;
    }

    private static String categoryFor(long id) {
        long bucket = id % 3;
        if (bucket == 0) {
            return "a";
        }
        if (bucket == 1) {
            return "b";
        }
        return "c";
    }

    private static Record row(Schema schema, long id, String category, double value) {
        Record record = GenericRecord.create(schema);
        record.setField("id", id);
        record.setField("category", category);
        record.setField("value", value);
        return record;
    }

    private static void commitDeletionVector(Table table, DataFile dataFile) throws IOException {
        DeleteWriteResult result = writeDeletionVector(table, dataFile);
        commitDeletes(table, result);
    }

    private static DeleteWriteResult writeDeletionVector(Table table, DataFile dataFile) throws IOException {
        OutputFileFactory puffinFiles = OutputFileFactory.builderFor(table, 1, 1)
                .format(FileFormat.PUFFIN)
                .build();
        DVFileWriter dvWriter = new BaseDVFileWriter(puffinFiles, path -> noPreviousDeletes());
        try (Closeable toClose = dvWriter) {
            for (long position : DELETED_POSITIONS) {
                dvWriter.delete(dataFile.location(), position, table.spec(), null);
            }
        }
        return dvWriter.result();
    }

    private static PositionDeleteIndex noPreviousDeletes() {
        return null;
    }

    private static void commitDeletes(Table table, DeleteWriteResult result) {
        org.apache.iceberg.RowDelta rowDelta = table.newRowDelta();
        result.deleteFiles().forEach(rowDelta::addDeletes);
        rowDelta.commit();
    }

    private static void verifyWithOracle(Table table) throws IOException {
        TreeSet<Long> liveIds = new TreeSet<>();
        try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
            for (Record record : reader) {
                liveIds.add((Long) record.getField("id"));
            }
        }
        TreeSet<Long> deletedIds = new TreeSet<>();
        for (long id = 0; id < ROW_COUNT; id++) {
            if (!liveIds.contains(id)) {
                deletedIds.add(id);
            }
        }
        printReport(liveIds, deletedIds);
        assertDeletedExactly(deletedIds);
    }

    private static void printReport(TreeSet<Long> liveIds, TreeSet<Long> deletedIds) {
        System.out.println("ORACLE-TOTAL-COUNT=" + ROW_COUNT);
        System.out.println("ORACLE-LIVE-COUNT=" + liveIds.size());
        System.out.println("ORACLE-DELETED-COUNT=" + deletedIds.size());
        System.out.println("ORACLE-DELETED-IDS=" + deletedIds);
    }

    private static void assertDeletedExactly(TreeSet<Long> deletedIds) {
        TreeSet<Long> expected = new TreeSet<>();
        for (long position : DELETED_POSITIONS) {
            expected.add(position);
        }
        if (!deletedIds.equals(expected)) {
            throw new IllegalStateException("oracle deleted-id set " + deletedIds + " != expected " + expected);
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

    private DeletionVectorFixtureGenerator() {}
}
