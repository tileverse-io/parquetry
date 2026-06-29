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
package equalitygen;

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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
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
 * Builds an Iceberg v2 merge-on-read EQUALITY-delete table directly through the Iceberg Java API
 * (no Spark, no Flink) and verifies the live-row set with Iceberg's own reader.
 *
 * <p>The table {@code events(id, category, flag, value)} is unpartitioned. One data file is
 * appended (data sequence number 1), then one equality-delete file keyed on {@code (category,
 * flag)} is committed through {@code newRowDelta} (delete sequence number 2). The equality match is
 * AND-over-fields within a tuple and OR-over the two delete tuples, with null-safe matching for a
 * tuple component that is NULL.
 */
public final class EqualityDeleteFixtureGenerator {

    private static final Schema TABLE_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "category", Types.StringType.get()),
            NestedField.optional(3, "flag", Types.StringType.get()),
            NestedField.optional(4, "value", Types.DoubleType.get()));

    public static void main(String[] args) throws IOException {
        Path warehouse = resolveWarehouse(args);
        deleteRecursively(warehouse);
        Files.createDirectories(warehouse);
        Table table = createTable(warehouse);
        appendData(table);
        commitEqualityDelete(table);
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
                        "format-version", "2",
                        "write.delete.mode", "merge-on-read",
                        "write.update.mode", "merge-on-read",
                        "write.merge.mode", "merge-on-read",
                        "write.parquet.compression-codec", "uncompressed",
                        "write.metadata.compression-codec", "none"),
                location);
    }

    private static void appendData(Table table) throws IOException {
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
    }

    /**
     * One labeled row per boundary the reader must distinguish, plus obvious filler live rows. The
     * id encodes the intent: ids that match a delete tuple are removed by the oracle, every other id
     * survives.
     */
    private static List<Record> dataRows(Schema schema) {
        List<Record> rows = new ArrayList<>();
        rows.add(row(schema, 1L, "a", "x", 10.0)); // matches T1 exactly -> DELETED
        rows.add(row(schema, 2L, null, "y", 20.0)); // matches T2 exactly (null key) -> DELETED
        rows.add(row(schema, 3L, null, "z", 30.0)); // null cell, T2 needs flag=y -> SURVIVES
        rows.add(row(schema, 4L, "b", "y", 40.0)); // T2 needs category IS NULL -> SURVIVES
        rows.add(row(schema, 5L, "a", null, 50.0)); // T1 needs flag=x -> SURVIVES
        rows.add(row(schema, 6L, "c", "x", 60.0)); // filler -> SURVIVES
        rows.add(row(schema, 7L, "c", "y", 70.0)); // filler -> SURVIVES
        rows.add(row(schema, 8L, "c", "z", 80.0)); // filler -> SURVIVES
        rows.add(row(schema, 9L, "a", "y", 90.0)); // category matches T1, flag does not -> SURVIVES
        rows.add(row(schema, 10L, "b", "x", 100.0)); // flag matches T1, category does not -> SURVIVES
        return rows;
    }

    private static Record row(Schema schema, long id, String category, String flag, double value) {
        Record record = GenericRecord.create(schema);
        record.setField("id", id);
        record.setField("category", category);
        record.setField("flag", flag);
        record.setField("value", value);
        return record;
    }

    private static void commitEqualityDelete(Table table) throws IOException {
        Schema deleteRowSchema = table.schema().select("category", "flag");
        DeleteFile deleteFile = writeEqualityDeleteFile(table, deleteRowSchema, deleteTuples(deleteRowSchema));
        table.newRowDelta().addDeletes(deleteFile).commit();
    }

    /**
     * The two delete tuples on {@code (category, flag)}: a both-non-null tuple and a tuple with a
     * NULL key component, to exercise null-safe equality matching.
     */
    private static List<Record> deleteTuples(Schema deleteRowSchema) {
        List<Record> deletes = new ArrayList<>();
        deletes.add(deleteTuple(deleteRowSchema, "a", "x")); // T1
        deletes.add(deleteTuple(deleteRowSchema, null, "y")); // T2 (null category)
        return deletes;
    }

    private static Record deleteTuple(Schema deleteRowSchema, String category, String flag) {
        Record tuple = GenericRecord.create(deleteRowSchema);
        tuple.setField("category", category);
        tuple.setField("flag", flag);
        return tuple;
    }

    private static DeleteFile writeEqualityDeleteFile(Table table, Schema deleteRowSchema, List<Record> deletes)
            throws IOException {
        int[] equalityFieldIds =
                deleteRowSchema.columns().stream().mapToInt(NestedField::fieldId).toArray();
        FileWriterFactory<Record> factory = new GenericFileWriterFactory.Builder(table)
                .equalityDeleteRowSchema(deleteRowSchema)
                .equalityFieldIds(equalityFieldIds)
                .build();
        OutputFile out = localOutput(table, "data/equality-deletes.parquet");
        EqualityDeleteWriter<Record> writer = factory.newEqualityDeleteWriter(encrypt(out), table.spec(), null);
        try (Closeable toClose = writer) {
            writer.write(deletes);
        }
        return writer.toDeleteFile();
    }

    private static void verifyWithOracle(Table table) throws IOException {
        TreeSet<Long> liveIds = new TreeSet<>();
        try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
            for (Record record : reader) {
                liveIds.add((Long) record.getField("id"));
            }
        }
        TreeSet<Long> deletedIds = new TreeSet<>();
        for (long id = 1L; id <= 10L; id++) {
            if (!liveIds.contains(id)) {
                deletedIds.add(id);
            }
        }
        printReport(liveIds, deletedIds);
        assertDeleted(deletedIds, 1L, 2L);
        assertLive(liveIds, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    }

    private static void printReport(TreeSet<Long> liveIds, TreeSet<Long> deletedIds) {
        System.out.println("ORACLE-LIVE-COUNT=" + liveIds.size());
        System.out.println("ORACLE-LIVE-IDS=" + liveIds);
        System.out.println("ORACLE-DELETED-COUNT=" + deletedIds.size());
        System.out.println("ORACLE-DELETED-IDS=" + deletedIds);
    }

    private static void assertDeleted(TreeSet<Long> deletedIds, long... expected) {
        for (long id : expected) {
            if (!deletedIds.contains(id)) {
                throw new IllegalStateException("expected id " + id + " to be deleted by the oracle");
            }
        }
        if (deletedIds.size() != expected.length) {
            throw new IllegalStateException("unexpected deleted-id set: " + deletedIds);
        }
    }

    private static void assertLive(TreeSet<Long> liveIds, long... expected) {
        for (long id : expected) {
            if (!liveIds.contains(id)) {
                throw new IllegalStateException("expected id " + id + " to survive the oracle");
            }
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

    private EqualityDeleteFixtureGenerator() {}
}
