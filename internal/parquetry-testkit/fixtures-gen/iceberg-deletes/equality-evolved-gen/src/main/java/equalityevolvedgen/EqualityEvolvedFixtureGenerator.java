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
package equalityevolvedgen;

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
import org.apache.iceberg.PartitionKey;
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
 * Builds an Iceberg v2 merge-on-read EQUALITY-delete table directly through the Iceberg Java API (no
 * Spark, no Flink) for the EVOLVED-table reconciliation path, and verifies the live-row set with
 * Iceberg's own reader.
 *
 * <p>The table {@code events(id, category, value)} is partitioned by {@code identity(category)}.
 * Three categories {@code a}, {@code b}, {@code c} are appended, five rows each, ids 0..14, {@code
 * value = id * 1.5}, one data file per partition (data sequence number 1). One equality-delete file
 * keyed on the single partition field {@code category} with the tuple {@code (category="a")} is then
 * committed through {@code newRowDelta} (delete sequence number 2). The delete removes every
 * category-a row.
 *
 * <p>The category column is later dropped from the data Parquet files by the host normalizer. This
 * generator writes and verifies the full (pre-drop) table because the live-row set is independent of
 * whether category is stored physically or reconstructed from the partition tuple. The oracle the
 * generator records therefore stays valid for the vendored omitted-column table.
 */
public final class EqualityEvolvedFixtureGenerator {

    private static final Schema TABLE_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "category", Types.StringType.get()),
            NestedField.optional(3, "value", Types.DoubleType.get()));

    private static final PartitionSpec SPEC =
            PartitionSpec.builderFor(TABLE_SCHEMA).identity("category").build();

    private static final List<String> CATEGORIES = List.of("a", "b", "c");

    private static final int ROWS_PER_CATEGORY = 5;

    private static final String DELETED_CATEGORY = "a";

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
                SPEC,
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
        org.apache.iceberg.AppendFiles append = table.newAppend();
        for (String category : CATEGORIES) {
            DataFile dataFile = writeCategoryDataFile(table, category);
            append.appendFile(dataFile);
        }
        append.commit();
    }

    private static DataFile writeCategoryDataFile(Table table, String category) throws IOException {
        List<Record> rows = dataRows(table.schema(), category);
        PartitionKey partition = partitionKey(table, category);
        OutputFile out = localOutput(table, dataFilePath(category));
        FileWriterFactory<Record> factory =
                new GenericFileWriterFactory.Builder(table).build();
        DataWriter<Record> writer = factory.newDataWriter(encrypt(out), table.spec(), partition);
        try (Closeable toClose = writer) {
            writer.write(rows);
        }
        return writer.toDataFile();
    }

    private static String dataFilePath(String category) {
        return "data/category=" + category + "/data-" + category + ".parquet";
    }

    /**
     * Five rows for one category with globally unique ids: category {@code a} gets ids 0..4, {@code
     * b} gets 5..9, {@code c} gets 10..14. Every category-a id is removed by the oracle; every other
     * id survives.
     */
    private static List<Record> dataRows(Schema schema, String category) {
        int base = CATEGORIES.indexOf(category) * ROWS_PER_CATEGORY;
        List<Record> rows = new ArrayList<>();
        for (int offset = 0; offset < ROWS_PER_CATEGORY; offset++) {
            long id = base + offset;
            rows.add(row(schema, id, category, id * 1.5));
        }
        return rows;
    }

    private static Record row(Schema schema, long id, String category, double value) {
        Record record = GenericRecord.create(schema);
        record.setField("id", id);
        record.setField("category", category);
        record.setField("value", value);
        return record;
    }

    private static PartitionKey partitionKey(Table table, String category) {
        PartitionKey key = new PartitionKey(table.spec(), table.schema());
        key.partition(partitionRow(table.schema(), category));
        return key;
    }

    private static Record partitionRow(Schema schema, String category) {
        Record record = GenericRecord.create(schema);
        record.setField("category", category);
        return record;
    }

    private static void commitEqualityDelete(Table table) throws IOException {
        Schema deleteRowSchema = table.schema().select("category");
        DeleteFile deleteFile = writeEqualityDeleteFile(table, deleteRowSchema, deleteTuples(deleteRowSchema));
        table.newRowDelta().addDeletes(deleteFile).commit();
    }

    /** A single delete tuple on the partition field {@code category}: {@code (category="a")}. */
    private static List<Record> deleteTuples(Schema deleteRowSchema) {
        List<Record> deletes = new ArrayList<>();
        deletes.add(deleteTuple(deleteRowSchema, DELETED_CATEGORY));
        return deletes;
    }

    private static Record deleteTuple(Schema deleteRowSchema, String category) {
        Record tuple = GenericRecord.create(deleteRowSchema);
        tuple.setField("category", category);
        return tuple;
    }

    /**
     * Writes the equality-delete file into the deleted category's partition. The delete is scoped to
     * partition {@code category=a} (the only partition any category-a row lives in), which honors how
     * an equality delete on a partition column applies in a partitioned table.
     */
    private static DeleteFile writeEqualityDeleteFile(Table table, Schema deleteRowSchema, List<Record> deletes)
            throws IOException {
        int[] equalityFieldIds =
                deleteRowSchema.columns().stream().mapToInt(NestedField::fieldId).toArray();
        FileWriterFactory<Record> factory = new GenericFileWriterFactory.Builder(table)
                .equalityDeleteRowSchema(deleteRowSchema)
                .equalityFieldIds(equalityFieldIds)
                .build();
        PartitionKey partition = partitionKey(table, DELETED_CATEGORY);
        OutputFile out = localOutput(table, "data/category=" + DELETED_CATEGORY + "/equality-deletes.parquet");
        EqualityDeleteWriter<Record> writer = factory.newEqualityDeleteWriter(encrypt(out), table.spec(), partition);
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
        long totalRows = (long) CATEGORIES.size() * ROWS_PER_CATEGORY;
        TreeSet<Long> deletedIds = new TreeSet<>();
        for (long id = 0L; id < totalRows; id++) {
            if (!liveIds.contains(id)) {
                deletedIds.add(id);
            }
        }
        printReport(totalRows, liveIds, deletedIds);
        assertDeleted(deletedIds, 0L, 1L, 2L, 3L, 4L);
        assertLive(liveIds, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L);
    }

    private static void printReport(long totalRows, TreeSet<Long> liveIds, TreeSet<Long> deletedIds) {
        System.out.println("ORACLE-TOTAL-COUNT=" + totalRows);
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
        if (liveIds.size() != expected.length) {
            throw new IllegalStateException("unexpected live-id set: " + liveIds);
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

    private EqualityEvolvedFixtureGenerator() {}
}
