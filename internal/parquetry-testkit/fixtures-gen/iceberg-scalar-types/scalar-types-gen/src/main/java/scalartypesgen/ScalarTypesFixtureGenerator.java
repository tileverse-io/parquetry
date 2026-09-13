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
package scalartypesgen;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.InternalRecordWrapper;
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
 * Builds an Iceberg v3 table holding one column of every scalar type, partitioned by identity on a
 * timestamp column, with two equality deletes (one keyed on a uuid, one on a decimal), directly
 * through the Iceberg Java API. Iceberg's own reader is the oracle for the live-row set. Every value
 * is a pure function of the row id (see the README table), which lets the parquetry tests assert
 * exact decoded values without a value dump.
 */
public final class ScalarTypesFixtureGenerator {

    static final Schema TABLE_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "ts", Types.TimestampType.withoutZone()),
            NestedField.optional(3, "tstz", Types.TimestampType.withZone()),
            NestedField.optional(4, "ts_ns", Types.TimestampNanoType.withoutZone()),
            NestedField.optional(5, "tstz_ns", Types.TimestampNanoType.withZone()),
            NestedField.optional(6, "t", Types.TimeType.get()),
            NestedField.optional(7, "dec", Types.DecimalType.of(9, 2)),
            NestedField.optional(8, "bigdec", Types.DecimalType.of(20, 3)),
            NestedField.optional(9, "u", Types.UUIDType.get()),
            NestedField.optional(10, "fx", Types.FixedType.ofLength(4)),
            NestedField.optional(11, "bin", Types.BinaryType.get()),
            NestedField.optional(12, "d", Types.DateType.get()),
            NestedField.optional(13, "label", Types.StringType.get()));

    static final PartitionSpec SPEC =
            PartitionSpec.builderFor(TABLE_SCHEMA).identity("ts").build();

    static final LocalDateTime[] PARTITION_TS = {
        LocalDateTime.of(2024, 1, 1, 0, 0, 0),
        LocalDateTime.of(2024, 6, 15, 12, 30, 45, 123_456_000),
        LocalDateTime.of(2025, 3, 10, 8, 0, 0)
    };
    static final LocalDateTime NANOS_BASE = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
    static final int ROWS_PER_PARTITION = 4;
    static final int ROW_COUNT = 12;
    static final long DELETED_BY_UUID = 2L;
    static final long DELETED_BY_DECIMAL = 7L;
    static final int DELETE_FILE_COUNT = 2;

    /** The rows removed by Iceberg's own reader from this table, one per equality delete. */
    static final List<Long> ORACLE_DELETED_IDS = List.of(DELETED_BY_UUID, DELETED_BY_DECIMAL);

    public static void main(String[] args) throws IOException {
        Path warehouse = resolveWarehouse(args);
        deleteRecursively(warehouse);
        Files.createDirectories(warehouse);
        Table table = createTable(warehouse);
        appendPartitionedData(table);
        commitEqualityDeletes(table);
        verifyWithOracle(table);
        System.out.println("WAREHOUSE=" + warehouse.resolve("scalars"));
    }

    private static Path resolveWarehouse(String[] args) {
        if (args.length > 0) {
            return Path.of(args[0]);
        }
        return Path.of("work", "warehouse").toAbsolutePath();
    }

    private static Table createTable(Path warehouse) {
        HadoopTables tables = new HadoopTables(new Configuration());
        String location = warehouse.toUri().toString() + "scalars";
        return tables.create(
                TABLE_SCHEMA,
                SPEC,
                ImmutableMap.of(
                        "format-version", "3",
                        "write.delete.mode", "merge-on-read",
                        "write.update.mode", "merge-on-read",
                        "write.merge.mode", "merge-on-read",
                        "write.parquet.compression-codec", "uncompressed",
                        "write.metadata.compression-codec", "none"),
                location);
    }

    private static void appendPartitionedData(Table table) throws IOException {
        FileWriterFactory<Record> factory =
                new GenericFileWriterFactory.Builder(table).build();
        AppendFiles append = table.newAppend();
        for (int partition = 0; partition < PARTITION_TS.length; partition++) {
            append.appendFile(writePartition(table, factory, partition));
        }
        append.commit();
    }

    private static DataFile writePartition(Table table, FileWriterFactory<Record> factory, int partition)
            throws IOException {
        List<Record> rows = partitionRows(table.schema(), partition);
        PartitionKey key = partitionKeyOf(table, rows.get(0));
        OutputFile out = localOutput(table, "data/p" + partition + "/data.parquet");
        DataWriter<Record> writer = factory.newDataWriter(encrypt(out), table.spec(), key);
        try (Closeable toClose = writer) {
            writer.write(rows);
        }
        return writer.toDataFile();
    }

    /**
     * A partition transform reads its source column in Iceberg's internal representation, where a
     * timestamp is a long of microseconds, while a {@link GenericRecord} holds the object-model
     * {@link LocalDateTime}. {@link InternalRecordWrapper} bridges the two.
     */
    private static PartitionKey partitionKeyOf(Table table, Record row) {
        PartitionKey key = new PartitionKey(table.spec(), table.schema());
        InternalRecordWrapper internalRow = new InternalRecordWrapper(table.schema().asStruct());
        key.partition(internalRow.wrap(row));
        return key;
    }

    private static List<Record> partitionRows(Schema schema, int partition) {
        List<Record> rows = new ArrayList<>();
        for (int i = 1; i <= ROWS_PER_PARTITION; i++) {
            rows.add(row(schema, partition * ROWS_PER_PARTITION + i));
        }
        return rows;
    }

    /** Every column is a pure function of the row id; the README documents each formula. */
    static Record row(Schema schema, long id) {
        int partition = (int) ((id - 1) / ROWS_PER_PARTITION);
        LocalDateTime nanos = NANOS_BASE.plusNanos(id);
        Record record = GenericRecord.create(schema);
        record.setField("id", id);
        record.setField("ts", PARTITION_TS[partition]);
        record.setField("tstz", PARTITION_TS[partition].plusMinutes(id).atOffset(ZoneOffset.UTC));
        record.setField("ts_ns", nanos);
        record.setField("tstz_ns", nanos.atOffset(ZoneOffset.UTC));
        record.setField("t", LocalTime.of((int) id, 30, 15, 250_000_000));
        record.setField("dec", decimalOf(id));
        record.setField("bigdec", bigDecimalOf(id));
        record.setField("u", uuidOf(id));
        record.setField("fx", new byte[] {(byte) id, (byte) (2 * id), (byte) (3 * id), (byte) (4 * id)});
        record.setField("bin", ByteBuffer.wrap(("bin-" + id).getBytes(StandardCharsets.UTF_8)));
        record.setField("d", LocalDate.of(2024, 1, 1).plusDays(id));
        record.setField("label", "row-" + id);
        return record;
    }

    static BigDecimal decimalOf(long id) {
        return BigDecimal.valueOf(id - 6).multiply(new BigDecimal("1.25")).setScale(2);
    }

    static BigDecimal bigDecimalOf(long id) {
        return new BigDecimal("123456789012345.678").multiply(BigDecimal.valueOf(id)).setScale(3);
    }

    /**
     * The row id occupies the high 64 bits, which keeps every uuid in the table small and positive.
     * Iceberg writes a uuid column bound as 16 unsigned big-endian bytes and then compares those
     * bounds with {@link UUID#compareTo}, which orders the two halves as signed longs. Uuids drawn
     * from a hash straddle the sign boundary, the two orderings disagree, and Iceberg discards a
     * uuid-keyed equality delete before applying it. Keeping every high half positive holds the two
     * orderings in agreement.
     */
    static UUID uuidOf(long id) {
        return new UUID(id, id * 1_000_003L);
    }

    private static void commitEqualityDeletes(Table table) throws IOException {
        DeleteFile byUuid = writeEqualityDelete(table, "u", 0, "data/p0/eq-delete-u.parquet", uuidOf(DELETED_BY_UUID));
        DeleteFile byDecimal = writeEqualityDelete(
                table, "dec", 1, "data/p1/eq-delete-dec.parquet", decimalOf(DELETED_BY_DECIMAL));
        table.newRowDelta().addDeletes(byUuid).addDeletes(byDecimal).commit();
    }

    /** One partition-scoped equality-delete file with a single tuple on {@code keyColumn}. */
    private static DeleteFile writeEqualityDelete(
            Table table, String keyColumn, int partition, String relativePath, Object keyValue) throws IOException {
        Schema deleteRowSchema = table.schema().select(keyColumn);
        int[] equalityFieldIds = {deleteRowSchema.findField(keyColumn).fieldId()};
        FileWriterFactory<Record> factory = new GenericFileWriterFactory.Builder(table)
                .equalityDeleteRowSchema(deleteRowSchema)
                .equalityFieldIds(equalityFieldIds)
                .build();
        Record tuple = GenericRecord.create(deleteRowSchema);
        tuple.setField(keyColumn, keyValue);
        PartitionKey key = partitionKeyOf(table, row(table.schema(), (long) partition * ROWS_PER_PARTITION + 1));
        OutputFile out = localOutput(table, relativePath);
        EqualityDeleteWriter<Record> writer = factory.newEqualityDeleteWriter(encrypt(out), table.spec(), key);
        try (Closeable toClose = writer) {
            writer.write(tuple);
        }
        return writer.toDeleteFile();
    }

    private static void verifyWithOracle(Table table) throws IOException {
        verifyBothDeleteFilesCommitted(table);
        TreeSet<Long> liveIds = readLiveIds(table);
        TreeSet<Long> deletedIds = complementOf(liveIds);
        printOracleReport(liveIds, deletedIds);
        if (!deletedIds.equals(new TreeSet<>(ORACLE_DELETED_IDS))) {
            throw new IllegalStateException(
                    "oracle removed " + deletedIds + ", the vendored fixture is pinned to " + ORACLE_DELETED_IDS);
        }
    }

    /**
     * Checked before the row set is read, which reports a delete file lost at commit time as such
     * rather than as a puzzling mismatch in the live rows.
     */
    private static void verifyBothDeleteFilesCommitted(Table table) {
        int committed = Iterables.size(table.currentSnapshot().addedDeleteFiles(table.io()));
        if (committed != DELETE_FILE_COUNT) {
            throw new IllegalStateException("expected " + DELETE_FILE_COUNT + " delete files, found " + committed);
        }
    }

    private static TreeSet<Long> readLiveIds(Table table) throws IOException {
        TreeSet<Long> liveIds = new TreeSet<>();
        try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
            for (Record record : reader) {
                liveIds.add((Long) record.getField("id"));
                printRow(record);
            }
        }
        return liveIds;
    }

    private static TreeSet<Long> complementOf(TreeSet<Long> liveIds) {
        TreeSet<Long> deletedIds = new TreeSet<>();
        for (long id = 1L; id <= ROW_COUNT; id++) {
            if (!liveIds.contains(id)) {
                deletedIds.add(id);
            }
        }
        return deletedIds;
    }

    private static void printOracleReport(TreeSet<Long> liveIds, TreeSet<Long> deletedIds) {
        System.out.println("ORACLE-LIVE-COUNT=" + liveIds.size());
        System.out.println("ORACLE-LIVE-IDS=" + liveIds);
        System.out.println("ORACLE-DELETED-IDS=" + deletedIds);
    }

    /** One line per live row, for cross-checking the parquetry tests' expected values by eye. */
    private static void printRow(Record record) {
        StringBuilder line = new StringBuilder("ROW");
        for (NestedField field : TABLE_SCHEMA.columns()) {
            Object value = record.getField(field.name());
            if (value instanceof ByteBuffer buffer) {
                value = new String(buffer.array(), StandardCharsets.UTF_8);
            }
            if (value instanceof byte[] bytes) {
                value = Arrays.toString(bytes);
            }
            line.append(' ').append(field.name()).append('=').append(value);
        }
        System.out.println(line);
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

    private ScalarTypesFixtureGenerator() {}
}
