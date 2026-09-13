# iceberg-scalar-types fixtures

Generates the vendored Iceberg table under
`internal/parquetry-testkit/src/main/resources/iceberg-scalar-types/`. The
reference writer (Apache Iceberg's own Java API) is the oracle: the physical
Parquet shapes, the manifest bound encodings, and the partition tuple encoding
are whatever Iceberg itself produces, and the expected live-row set is what
Iceberg itself reports after the deletes.

Currently produced:

- `scalars/` - an Iceberg `format-version=3` table with one column of every
  scalar Iceberg type, partitioned by `identity(ts)`, with two equality delete
  files.

## What `scalars/` contains

A `format-version=3` table `scalars` partitioned by `identity(ts)`, written
merge-on-read with uncompressed Parquet. Every column is a pure function of the
row id, which lets a test assert an exact decoded value without consulting a
value dump:

| id | name | Iceberg type | value for row `id` |
|----|------|--------------|--------------------|
| 1  | `id`      | `long` (required)  | 1..12 |
| 2  | `ts`      | `timestamp`        | the partition timestamp for P = (id-1)/4 |
| 3  | `tstz`    | `timestamptz`      | the partition timestamp plus `id` minutes, UTC |
| 4  | `ts_ns`   | `timestamp_ns`     | `2024-01-01T00:00:00` plus `id` nanoseconds |
| 5  | `tstz_ns` | `timestamptz_ns`   | the same instant as `ts_ns`, UTC |
| 6  | `t`       | `time`             | `LocalTime.of(id, 30, 15, 250_000_000)` |
| 7  | `dec`     | `decimal(9, 2)`    | `(id - 6) * 1.25`, negative below id 6 |
| 8  | `bigdec`  | `decimal(20, 3)`   | `123456789012345.678 * id` |
| 9  | `u`       | `uuid`             | `new UUID(id, id * 1_000_003L)` |
| 10 | `fx`      | `fixed[4]`         | bytes `{id, 2*id, 3*id, 4*id}` |
| 11 | `bin`     | `binary`           | `"bin-" + id` as UTF-8 |
| 12 | `d`       | `date`             | `2024-01-01` plus `id` days |
| 13 | `label`   | `string`           | `"row-" + id` |

The three partitions and their data files:

| partition | data file | ids | `ts` |
|-----------|-----------|-----|------|
| P0 | `data/p0/data.parquet` | 1-4   | `2024-01-01T00:00:00` |
| P1 | `data/p1/data.parquet` | 5-8   | `2024-06-15T12:30:45.123456` |
| P2 | `data/p2/data.parquet` | 9-12  | `2025-03-10T08:00:00` |

All three data files are appended in one commit, at data sequence number 1. Two
partition-scoped equality delete files are then committed together through
`newRowDelta`, at delete sequence number 2:

| delete file | keyed on | tuple | partition | removes |
|-------------|----------|-------|-----------|---------|
| `data/p0/eq-delete-u.parquet`   | `u` (field 9)   | the uuid of id 2 | P0 | id 2 |
| `data/p1/eq-delete-dec.parquet` | `dec` (field 7) | `1.25` (id 7)    | P1 | id 7 |

### Live rows

Iceberg's own reader (`IcebergGenerics`) is the oracle and reports **10 live
rows**, ids `{1, 3, 4, 5, 6, 8, 9, 10, 11, 12}`. Both equality deletes apply,
because the append commits at data sequence number 1 and the deletes at 2.

### Why the uuids are row-keyed

Iceberg writes a `uuid` column bound as 16 unsigned big-endian bytes, and then
compares those bounds with `java.util.UUID.compareTo`, which orders the most and
least significant halves as **signed** longs. The two orderings disagree across
the `0x80...` boundary, and for a data file whose uuids fall on both sides of it
the stored lower bound tests as greater than the stored upper bound. Iceberg's
`DeleteFileIndex.canContainEqDeletesForFile` then finds no overlap and discards
a uuid-keyed equality delete before applying it. Putting the row id in the high
64 bits keeps every uuid in this table small and positive, which holds the two
orderings in agreement and lets the delete take effect.

### Generator adjustment

`PartitionKey.partition` reads its source column in Iceberg's internal
representation, where a timestamp is a long of microseconds, while a
`GenericRecord` holds the object-model `LocalDateTime`. Passing the record
directly fails with `Not an instance of java.lang.Long`. The generator wraps the
row in `InternalRecordWrapper`, which is the same bridge used by Iceberg's own
partitioned writers. No row formula changes.

The `GenericRecord` setter types accepted by Iceberg are otherwise as expected:
`byte[]` for `fixed`, `ByteBuffer` for `binary`, `java.util.UUID` for `uuid`,
`LocalTime` for `time`, `LocalDateTime` for `timestamp` and `timestamp_ns`,
`OffsetDateTime` for `timestamptz` and `timestamptz_ns`, `BigDecimal` at the
declared scale for `decimal`, and `LocalDate` for `date`.

## Pinned shapes

Everything below is observed from the vendored fixture, not assumed. Regenerate
and re-check these whenever the Iceberg version moves.

### Physical Parquet schema of a data file

Every leaf is stamped with its Iceberg field id. `par schema` renders the tree
as:

```
message table {
  required INT64 id;
  optional INT64 ts (Timestamp);
  optional INT64 tstz (Timestamp);
  optional INT64 ts_ns (Timestamp);
  optional INT64 tstz_ns (Timestamp);
  optional INT64 t (Time);
  optional INT32 dec (Decimal);
  optional FIXED_LEN_BYTE_ARRAY bigdec (Decimal);
  optional FIXED_LEN_BYTE_ARRAY u (UuidType);
  optional FIXED_LEN_BYTE_ARRAY fx;
  optional BYTE_ARRAY bin;
  optional INT32 d (DateType);
  optional BYTE_ARRAY label (StringType);
}
```

with these physical types, type lengths, and logical-type parameters:

| field id | name | physical | length | logical type | converted type |
|---|---|---|---|---|---|
| 1  | `id`      | INT64                | -  | none | NONE |
| 2  | `ts`      | INT64                | -  | `Timestamp(isAdjustedToUTC=false, timeUnit=microseconds)` | NONE |
| 3  | `tstz`    | INT64                | -  | `Timestamp(isAdjustedToUTC=true, timeUnit=microseconds)` | TIMESTAMP_MICROS |
| 4  | `ts_ns`   | INT64                | -  | `Timestamp(isAdjustedToUTC=false, timeUnit=nanoseconds)` | NONE |
| 5  | `tstz_ns` | INT64                | -  | `Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds)` | NONE |
| 6  | `t`       | INT64                | -  | `Time(isAdjustedToUTC=false, timeUnit=microseconds)` | NONE |
| 7  | `dec`     | INT32                | -  | `Decimal(precision=9, scale=2)` | DECIMAL |
| 8  | `bigdec`  | FIXED_LEN_BYTE_ARRAY | 9  | `Decimal(precision=20, scale=3)` | DECIMAL |
| 9  | `u`       | FIXED_LEN_BYTE_ARRAY | 16 | `UUID` | NONE |
| 10 | `fx`      | FIXED_LEN_BYTE_ARRAY | 4  | none | NONE |
| 11 | `bin`     | BYTE_ARRAY           | -  | none | NONE |
| 12 | `d`       | INT32                | -  | `Date` | DATE |
| 13 | `label`   | BYTE_ARRAY           | -  | `String` | UTF8 |

Two of these are worth calling out because they differ from a naive
expectation:

- **`dec` is INT32, not a 4-byte FIXED_LEN_BYTE_ARRAY.** This is Iceberg's own
  writer rule, not a quirk of this fixture: Iceberg backs a decimal with INT32
  up to precision 9, with INT64 up to precision 18, and with a minimal-width
  FIXED_LEN_BYTE_ARRAY above that. Only `bigdec` (precision 20) lands on
  FIXED_LEN_BYTE_ARRAY, at 9 bytes. A reader must accept all three physical
  backings for a decimal column.
- **Only the UTC-adjusted microsecond timestamp gets a converted type.** The
  legacy Parquet `TIMESTAMP_MICROS` converted type implies UTC, and Iceberg
  emits it only for `tstz`. The nanosecond columns have no converted-type
  equivalent at all.

The two equality delete files hold exactly their key column, at the table's
field id: `eq-delete-u.parquet` is one FIXED_LEN_BYTE_ARRAY(16) `UUID` column at
field id 9, and `eq-delete-dec.parquet` is one INT32 `Decimal(9, 2)` column at
field id 7. Each holds a single row.

The first two rows of `data/p0/data.parquet`, decoded by `par head`:

```
{"id":1,"ts":1704067200000000,"tstz":1704067260000000,"ts_ns":1704067200000000001,"tstz_ns":1704067200000000001,"t":5415250000,"dec":-625,"bigdec":"AAG2m0umMPNO","u":"00000000-0000-0001-0000-0000000f4243","fx":"AQIDBA==","bin":"YmluLTE=","d":19724,"label":"row-1"}
{"id":2,"ts":1704067200000000,"tstz":1704067320000000,"ts_ns":1704067200000000002,"tstz_ns":1704067200000000002,"t":9015250000,"dec":-500,"bigdec":"AANtNpdMYeac","u":"00000000-0000-0002-0000-0000001e8486","fx":"AgQGCA==","bin":"YmluLTI=","d":19725,"label":"row-2"}
```

### Manifest bound encodings

Manifest lower and upper bounds for `data/p0/data.parquet` (ids 1-4), by field
id, as raw hex:

| field id | name | lower | upper | encoding |
|---|---|---|---|---|
| 1  | `id`      | `0100000000000000` | `0400000000000000` | 8-byte little-endian long |
| 2  | `ts`      | `00202110d70d0600` | `00202110d70d0600` | 8-byte little-endian micros |
| 3  | `tstz`    | `00a7b413d70d0600` | `003c6f1ed70d0600` | 8-byte little-endian micros |
| 4  | `ts_ns`   | `010065011710a617` | `040065011710a617` | 8-byte little-endian nanos |
| 5  | `tstz_ns` | `010065011710a617` | `040065011710a617` | 8-byte little-endian nanos |
| 6  | `t`       | `5028c64201000000` | `501481c603000000` | 8-byte little-endian micros since midnight |
| 7  | `dec`     | `fd8f`             | `ff06`             | minimal two's-complement big-endian unscaled value |
| 8  | `bigdec`  | `01b69b4ba630f34e` | `06da6d2e98c3cd38` | minimal two's-complement big-endian unscaled value |
| 9  | `u`       | `000000000000000100000000000f4243` | `000000000000000400000000003d090c` | 16 raw big-endian bytes |
| 10 | `fx`      | `01020304`         | `04080c10`         | the 4 raw bytes |
| 11 | `bin`     | `62696e2d31`       | `62696e2d34`       | the raw bytes |
| 12 | `d`       | `0c4d0000`         | `0f4d0000`         | 4-byte little-endian days since epoch |
| 13 | `label`   | `726f772d31`       | `726f772d34`       | the raw UTF-8 bytes |

Reading a few of these back confirms the encodings: field 2's
`00202110d70d0600` little-endian is 1704067200000000 microseconds, which is
`2024-01-01T00:00:00`; field 6's `5028c64201000000` is 5415250000 microseconds
since midnight, which is `01:30:15.250`; field 7's `fd8f` is -625 at scale 2,
which is `-6.25`; field 12's `0c4d0000` is 19724 days, which is `2024-01-02`;
field 9's lower bound is `new UUID(1, 1000003)`, with the row id in the leading
eight bytes.

Two encoding notes:

- **A decimal bound is minimal-width, independent of the physical type.** Field
  7 takes two bytes even though the column is INT32, and field 8 takes eight
  even though the column is a 9-byte FIXED_LEN_BYTE_ARRAY. A reader must
  sign-extend from the length given in the manifest rather than from the column
  width.
- **A uuid bound is 16 raw big-endian bytes**, ordered as unsigned bytes. The
  note on row-keyed uuids above covers why that ordering matters.

The delete files' bounds hold their single key value in the same encodings:
field 9 is `000000000000000200000000001e8486` for both bounds of
`eq-delete-u.parquet`, and field 7 is `7d` (125 at scale 2, `1.25`) for both
bounds of `eq-delete-dec.parquet`.

### Partition tuple

The manifest `data_file.partition` struct, from both manifests' Avro writer
schema:

```json
{"field-id": 102, "doc": "Partition data tuple, schema based on the partition spec", "name": "partition", "type": {"type": "record", "name": "r102", "fields": [{"field-id": 1000, "default": null, "name": "ts", "type": ["null", {"logicalType": "timestamp-micros", "adjust-to-utc": false, "type": "long"}]}]}}
```

One field `ts`, an Avro `long` with logical type `timestamp-micros` and
`adjust-to-utc` false, at partition field id 1000. The three stored tuples are
`2024-01-01T00:00:00`, `2024-06-15T12:30:45.123456`, and `2025-03-10T08:00:00`.

## Toolchain

- Apache Iceberg 1.11.0 Java API directly (no Spark, no Flink), via
  `HadoopTables` + `GenericFileWriterFactory`. The standalone generator under
  `scalar-types-gen/` is NOT part of the parquetry reactor and builds with
  Java 17.
- Path normalization on the host with `fastavro`. The system `python3` has it;
  no virtualenv is needed.

## Regenerate

```bash
cd internal/parquetry-testkit/fixtures-gen/iceberg-scalar-types/scalar-types-gen

# 1. Build the raw warehouse with the Iceberg Java API (Java 17).
#    Writes ./work/warehouse/scalars (absolute generation paths), prints one
#    ROW line per live row, and prints the oracle live/deleted id sets.
JAVA_HOME=/Users/groldan/.sdkman/candidates/java/17.0.17-tem \
  /Users/groldan/.sdkman/candidates/maven/current/bin/mvn -q -e compile exec:java \
  -Dexec.mainClass=scalartypesgen.ScalarTypesFixtureGenerator \
  -Dexec.jvmArgs="--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED"

# 2. Normalize paths and vendor into
#    src/main/resources/iceberg-scalar-types/scalars.
cd ..
python3 normalize_and_vendor.py
```

`normalize_and_vendor.py` rewrites the absolute generation path to the logical
root `file:///iceberg-scalar-types/scalars` in the metadata JSON and the
manifest list / manifest Avro. The Iceberg Java writer records the path with a
`file:` scheme in the metadata and as a bare filesystem path in the manifest
`data_file.file_path`; every spelling is rewritten to the schemed clean root.
Neither the data Parquet files nor the equality-delete Parquet files hold a path
column, and all five are left byte-for-byte untouched. The script keeps only the
current metadata document as `v1.metadata.json` (the reader picks the highest
`vN.metadata.json`) and drops `version-hint.text` and the Hadoop `.crc`
checksums. The result is portable and resolves from any extraction directory.

After regenerating, rebuild the testkit with a `clean`. Without it the jar
plugin reuses the previous jar and the new fixture never reaches the bundle:

```bash
./mvnw -q -pl :parquetry-testkit clean install -DskipTests
```

`scalar-types-gen/target/`, `scalar-types-gen/work/`, `scalar-types-gen/cp.txt`,
`.venv/`, and `*.log` are regeneration scratch and are not committed.

## Consumers

```java
Path tableDir = TestCorpus.extractDirectory("iceberg-scalar-types/scalars", tempDir);
IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults());
```
