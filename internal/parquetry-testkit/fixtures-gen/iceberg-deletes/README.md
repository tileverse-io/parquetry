# iceberg-deletes fixtures

Generates the vendored Iceberg merge-on-read delete tables under
`internal/parquetry-testkit/src/main/resources/iceberg-deletes/`. The reference
writer (Apache Iceberg via Spark) is the oracle: the expected live-row set is
what Iceberg itself reports after the delete.

Currently produced:

- `positional/` - an Iceberg v2 table with one positional delete file.
- `equality/` - an Iceberg v2 table with one equality delete file keyed on two
  fields.
- `deletion-vectors/` - an Iceberg format-version=3 table with one Puffin
  deletion vector for its single data file.

## What `positional/` contains

A `format-version=2` table `db.events(id bigint, category string, value double)`
written merge-on-read:

- one data file, 1000 rows, `id` 0..999, `value = id * 1.5`, sorted by `id`, in
  ten 100-row Parquet row groups (so the absolute row position equals `id`);
- one position-delete file removing `id in [300,400)` (row group 3 in full) and
  `id in [595,605)` (spanning the row-group-5/6 boundary): 110 deletions,
  leaving 890 live rows.

The row-group layout lets a reader demonstrate delete-aware pruning: row group 3
is eliminated before decode, several groups have no deletes, and the 595..604
span crosses a row-group boundary.

## Toolchain

- Spark 3.4.1 + Iceberg 1.7.1, run inside `apache/sedona:1.6.1` (only its bundled
  Spark is used; the Iceberg runtime is pulled by `--packages` and cached under
  `~/.ivy2`). Sedona itself is not used.
- Path normalization on the host with `pyarrow` and `fastavro`.

## Regenerate

```bash
cd internal/parquetry-testkit/fixtures-gen/iceberg-deletes

# 1. Build the raw warehouse with Spark/Iceberg in Docker.
#    Writes ./work/warehouse/db/events (absolute generation paths), and
#    self-checks 1 data file, 1 delete file, 110 deletes, 890 live rows.
./run.sh

# 2. Normalize paths and vendor into src/main/resources/iceberg-deletes/positional.
#    Needs pyarrow + fastavro; reuse the system pyarrow and add fastavro:
python3 -m venv --system-site-packages .venv
./.venv/bin/python -m pip install fastavro
./.venv/bin/python normalize_and_vendor.py
```

`normalize_and_vendor.py` rewrites the absolute generation path to the logical
root `file:///iceberg-deletes/positional` in the metadata JSON, the manifest
list / manifest Avro, and the position-delete Parquet's `file_path` column; keeps
only the current metadata document as `v1.metadata.json` (the reader picks the
highest `vN.metadata.json`); and drops `version-hint.text` and the Hadoop `.crc`
checksums. The result is portable and resolves from any extraction directory.

`work/`, `probe-work/`, `.venv/`, and `*.log` are regeneration scratch and are
not committed.

## What `equality/` contains

A `format-version=2`, unpartitioned table
`events(id long, category string, flag string, value double)` written
merge-on-read:

- one data file with ten labeled rows whose ids encode each delete boundary;
- one equality delete file keyed on two fields `(category, flag)` with two
  delete tuples joined by OR over rows, AND over the two fields within a tuple:
  - `T1 = (category="a", flag="x")` (both non-null),
  - `T2 = (category=NULL, flag="y")` (a NULL key component, null-safe matching).

The append commits at data sequence number 1; the equality delete commits
through `newRowDelta` at delete sequence number 2. The delete applies to the
data because 1 < 2. Iceberg's own reader (`IcebergGenerics`) is the oracle. It
reports 8 live rows; ids 1 and 2 are removed:

| id | category | flag | result   | why                                    |
|----|----------|------|----------|----------------------------------------|
| 1  | a        | x    | DELETED  | matches T1 exactly                     |
| 2  | NULL     | y    | DELETED  | matches T2 exactly (null key)          |
| 3  | NULL     | z    | SURVIVES | T2 needs flag=y                        |
| 4  | b        | y    | SURVIVES | T2 needs category IS NULL              |
| 5  | a        | NULL | SURVIVES | T1 needs flag=x                        |
| 6  | c        | x    | SURVIVES | filler                                 |
| 7  | c        | y    | SURVIVES | filler                                 |
| 8  | c        | z    | SURVIVES | filler                                 |
| 9  | a        | y    | SURVIVES | category matches T1, flag does not     |
| 10 | b        | x    | SURVIVES | flag matches T1, category does not     |

### Toolchain

- Apache Iceberg 1.11.0 Java API directly (no Spark, no Flink), via
  `HadoopTables` + `GenericFileWriterFactory`. The standalone generator under
  `equality-gen/` is NOT part of the parquetry reactor and builds with Java 17.
- Path normalization on the host with `pyarrow` and `fastavro` (the same venv as
  `positional/`).

### Regenerate

```bash
cd internal/parquetry-testkit/fixtures-gen/iceberg-deletes/equality-gen

# 1. Build the raw warehouse with the Iceberg Java API (Java 17).
#    Writes ./work/warehouse/events (absolute generation paths) and prints the
#    oracle live count and deleted-id set.
JAVA_HOME=/Users/groldan/.sdkman/candidates/java/17.0.17-tem \
  /Users/groldan/.sdkman/candidates/maven/current/bin/mvn -q -e compile exec:java \
  -Dexec.mainClass=equalitygen.EqualityDeleteFixtureGenerator

# 2. Normalize paths and vendor into src/main/resources/iceberg-deletes/equality.
#    Reuses the positional venv (system pyarrow + fastavro):
cd ..
./.venv/bin/python normalize_and_vendor_equality.py
```

`normalize_and_vendor_equality.py` rewrites the absolute generation path to the
logical root `file:///iceberg-deletes/equality` in the metadata JSON and the
manifest list / manifest Avro. The Iceberg Java writer records the path with a
`file:` scheme in the metadata and as a bare filesystem path in the manifest
`data_file.file_path`; every spelling is rewritten to the schemed clean root.
The equality delete Parquet has no `file_path` column (its columns are the key
fields `category` and `flag`); no Parquet path rewrite applies to it. The
script keeps only the current metadata document as `v1.metadata.json` and drops
`version-hint.text` and the Hadoop `.crc` checksums.

`equality-gen/target/`, `equality-gen/work/`, and `equality-gen/cp.txt` are
regeneration scratch and are not committed.

## What `equality-evolved/` contains

A `format-version=2` table `events(id long required, category string, value
double)` PARTITIONED by `identity(category)`, written merge-on-read, with the
partition column omitted from the data files (the evolved-table reconciliation
shape) AND an equality delete keyed on that omitted column:

- three data files, one per partition `category=a`, `category=b`, `category=c`,
  five rows each, ids 0..14, `value = id * 1.5` (category a gets ids 0..4, b gets
  5..9, c gets 10..14);
- one equality delete keyed on the single partition field `category` with the
  tuple `(category="a")`. It removes every category-a row.

The category column is dropped from each data Parquet file (id and value keep
their field ids 1 and 3). A reader reconstructs category from the manifest
partition tuple and must fold the equality delete against that reconstructed
constant. The equality-delete Parquet keeps its category column (category is the
equality field, field id 2).

The append commits at data sequence number 1; the equality delete commits
through `newRowDelta` at delete sequence number 2. The delete applies to the
data because 1 < 2. Iceberg's own reader (`IcebergGenerics`) on the FULL table,
read before the column drop, is the oracle. Dropping the stored category column
does not change which rows are live (category is reconstructed from the
partition); the oracle therefore stays valid for the vendored omitted-column
table. It reports 10 live rows (ids 5..14); the five category-a ids 0..4 are
removed.

### Toolchain

- Apache Iceberg 1.11.0 Java API directly (no Spark, no Flink), via
  `HadoopTables` + `GenericFileWriterFactory`. The standalone generator under
  `equality-evolved-gen/` is NOT part of the parquetry reactor and builds with
  Java 17.
- Path normalization and the category-column drop on the host with `pyarrow` and
  `fastavro` (the same venv as `positional/` and `equality/`).

### Regenerate

```bash
cd internal/parquetry-testkit/fixtures-gen/iceberg-deletes/equality-evolved-gen

# 1. Build the raw warehouse with the Iceberg Java API (Java 17).
#    Writes ./work/warehouse/events (absolute generation paths) and prints the
#    oracle total/live counts and the deleted-id set.
JAVA_HOME=/Users/groldan/.sdkman/candidates/java/17.0.17-tem \
  /Users/groldan/.sdkman/candidates/maven/current/bin/mvn -q -e compile exec:java \
  -Dexec.mainClass=equalityevolvedgen.EqualityEvolvedFixtureGenerator \
  -Dexec.jvmArgs="--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED"

# 2. Normalize paths, drop category from the data files, and vendor into
#    src/main/resources/iceberg-deletes/equality-evolved.
#    Reuses the positional/equality venv (system pyarrow + fastavro):
cd ..
./.venv/bin/python normalize_and_vendor_equality_evolved.py
```

`normalize_and_vendor_equality_evolved.py` rewrites the absolute generation path
to the logical root `file:///iceberg-deletes/equality-evolved` in the metadata
JSON and the manifest list / manifest Avro (every spelling: the schemed `file:`
form the Iceberg Java writer records in the metadata and the bare filesystem
path it records in the manifest `data_file.file_path`). It then drops the
`category` column from each data Parquet file while preserving the `id` and
`value` field ids, leaving the equality-delete Parquet untouched (it keeps its
`category` column, and has no `file_path` column). The script keeps only the
current metadata document as `v1.metadata.json` and drops `version-hint.text`
and the Hadoop `.crc` checksums.

`equality-evolved-gen/target/`, `equality-evolved-gen/work/`, and
`equality-evolved-gen/cp.txt` are regeneration scratch and are not committed.

## What `deletion-vectors/` contains

A `format-version=3`, unpartitioned table `events(id long required, category
string, value double)` written merge-on-read:

- one data file of 100 rows, `id` 0..99, `value = id * 1.5`, `category` cycling
  over `a`, `b`, `c` by `id % 3`, written in `id` order in a single Parquet row
  group (the absolute row position equals `id`);
- one Puffin deletion vector for that data file removing a contiguous run plus
  scattered singletons plus the tail: positions `{10, 11, 12, 13, 14, 50, 73,
  97, 98, 99}` (10 deletions, leaving 90 live rows).

The append commits at data sequence number 1; the deletion vector commits
through `newRowDelta` at delete sequence number 2. The delete applies to the
data because 1 < 2. Iceberg's own reader (`IcebergGenerics`) is the oracle: it
reports 90 live rows, with ids 10..14, 50, 73, and 97..99 removed.

The deletion-vector blob inside the Puffin file is framed as a 4-byte
big-endian length, a 4-byte little-endian magic number (`1681511377`, on-disk
bytes `d1 d3 39 64`), the portable little-endian Roaring position bitmap, and a
4-byte big-endian CRC-32 of the magic-plus-bitmap. The manifest entry's
`content_offset` and `content_size_in_bytes` point into the Puffin file at this
blob.

### Toolchain

- Apache Iceberg 1.11.0 Java API directly (no Spark, no Flink), via
  `HadoopTables` + `GenericFileWriterFactory` for the data file and
  `BaseDVFileWriter` for the Puffin deletion vector. The standalone generator
  under `dv-gen/` is NOT part of the parquetry reactor and builds with Java 17.
- Path normalization on the host with `pyarrow` and `fastavro` (the same venv as
  `positional/` and `equality/`).

### Regenerate

```bash
cd internal/parquetry-testkit/fixtures-gen/iceberg-deletes/dv-gen

# 1. Build the raw warehouse with the Iceberg Java API (Java 17).
#    Writes ./work/warehouse/events (absolute generation paths) and prints the
#    oracle total/live counts and the deleted-id set.
JAVA_HOME=/Users/groldan/.sdkman/candidates/java/17.0.17-tem \
  /Users/groldan/.sdkman/candidates/maven/current/bin/mvn -q -e compile exec:java \
  -Dexec.mainClass=dvgen.DeletionVectorFixtureGenerator \
  -Dexec.jvmArgs="--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED"

# 2. Normalize paths and vendor into
#    src/main/resources/iceberg-deletes/deletion-vectors.
#    Reuses the positional/equality venv (system pyarrow + fastavro):
cd ..
./.venv/bin/python normalize_and_vendor_deletion_vectors.py
```

`normalize_and_vendor_deletion_vectors.py` rewrites the absolute generation path
to the logical root `file:///iceberg-deletes/deletion-vectors` in the metadata
JSON and the manifest list / manifest Avro. The Iceberg Java writer records the
path with a `file:` scheme in some fields (the Puffin `file_path`) and as a bare
filesystem path in others (the data `file_path` and the delete entry's
`referenced_data_file`); every spelling is rewritten to the schemed clean root.
The script keeps only the current metadata document as `v1.metadata.json` and
drops `version-hint.text` and the Hadoop `.crc` checksums.

The `.puffin` deletion-vector file is left byte-for-byte untouched: its blob is
path-free and the manifest `content_offset` / `content_size_in_bytes` point into
it, which rewriting bytes would invalidate.

`dv-gen/target/`, `dv-gen/work/`, and `dv-gen/cp.txt` are regeneration scratch
and are not committed.
