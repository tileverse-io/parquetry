# iceberg-deletes fixtures

Generates the vendored Iceberg merge-on-read delete tables under
`internal/parquetry-testkit/src/main/resources/iceberg-deletes/`. The reference
writer (Apache Iceberg via Spark) is the oracle: the expected live-row set is
what Iceberg itself reports after the delete.

Currently produced:

- `positional/` - an Iceberg v2 table with one positional delete file.
- `equality/` - an Iceberg v2 table with one equality delete file keyed on two
  fields.

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
