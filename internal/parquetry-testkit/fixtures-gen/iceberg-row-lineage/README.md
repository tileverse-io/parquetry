# iceberg-row-lineage fixtures

Generates the vendored Iceberg format-version=3 row-lineage tables under
`internal/parquetry-testkit/src/main/resources/iceberg-row-lineage/`. Every row
in a v3 table has two reserved metadata columns that are not part of the user
schema:

- `_row_id` (reserved field id 2147483540, optional long): the row's stable
  identity. On read it is `coalesce(materialized physical _row_id column, data
  file first_row_id + within-file 0-based row position)`.
- `_last_updated_sequence_number` (reserved field id 2147483539, optional long):
  the sequence number when the row was last updated. On read it is
  `coalesce(materialized physical column, data file data_sequence_number)`.

Row lineage is on by default for a v3 table in Iceberg 1.11.0; there is no table
property to set. It shows up as a top-level `next-row-id` in the table metadata
and a per-snapshot `first-row-id` base plus `added-rows`.

Currently produced:

- `fresh/` - a v3 table with three data files across two commits. The first
  commit appends two files into one manifest (exercising cumulative
  within-manifest `first_row_id` inheritance); the second commit appends one file
  into a second manifest. No file materializes `_row_id`. Every row's lineage is
  synthesized, and the user `id` is offset from `_row_id` to keep the two columns
  distinct.
- `materialized/` - a v3 table whose second data file physically materializes
  the `_row_id` column as a nullable column with a mix of stored and null cells,
  exercising per-row coalesce (a stored cell wins; a null cell resolves to the
  file base plus position).
- `evolved/` - a v3 table whose two data files sit on opposite sides of a schema
  evolution. The first predates the evolution and reconciles on read with fully
  synthesized lineage; the second is written against the evolved schema and
  materializes BOTH reserved lineage columns, with the two mixes staggered
  against each other.

## Toolchain

- Apache Iceberg 1.11.0 Java API directly (no Spark, no Flink), via
  `HadoopTables` + `GenericFileWriterFactory` for synthesized-lineage files, and
  `Parquet.write` + `GenericParquetWriter` for the file that materializes the
  `_row_id` column. The standalone generators under `fresh-gen/` and
  `materialized-gen/` are NOT part of the parquetry reactor and build with Java
  17.
- Path normalization on the host with `pyarrow` and `fastavro` (reuse the
  `../iceberg-deletes/.venv`).

## How the oracle is read

Iceberg 1.11.0's generic reader (`IcebergGenerics.read(table)`) does NOT project
the synthesized `_row_id` / `_last_updated_sequence_number` columns; selecting
those names returns null, because row lineage synthesis on read is done by the
query engines (Spark/Flink), not the generic Java reader. The authoritative
per-row values instead come from Iceberg's own scan planner, which assigns each
data file its `firstRowId()` base and `dataSequenceNumber()`:

- synthesized `_row_id` = `firstRowId + within-file 0-based position`;
- synthesized `_last_updated_sequence_number` = `dataSequenceNumber`.

The `materialized/` generator additionally reads each data file back through
`Parquet.read(...).project(projectionWithLineageColumns).createReaderFunc(msg ->
GenericParquetReaders.buildReader(projection, msg, idToConstant))` with
`idToConstant` seeded from `PartitionUtil` semantics (`_row_id` -> file
`firstRowId`, `_last_updated_sequence_number` -> file `dataSequenceNumber`). That
is the read path that resolves the coalesce, and it proves the stored physical
`_row_id` wins over the synthesized base.

## What `fresh/` contains

A `format-version=3`, unpartitioned table `events(id long required, value
double)` with three data files across two commits. The user `id` runs 100..114
while `_row_id` runs 0..14, keeping the two columns distinct to defeat a reader
test that could pass by confusing them:

- commit 1 (data sequence number 1) appends TWO files at once into a SINGLE
  manifest M1:
  - file A: ids 100..104, `value = id * 1.5`, `first_row_id` base 0;
  - file B: ids 105..109, `value = id * 1.5`, `first_row_id` base 5 (M1's base 0
    plus file A's 5 records - the cumulative within-manifest offset);
- commit 2 (data sequence number 2) appends ONE file into a second manifest M2:
  - file C: ids 110..114, `value = id * 1.5`, `first_row_id` base 10.

No file materializes `_row_id`. Every row's `_row_id` is its file base plus its
within-file position, and its `_last_updated_sequence_number` is the data
sequence number of the commit that appended it. The oracle:

| id  | _row_id | _last_updated_sequence_number | data file | manifest | first_row_id | data_sequence_number |
|-----|---------|-------------------------------|-----------|----------|--------------|----------------------|
| 100 | 0       | 1                             | file A    | M1       | 0            | 1                    |
| 101 | 1       | 1                             | file A    | M1       | 0            | 1                    |
| 102 | 2       | 1                             | file A    | M1       | 0            | 1                    |
| 103 | 3       | 1                             | file A    | M1       | 0            | 1                    |
| 104 | 4       | 1                             | file A    | M1       | 0            | 1                    |
| 105 | 5       | 1                             | file B    | M1       | 5            | 1                    |
| 106 | 6       | 1                             | file B    | M1       | 5            | 1                    |
| 107 | 7       | 1                             | file B    | M1       | 5            | 1                    |
| 108 | 8       | 1                             | file B    | M1       | 5            | 1                    |
| 109 | 9       | 1                             | file B    | M1       | 5            | 1                    |
| 110 | 10      | 2                             | file C    | M2       | 10           | 2                    |
| 111 | 11      | 2                             | file C    | M2       | 10           | 2                    |
| 112 | 12      | 2                             | file C    | M2       | 10           | 2                    |
| 113 | 13      | 2                             | file C    | M2       | 10           | 2                    |
| 114 | 14      | 2                             | file C    | M2       | 10           | 2                    |

### first_row_id is inherited cumulatively, not written per data file

In the manifest that lists a data file, the Avro field `data_file.first_row_id`
(type `["null","long"]`, Avro field-id 142) is NULL for ALL three data files,
including file B (the second entry in a multi-file manifest). The base comes from
the manifest-list entry: the Avro field `ManifestFile.first_row_id` (type
`["null","long"]`, Avro field-id 520) is 0 for manifest M1 (which lists files A
and B, in that entry order) and 10 for manifest M2 (file C). A reader must
therefore compute a file's base as the containing `ManifestFile.first_row_id`
plus the cumulative `record_count` of prior data-file entries in that same
manifest: file A = 0 + 0 = 0, file B = 0 + 5 = 5, file C = 10 + 0 = 10. Because
file B's `data_file.first_row_id` is NULL and its true base (5) is only
recoverable from M1's base (0) plus file A's record count (5), cumulative
within-manifest inheritance is required, not optional. The table metadata also
records `next-row-id: 15` and, per snapshot, `first-row-id` (0 and 10) with
`added-rows` (10 and 5).

### Regenerate

```bash
cd internal/parquetry-testkit/fixtures-gen/iceberg-row-lineage/fresh-gen

# 1. Build the raw warehouse with the Iceberg Java API (Java 17).
#    Writes ./work/warehouse/events (absolute generation paths) and prints the
#    per-file first_row_id / data_sequence_number and the per-row oracle.
JAVA_HOME=/Users/groldan/.sdkman/candidates/java/17.0.17-tem \
  /Users/groldan/.sdkman/candidates/maven/current/bin/mvn -q -e compile exec:java \
  -Dexec.mainClass=rowlineagegen.FreshRowLineageFixtureGenerator \
  -Dexec.jvmArgs="--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED"

# 2. Normalize paths and vendor into
#    src/main/resources/iceberg-row-lineage/fresh.
#    Reuses the iceberg-deletes venv (system pyarrow + fastavro):
cd ..
../iceberg-deletes/.venv/bin/python normalize_and_vendor_fresh.py
```

`normalize_and_vendor_fresh.py` rewrites the absolute generation path to the
logical root `file:///iceberg-row-lineage/fresh` in the metadata JSON and the
manifest list / manifest Avro (every spelling: the schemed `file:` form the
Iceberg Java writer records in the metadata and the bare filesystem path it
records in the manifest `data_file.file_path`). The data Parquet files have no
`file_path` column; no Parquet path rewrite applies. The row-lineage bases are
numeric and path-free and are left untouched. The script keeps only the current
metadata document as `v1.metadata.json` and drops `version-hint.text` and the
Hadoop `.crc` checksums.

`fresh-gen/target/`, `fresh-gen/work/`, and `fresh-gen/cp.txt` are regeneration
scratch and are not committed.

## What `materialized/` contains

A `format-version=3`, unpartitioned table `events(id long required, value
double)` with two data files:

- data file 0: ids 0..4, `first_row_id` base 0, data sequence number 1, no
  `_row_id` column (fully synthesized lineage, for contrast);
- data file 1: ids 5..9, `first_row_id` base 5, data sequence number 2, written
  against a schema that ADDS the reserved `_row_id` field (Parquet field id
  2147483540) as a NULLABLE column with a MERGE-with-insert mix of stored and
  null cells: odd within-file positions store 1001 and 1003; even positions store
  null.

The oracle, read through Iceberg's coalesce path, proves per-row resolution for
file 1: a stored cell wins, a null cell resolves to the file base (5) plus the
within-file position. File 0 stays synthesized.

| id | _row_id | _last_updated_sequence_number | data file           | _row_id cell | note                        |
|----|---------|-------------------------------|---------------------|--------------|-----------------------------|
| 0  | 0       | 1                             | file 0              | (no column)  | synthesized (base 0 + pos 0)|
| 1  | 1       | 1                             | file 0              | (no column)  | synthesized                 |
| 2  | 2       | 1                             | file 0              | (no column)  | synthesized                 |
| 3  | 3       | 1                             | file 0              | (no column)  | synthesized                 |
| 4  | 4       | 1                             | file 0              | (no column)  | synthesized                 |
| 5  | 5       | 2                             | file 1 materialized | null         | null cell -> base 5 + pos 0 |
| 6  | 1001    | 2                             | file 1 materialized | 1001         | stored wins                 |
| 7  | 7       | 2                             | file 1 materialized | null         | null cell -> base 5 + pos 2 |
| 8  | 1003    | 2                             | file 1 materialized | 1003         | stored wins                 |
| 9  | 9       | 2                             | file 1 materialized | null         | null cell -> base 5 + pos 4 |

The physical `_row_id` column of file 1 is `[null, 1001, null, 1003, null]`.
`_last_updated_sequence_number` is the file's data sequence number (2) for the
materialized file; that column is not materialized in this fixture and comes
from `data_sequence_number`. As with `fresh/`, the manifest
`data_file.first_row_id` is NULL and the manifest-list `ManifestFile.first_row_id`
bases are 0 and 5; the metadata records `next-row-id: 10`.

### Regenerate

```bash
cd internal/parquetry-testkit/fixtures-gen/iceberg-row-lineage/materialized-gen

# 1. Build the raw warehouse with the Iceberg Java API (Java 17).
#    Writes ./work/warehouse/events (absolute generation paths), prints the live
#    ids, and reads each file's per-row _row_id / _last_updated_sequence_number
#    through the coalesce path (per-row: stored cells win, null cells resolve to
#    the file base plus position).
JAVA_HOME=/Users/groldan/.sdkman/candidates/java/17.0.17-tem \
  /Users/groldan/.sdkman/candidates/maven/current/bin/mvn -q -e compile exec:java \
  -Dexec.mainClass=rowlineagegen.MaterializedRowLineageFixtureGenerator \
  -Dexec.jvmArgs="--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED"

# 2. Normalize paths and vendor into
#    src/main/resources/iceberg-row-lineage/materialized.
#    Reuses the iceberg-deletes venv (system pyarrow + fastavro):
cd ..
../iceberg-deletes/.venv/bin/python normalize_and_vendor_materialized.py
```

`normalize_and_vendor_materialized.py` mirrors the fresh script against the
logical root `file:///iceberg-row-lineage/materialized`. The second data file
keeps its physical `_row_id` column (Parquet field id 2147483540) with its mix
of stored and null cells untouched. The script keeps only the current metadata
document as `v1.metadata.json` and drops `version-hint.text` and the Hadoop
`.crc` checksums.

`materialized-gen/target/`, `materialized-gen/work/`, and
`materialized-gen/cp.txt` are regeneration scratch and are not committed.

## What `evolved/` contains

A `format-version=3`, unpartitioned table whose schema evolves between its two
commits, which puts one data file on each side of the evolution:

- schema 0 is `events(id long required, value double)`; commit 1 (data sequence
  number 1) appends the legacy file under it: ids 200..204, `value = id * 1.5`,
  `first_row_id` base 0, no materialized lineage column;
- the schema then evolves to `events(id long required, amount double, label
  string)`. The rename keeps field id 2, and `label` is a new field id 3. A
  schema update writes a metadata document without a snapshot, which is why the
  two data commits keep sequence numbers 1 and 2;
- commit 2 (data sequence number 2) appends the evolved file: ids 205..209,
  `amount = id * 1.5`, `label = "row-" + id`, `first_row_id` base 5, written
  against a schema that adds BOTH reserved lineage fields as nullable columns.

The legacy file is short of `label` and names field 2 `value`. A reader must
therefore reconcile it against the current schema rather than pass it through.
That is the
read path this fixture exists to pin: a projection naming a lineage column over a
reconciled file.

The two materialized mixes are staggered on purpose. `_row_id` stores a value at
ODD within-file positions; `_last_updated_sequence_number` stores one at EVEN
positions. No row takes a stored value for both columns, and no row falls back
for both. A reader that confuses the two columns therefore disagrees with the
oracle on every row of the evolved file.

| id  | _row_id | _last_updated_sequence_number | data file | _row_id cell | _last_updated cell | note                                  |
|-----|---------|-------------------------------|-----------|--------------|--------------------|---------------------------------------|
| 200 | 0       | 1                             | legacy    | (no column)  | (no column)        | reconciled, fully synthesized         |
| 201 | 1       | 1                             | legacy    | (no column)  | (no column)        | reconciled, fully synthesized         |
| 202 | 2       | 1                             | legacy    | (no column)  | (no column)        | reconciled, fully synthesized         |
| 203 | 3       | 1                             | legacy    | (no column)  | (no column)        | reconciled, fully synthesized         |
| 204 | 4       | 1                             | legacy    | (no column)  | (no column)        | reconciled, fully synthesized         |
| 205 | 5       | 7000                          | evolved   | null         | 7000               | base 5 + pos 0; stored sequence wins  |
| 206 | 1001    | 2                             | evolved   | 1001         | null               | stored row id wins; sequence from file|
| 207 | 7       | 7002                          | evolved   | null         | 7002               | base 5 + pos 2; stored sequence wins  |
| 208 | 1003    | 2                             | evolved   | 1003         | null               | stored row id wins; sequence from file|
| 209 | 9       | 7004                          | evolved   | null         | 7004               | base 5 + pos 4; stored sequence wins  |

The physical `_row_id` column of the evolved file is `[null, 1001, null, 1003,
null]` and its `_last_updated_sequence_number` column is `[7000, null, 7002,
null, 7004]`. As with the other two tables, the manifest
`data_file.first_row_id` is NULL and the bases come from the manifest-list
`ManifestFile.first_row_id` (0 and 5); the metadata records `next-row-id: 10`.

### Regenerate

```bash
cd internal/parquetry-testkit/fixtures-gen/iceberg-row-lineage/evolved-gen

# 1. Build the raw warehouse with the Iceberg Java API (Java 17).
#    Writes ./work/warehouse/events (absolute generation paths), prints the live
#    ids and the evolved schema, and reads each file's per-row lineage through
#    the coalesce path.
JAVA_HOME=/Users/groldan/.sdkman/candidates/java/17.0.17-tem \
  /Users/groldan/.sdkman/candidates/maven/current/bin/mvn -q -e compile exec:java \
  -Dexec.mainClass=rowlineagegen.EvolvedRowLineageFixtureGenerator \
  -Dexec.jvmArgs="--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED"

# 2. Normalize paths and vendor into
#    src/main/resources/iceberg-row-lineage/evolved.
#    Reuses the iceberg-deletes venv (system pyarrow + fastavro):
cd ..
../iceberg-deletes/.venv/bin/python normalize_and_vendor_evolved.py
```

`normalize_and_vendor_evolved.py` mirrors the sibling scripts against the logical
root `file:///iceberg-row-lineage/evolved`. Keeping only the current metadata
document also keeps the evolution: that document holds both schemas and points
`current-schema-id` at the evolved one, which is what makes the legacy file
reconcile on read.

`evolved-gen/target/`, `evolved-gen/work/`, and `evolved-gen/cp.txt` are
regeneration scratch and are not committed.
