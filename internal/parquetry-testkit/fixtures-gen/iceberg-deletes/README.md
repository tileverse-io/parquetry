# iceberg-deletes fixtures

Generates the vendored Iceberg merge-on-read delete tables under
`internal/parquetry-testkit/src/main/resources/iceberg-deletes/`. The reference
writer (Apache Iceberg via Spark) is the oracle: the expected live-row set is
what Iceberg itself reports after the delete.

Currently produced:

- `positional/` - an Iceberg v2 table with one positional delete file.

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
