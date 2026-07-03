# iceberg-partitioned test fixtures

Two small Apache Iceberg tables that exercise the parquetry-iceberg partitioned-table reader. Both
are partitioned by `identity(category)` over three partitions (`a`, `b`, `c`), 100 rows each (300
total). `id` and `amount` are `long`, `category` is `string`. The `id` ranges are `0..99`,
`1000..1099`, and `2000..2099` for `a`, `b`, `c` respectively, and `amount = id * 2`. Distinct id
ranges per partition let a predicate on `category` be checked against an independent row prediction.

| Table | Layout | Exercises |
| --- | --- | --- |
| `by_category` | the `category` column is retained physically in every data file | read across partitions, partition-value file pruning |
| `by_category_omitted` | the `category` column is dropped from the data files; `id` and `amount` keep field ids 1 and 3 | identity-partition value reconstruction from the manifest partition tuple |

The Iceberg schema field ids are `id=1`, `category=2`, `amount=3`; the partition spec is
`{source-id: 2, field-id: 1000, transform: identity, name: category}`.

## How these were generated

Produced by `generate_fixture.py` in this directory, with:

- pyiceberg 0.11.1 (`pyiceberg[pyarrow,sql-sqlite]`)
- pyarrow 24.0.0
- fastavro 1.12.2

```bash
python3 generate_fixture.py
```

The script:

1. Creates a partitioned table in a throwaway SQLite-catalog warehouse and appends the 300 rows. The
   append makes pyiceberg write real data files, a manifest, a manifest list, and metadata documents.
2. Rewrites the absolute generation path to the clean logical root `file:///iceberg-partitioned/...`
   in the metadata json and in the manifest-list / manifest Avro files (Avro is round-tripped with
   fastavro, preserving its schema, records, codec, and file metadata; only path strings change).
   The reader resolves a table by stripping the metadata `location` prefix from each manifest and
   data path. Any consistent root therefore works, and no personal path is committed. The metadata
   json is also pretty-printed (2-space indent) for readability; the reader ignores the whitespace.
3. Renames the latest metadata document to `v1.metadata.json` (the reader picks the highest
   `vN.metadata.json`) and removes the empty initial metadata document.
4. For `by_category_omitted`, copies the rewritten table, rewrites its root to
   `file:///iceberg-partitioned/by_category_omitted`, and drops the `category` column from each
   Parquet data file with pyarrow while re-stamping `PARQUET:field_id` on the kept `id` and `amount`
   columns. The manifests still record the partition tuple (`category = a|b|c`), which forces the
   reader to reconstruct `category` from the manifest rather than from the file.

Only `by_category/` and `by_category_omitted/` are bundled into the testkit jar (see the resource
includes in `pom.xml`); `generate_fixture.py` and this README are kept in source for reproducibility.

To consume a table from a test, extract it with `TestCorpus.extractDirectory("iceberg-partitioned/by_category", tempDir)`
and open it with `IcebergTableCatalog.openLocal(tempDir, IcebergOptions.defaults())`.
