# par

`par` is a command-line tool for inspecting and transforming Parquet and GeoParquet files, built on the clean-room
parquetry reader/writer. It reads from local paths and cloud object stores, pushes column projection and SQL-style
filters down into the read, and emits the result either as a new Parquet/GeoParquet file (`cp`) or as JSON, CSV/TSV,
text, or Arrow IPC.

```bash
# First rows as JSON (one object per line)
par head data.parquet

# Project + filter, straight to a JSON-lines stream
par cat data.parquet --columns name,pop --filter "pop > 1000000"

# Stream Arrow IPC into another tool
par cat data.parquet -o arrow | duckdb -c "SELECT count(*) FROM read_arrow('/dev/stdin')"
```

Every command takes a Parquet file path or URI as its argument (`cp` takes a source and a destination).

## Commands

| Command | What it does |
| --- | --- |
| `par schema <uri>` | Print the Parquet message-type tree. |
| `par meta <uri>` | Print a one-screen file summary. |
| `par cat <uri>` | Decode rows to stdout (jsonl by default). |
| `par head <uri>` | Decode the first rows to stdout (jsonl by default); defaults to 10 rows. |
| `par cp <src> <dst>` | Read src, apply projection/filter, write a new Parquet file at dst (local or cloud). |
| `par explain <uri>` | Print the filter/scan plan for a file. |
| `par row-groups <uri>` | List the file's row groups. |
| `par row-count <uri>` | Print the number of rows (footer-only when there is no `--filter`). |
| `par stats <uri>` | Print per-column statistics. |

Run `par`, `par --help`, or `par <command> --help` for the full usage of any command.

## Common options

These apply to the row/scan commands (`cat`, `head`, `cp`, `explain`, `row-count`):

| Option | Meaning |
| --- | --- |
| `--columns <col>` | Leaf columns to keep, comma-separated (e.g. `--columns name,pop` or `--columns addr.city`). Default: all. |
| `--filter <sql>` | A SQL `WHERE` predicate, pushed down into the read. See [Filtering](#filtering). |
| `--filter-help` | List the supported `--filter` predicates and exit. |
| `--limit <n>` | Cap the number of rows emitted (or written, for `cp`). |
| `-o, --format <fmt>` | Output format (see [Output formats](#output-formats)). |
| `-v, --verbose` | Print full stack traces on error instead of a one-line message. |

## Output formats

`-o/--format` accepts a different set per command:

- **`cat` / `head`**: `jsonl` (default), `csv`, `tsv`, `text`, `arrow`.
- **`schema`, `meta`, `explain`, `row-groups`, `stats`**: `text` (default) and `json`.
- **`row-count`**: prints a plain integer (no format option).

Notes:

- **Nested and Variant columns render as real JSON.** In `jsonl`, a list becomes a JSON array, a struct/map becomes a
  JSON object, and a Parquet Variant renders inline as its natural JSON value. In `csv`/`tsv`/`text`, a nested or
  Variant top-level field renders as a single compact-JSON cell; flat columns render as scalars.
- **`-o arrow`** writes a binary Arrow IPC stream to stdout (flat columns only), for piping into Arrow-aware tools such
  as DuckDB, polars, or pandas. Geometry columns are tagged with the `geoarrow.wkb` extension.

## Filtering

`--filter` takes a SQL `WHERE` expression and applies it as an exact, pushed-down predicate. Run `par cat --filter-help`
(on any filtering command) to print the supported set:

- **Comparisons**: `=` `!=` `<>` `<` `<=` `>` `>=`
- **Logical**: `AND`, `OR`, `NOT`
- **Sets / ranges**: `IN (...)`, `NOT IN (...)`, `BETWEEN x AND y`, `IS NULL`, `IS NOT NULL`
- **Spatial relations**: `ST_Intersects`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps`, `ST_Disjoint`, `ST_Equals`,
  `ST_Contains`, `ST_Within`, `ST_Covers`, `ST_CoveredBy`, `ST_DWithin(geom, query, distance)`
- **Query geometry**: `ST_GeomFromText('WKT')`, `ST_MakeEnvelope(minx, miny, maxx, maxy)`

The left side of a comparison is a column; the right side is a literal (number, `'string'`, or `true`/`false`).
`ST_Contains`/`ST_Within` and `ST_Covers`/`ST_CoveredBy` honor argument order. Spatial tests run in the file's native
CRS; the query geometry is assumed to already be in that CRS (no reprojection). An unsupported predicate fails with
exit code 5 and a message pointing back at `--filter-help`.

```bash
par cat cities.parquet --filter "name IN ('Rosario', 'Cordoba')"
par cat cities.parquet --filter "pop BETWEEN 100000 AND 2000000 AND capital = true"
par cat geo.parquet --columns id \
  --filter "ST_Intersects(geometry, ST_MakeEnvelope(-61, -33.5, -60, -32.5))"
```

## Files and cloud storage

The `<uri>` argument is a local path or a URI; the backend is chosen from the scheme:

- a bare path or `file://...` for the local filesystem,
- `s3://bucket/key` for Amazon S3 and S3-compatible stores,
- `gs://bucket/key` for Google Cloud Storage,
- `az://account/container/blob` (or an `https://account.blob.core.windows.net/...` URL) for Azure Blob Storage, and `abfs(s)://` for ADLS Gen2,
- `http(s)://host/path` for HTTP range-request servers.

Connection settings for the source come from these options (when not using ambient credentials / default chains):

| Option | Meaning |
| --- | --- |
| `--provider <id>` | Force the storage provider: `s3`, `gcs`, `azure`, `http`, `file`. |
| `--region <region>` | S3 region. |
| `--access-key <key>` / `--secret-key <key>` | S3 credentials. |
| `--path-style` | S3 path-style addressing (for MinIO / S3-compatible endpoints). |
| `--anonymous` | Access the store without credentials. |
| `--gcs-project <project>` | Google Cloud project id. |
| `--endpoint <url>` | Service endpoint override for S3, GCS, and Azure (e.g. `http://localhost:9000` for MinIO, `http://localhost:4443` for fake-gcs-server). |

`cp` writes a new Parquet file to a local path or a cloud URI, regenerating GeoParquet metadata on the destination and
forwarding other key-value metadata. A destination ending in `/` (or an existing local directory) writes the source
filename inside it. The destination has its own connection options mirroring the source ones, prefixed `--dst-`
(`--dst-provider`, `--dst-region`, `--dst-access-key`, `--dst-secret-key`, `--dst-path-style`, `--dst-anonymous`,
`--dst-gcs-project`, `--dst-endpoint`), plus `-f`/`--overwrite` to replace an existing destination. This lets `cp`
move data between two different stores in one command.

The write path currently handles flat columns (primitives, including WKB geometry); copying a file whose schema
contains nested list/map/struct or Variant columns is not yet supported and fails with a clear error.

## Shell completion

`par generate-completion` prints a bash completion script (also usable from zsh). Source it for the current shell, or install it to persist:

```bash
# bash, current shell
source <(par generate-completion)

# bash, persistent (bash-completion v2)
par generate-completion > ~/.local/share/bash-completion/completions/par

# zsh, current shell (enable bash-style completion first)
autoload -Uz compinit && compinit
autoload -Uz bashcompinit && bashcompinit
source <(par generate-completion)
```

This works the same for the JVM launcher and the native binary, since `generate-completion` is a built-in subcommand.

## Exit codes

| Code | Name | Meaning |
| --- | --- | --- |
| 0 | OK | Success. |
| 1 | GENERIC | Unclassified error. |
| 2 | USAGE | Bad arguments or unknown option. |
| 3 | SCHEMA | Schema mismatch (e.g. a projected column does not exist). |
| 4 | FORMAT | Malformed file or an unsupported on-disk feature. |
| 5 | FILTER | A `--filter` expression could not be parsed or translated. |

Errors print as `par: <message>` on stderr; add `-v` for a full stack trace.

## Examples

```bash
# Inspect
par schema data.parquet
par meta data.parquet
par row-count data.parquet
par stats -o json data.parquet
par row-groups -o json data.parquet

# Read rows
par head data.parquet                      # first 10 rows as jsonl
par cat data.parquet --limit 100 -o csv    # first 100 rows as CSV
par cat data.parquet --columns name,pop --filter "pop > 1000000"

# Understand a query before running it
par explain data.parquet --filter "pop > 1000000"

# Pipe Arrow into a query engine
par cat data.parquet -o arrow | duckdb -c "SELECT * FROM read_arrow('/dev/stdin') LIMIT 5"

# Read from S3 (path-style / custom endpoint, e.g. MinIO)
par cat s3://bucket/data.parquet --provider s3 --endpoint https://minio.example:9000 --path-style

# Copy with projection + filter, local to GCS
par cp data.parquet gs://bucket/out/ --columns name,pop --filter "pop > 1000000" --dst-gcs-project my-project
```
