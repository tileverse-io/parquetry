# The par command line tool

!!! warning "Experimental"
    `par` works, but its commands, options and output may change between releases.

You have a Parquet or GeoParquet file and want to see its schema, its metadata and a few rows without writing code. `par` prints them from the command line, filters rows with a SQL `WHERE` expression, and copies files into a new layout. It reads local files, files on object storage, directories of files and Apache Iceberg tables.

## Build par

`par` is not published as a download. It builds from the source repository, with JDK 25:

```bash
git clone https://github.com/tileverse-io/parquetry.git
cd parquetry
./mvnw -pl :par -am package -DskipTests
```

The build writes a self-contained jar, `apps/par/target/par-<version>-shaded.jar`. The script `apps/par/bin/par` runs that jar with the JVM flags required by Parquetry, `--enable-preview` and `--enable-native-access=ALL-UNNAMED` ([Getting started](../getting-started.md#requirements) explains both). The environment variable `JAVA_OPTS` passes more JVM options to the script, such as `-Xmx2g`.

Put the script on your `PATH`, or define an alias:

```bash
alias par="$PWD/apps/par/bin/par"
par --help
```

It prints the commands:

```
Usage: par [OPTIONS] COMMAND
Inspect and transform Parquet files.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  schema               Print the Parquet message-type tree of a file,
                         directory, or Iceberg table.
  meta                 Print a one-screen file summary.
  cat                  Decode rows of a file, directory, or Iceberg table to
                         stdout (jsonl by default).
  head                 Decode the first rows of a file, directory, or Iceberg
                         table to stdout (jsonl by default).
  cp                   Read src, apply projection/filter, write a new Parquet
                         file at dst.
  explain              Print the file-pruning and scan plan for a Parquet file,
                         directory, or Iceberg table.
  row-groups           List the file's row groups.
  row-count            Print the number of rows in a Parquet file.
  stats                Print per-column statistics.
  generate-completion  Generate bash/zsh completion script for par.
```

`par <command>` without arguments prints the options of that command, after a line naming the missing argument.

## Look at a file

The examples on these pages use the sample file of the GeoParquet specification, saved as `countries.parquet` as in [Getting started](../getting-started.md#read-a-file). `par schema` prints the columns and their types:

```console
$ par schema countries.parquet
message schema {
  optional DOUBLE pop_est;
  optional BYTE_ARRAY continent (StringType);
  optional BYTE_ARRAY name (StringType);
  optional BYTE_ARRAY iso_a3 (StringType);
  optional INT64 gdp_md_est;
  optional BYTE_ARRAY geometry (Geometry);
  optional group bbox {
    optional DOUBLE xmax;
    optional DOUBLE xmin;
    optional DOUBLE ymax;
    optional DOUBLE ymin;
  }
}
```

`par meta` summarizes the footer, the metadata stored at the end of the file. A row group is a block of rows described in the footer:

```console
$ par meta countries.parquet
created by:     parquet-cpp-arrow version 20.0.0
format version: 2
rows:           5
row groups:     1
leaf columns:   10
uncompressed:   23719 bytes
compressed:     21777 bytes
key-value:      ARROW:schema, geo
```

`par head` prints the first rows, ten by default, as one JSON object per line. `--columns` keeps some columns, and `--limit` sets the number of rows:

```console
$ par head countries.parquet --columns name,pop_est --limit 3
{"pop_est":889953.0,"name":"Fiji"}
{"pop_est":5.8005463E7,"name":"Tanzania"}
{"pop_est":603253.0,"name":"W. Sahara"}
```

The columns come in the order of the file, not the order of `--columns`. A nested column is named by its path, such as `--columns bbox.xmin`.

## The commands

| Command | What it prints or does |
| :--- | :--- |
| `par schema <uri>` | The schema of the file, as a Parquet message type. |
| `par meta <uri>` | A summary of the footer: writer, rows, row groups, sizes, key-value metadata. |
| `par head <uri>` | The first rows, ten unless `--limit` says otherwise. |
| `par cat <uri>` | The rows. |
| `par row-count <uri>` | The number of rows, or of matching rows with `--filter`. |
| `par row-groups <uri>` | The row groups, with their rows and sizes. |
| `par stats <uri>` | The statistics of the column chunks: value and null counts, minimum, maximum. |
| `par explain <uri>` | The parts of the file skipped by a filter ([Filter rows](filtering.md)). |
| `par cp <src> <dst>` | A new Parquet file, with a subset of the rows and columns ([Copy files](copy.md)). |
| `par generate-completion` | A completion script for bash and zsh. |

A column chunk holds the values of one column within one row group. `par stats` prints one line per column chunk:

```console
$ par stats countries.parquet
rowGroup  column                type                        values       nulls               min               max
--------  --------------------  ----------------------  ----------  ----------  ----------------  ----------------
0         pop_est               DOUBLE                           5           0          603253.0      3.28239523E8
0         continent             BYTE_ARRAY                       5           0            Africa           Oceania
0         name                  BYTE_ARRAY                       5           0            Canada         W. Sahara
```

The output above is cut after three lines. `par stats` prints the minimum and maximum of a binary column as raw bytes. For a geometry column, these bytes print as unreadable text.

## Options of the row commands

`cat`, `head`, `cp`, `explain` and `row-count` take these options:

| Option | Meaning |
| :--- | :--- |
| `--columns <col>[,<col>...]` | The leaf columns to keep, separated by commas. All columns by default. |
| `--filter <sql>` | A SQL `WHERE` expression, applied inside the read ([Filter rows](filtering.md)). |
| `--filter-help` | Print the supported `--filter` expressions and exit. |
| `--limit <n>` | The maximum number of rows printed, or written by `cp`. |
| `-o`, `--format <format>` | The output format. |
| `-v`, `--verbose` | Print the full stack trace of an error. |

## Output formats

`-o` takes a different set of formats for each command. The names are case-insensitive.

- `cat` and `head` print `jsonl` by default, one JSON object per line. They also print `csv`, `tsv`, `text` and `arrow`.
- `schema`, `meta`, `explain`, `row-groups` and `stats` print `text` by default, and `json`.
- `row-count` prints a plain number.

```console
$ par cat countries.parquet --columns name,continent,pop_est -o csv
pop_est,continent,name
889953.0,Oceania,Fiji
5.8005463E7,Africa,Tanzania
603253.0,Africa,W. Sahara
3.7589262E7,North America,Canada
3.28239523E8,North America,United States of America
```

In `jsonl`, a list prints as a JSON array, and a struct or a map as a JSON object. A Parquet Variant column prints as its JSON value. In `csv`, `tsv` and `text`, a nested column prints as one cell of compact JSON. A binary column without a string type, such as a WKB geometry, prints as Base64 text.

`-o arrow` writes an Arrow IPC stream to standard output, for a tool reading Arrow, such as DuckDB, Polars or pandas. Geometry columns are tagged as `geoarrow.wkb`. With DuckDB 2.0.0 and its `nanoarrow` community extension:

```console
$ par cat countries.parquet --columns name,continent,pop_est -o arrow \
    | duckdb -csv -c "install nanoarrow from community; load nanoarrow;
        select continent, count(*) as n from read_arrow('/dev/stdin')
        group by continent order by continent"
continent,n
Africa,2
North America,2
Oceania,1
```

[Arrow interop](../guides/arrow.md) lists how the Parquet types map to Arrow types.

## Read from object storage

The `<uri>` argument is a local path or a URI. Its scheme selects the storage backend:

- a path, or `file://`, for a local file;
- `s3://bucket/key` for Amazon S3 and S3-compatible services;
- `gs://bucket/key` for Google Cloud Storage;
- `az://account/container/blob`, or an `https://account.blob.core.windows.net/...` URL, for Azure Blob Storage, and `abfs://` or `abfss://` for Azure Data Lake Storage Gen2;
- `http://` or `https://` for a server answering range requests.

`par` includes the backends listed in [Cloud storage](../guides/cloud-storage.md#choose-a-backend). Without options, a backend looks for credentials in its usual places, such as environment variables and configuration files. These options set them on the command line:

| Option | Meaning |
| :--- | :--- |
| `--provider <id>` | Force the backend: `s3`, `gcs`, `azure`, `http` or `file`. |
| `--region <region>` | The S3 region. |
| `--access-key <key>`, `--secret-key <key>` | The S3 credentials. |
| `--path-style` | S3 path-style addressing, as required by MinIO and other S3-compatible services. |
| `--endpoint <url>` | The endpoint of S3, Google Cloud Storage or Azure, such as `http://localhost:9000` for a local MinIO. |
| `--gcs-project <project>` | The Google Cloud project. |
| `--anonymous` | Read without credentials, for a public bucket. |

## Read directories and Iceberg tables

`schema`, `cat`, `head`, `row-count` and `explain` read more than one file as one dataset:

- For a local directory, `par` reads the `.parquet` files below it, subdirectories included. The files must have the same schema.
- For a glob, such as `'data/*.parquet'`, `par` reads the matching files. Quote it to keep the shell from expanding it.
- A remote URI ending in `/` is a prefix. `par` reads the `.parquet` files below it.
- A directory or a prefix holding `metadata/*.metadata.json` is an Apache Iceberg table. `par` reads its current snapshot ([Apache Iceberg tables](../experimental/iceberg.md)).

`meta`, `row-groups`, `stats` and `cp` read one file. [Multi-file datasets](../guides/datasets.md) describes how a dataset reads its files.

## Exit codes

| Code | Meaning |
| :--- | :--- |
| 0 | Success. |
| 1 | Another error, such as a missing file or an unknown column. |
| 2 | Wrong arguments or an unknown option. |
| 3 | A schema error, such as two files of a directory with different schemas. |
| 4 | A malformed file, or a feature of the file not supported by Parquetry. |
| 5 | A `--filter` expression rejected by `par`, such as an unsupported function. |

An error prints as `par: <message>` on standard error. `-v` adds the stack trace.

## Shell completion

`par generate-completion` prints a completion script for bash, also usable from zsh:

```bash
# bash, current shell
source <(par generate-completion)

# zsh, current shell
autoload -Uz compinit && compinit
autoload -Uz bashcompinit && bashcompinit
source <(par generate-completion)
```

## Where to go next

- [Filter rows](filtering.md) covers the `--filter` expressions and `par explain`.
- [Copy files](copy.md) covers `par cp`.
