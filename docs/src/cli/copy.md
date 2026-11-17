# Copy files with par

!!! warning "Experimental"
    `par` works, but its commands, options and output may change between releases.

You have a Parquet or GeoParquet file and want a new file from it: some of its rows and columns, a layout better suited to selective queries, or a copy in another store. `par cp` reads the source with the columns and the filter given, and writes a new Parquet file with Parquetry's writer. [Lay out files for selective queries](../guides/file-layout.md) gives the recipe for the layout, with `par cp`.

## Copy some rows and columns

With `countries.parquet` from [Getting started](../getting-started.md#read-a-file), this command keeps the African countries and three columns:

```console
$ par cp countries.parquet africa.parquet --columns name,pop_est,geometry --filter "continent = 'Africa'"
$ par cat africa.parquet --columns name,pop_est
{"pop_est":5.8005463E7,"name":"Tanzania"}
{"pop_est":603253.0,"name":"W. Sahara"}
```

`--columns`, `--filter` and `--limit` work as in the other commands ([Filter rows](filtering.md)). The filter can name a column left out of the copy, such as `continent` here.

The copy is a GeoParquet file. `par cp` writes new GeoParquet metadata for the columns of the copy, and copies the other key-value metadata of the source. It also adds `bbox` columns, the covering of GeoParquet 1.1: four columns holding the bounding boxes of the geometries, used by readers to skip data under a spatial filter.

```console
$ par schema africa.parquet
message schema {
  optional DOUBLE pop_est;
  optional BYTE_ARRAY name (StringType);
  optional BYTE_ARRAY geometry (Geometry);
  optional group bbox {
    optional FLOAT xmin;
    optional FLOAT ymin;
    optional FLOAT xmax;
    optional FLOAT ymax;
  }
}
```

The `bbox` columns hold `FLOAT` values for a geometry column in longitude and latitude (CRS84 or EPSG:4326), and `DOUBLE` values otherwise.

`par cp` copies struct, list, map and Variant columns. It rejects a column of the legacy `INT96` type with exit code 4, as in this file of the Apache parquet-testing repository. `--columns` can leave that column out:

```console
$ par cp alltypes_plain.parquet copy.parquet
par: cp cannot write INT96 column 'timestamp_col'
```

## Choose the destination

The destination is a local path or a URI, with the schemes of [Read from object storage](index.md#read-from-object-storage).

- A destination ending in `/`, or an existing local directory, receives a file named after the source.
- An existing destination stops the copy, unless `-f` or `--overwrite` is given:

    ```console
    $ par cp countries.parquet countries-copy.parquet
    par: destination exists (use -f to overwrite): countries-copy.parquet
    ```

- The writer keeps its working files in the system temporary directory. `--temp-dir <dir>` puts them elsewhere, for example when that volume is small or read-only.

The source and the destination can sit in two different stores. The storage options of [Read from object storage](index.md#read-from-object-storage) apply to the source. The destination has its own, with the `--dst-` prefix:

| Option | Meaning |
| :--- | :--- |
| `--dst-provider <id>` | Force the backend of the destination: `s3`, `gcs`, `azure`, `http` or `file`. |
| `--dst-region <region>` | The S3 region of the destination. |
| `--dst-access-key <key>`, `--dst-secret-key <key>` | The S3 credentials of the destination. |
| `--dst-path-style` | S3 path-style addressing for the destination. |
| `--dst-endpoint <url>` | The endpoint of S3, Google Cloud Storage or Azure for the destination. |
| `--dst-gcs-project <project>` | The Google Cloud project of the destination. |
| `--dst-anonymous` | Write to the destination without credentials. |

## Change the layout

These options change the layout of the copy. [Lay out files for selective queries](../guides/file-layout.md) explains the choices and the order of the rows.

| Option | Meaning |
| :--- | :--- |
| `--row-group-bytes <size>` | Close a row group at about this size of uncompressed data, such as `256MB`. The units `K`, `M` and `G` are binary. |
| `--row-group-rows <n>` | Close a row group after `n` rows. |
| `--covering-page-values <n>` | The number of values in a page of the `bbox` columns, 1024 by default. The other columns keep the page size of the writer. |
| `--float-column-order <order>` | The order of the statistics of `FLOAT`, `DOUBLE` and `FLOAT16` columns: `type_defined`, the default, or `ieee_754_total_order` ([Choose the order of float statistics](../guides/write.md#choose-the-order-of-float-statistics)). |

A row group is a block of rows described in the footer of the file. A page is a part of a column within a row group, and the unit skipped by the page index. Without `--row-group-bytes` or `--row-group-rows`, row groups hold about 128 MB of uncompressed data.

## Choose the GeoParquet metadata

`--geoparquet-metadata <version>` sets the GeoParquet metadata of the copy:

- `dual`, the default: native geometry types and a 1.1.0 `geo` document, readable by GeoParquet 1.x and 2.0 readers;
- `2.0`: native geometry types and a 2.0.0 document;
- `1.1`: a 1.1.0 document only.

`--bbox-covering <mode>` decides the `bbox` columns of a copy whose source declares no covering: `auto`, the default, `float`, `double` or `none`.

With `auto`, a `bbox` struct already in the source becomes the covering of the copy. DuckDB writes such a struct without declaring it in the metadata. The sample file has one, with its fields in the order `xmax`, `xmin`, `ymax`, `ymin`. The copy declares it, and writes the fields in the order required by GeoParquet 1.1:

```console
$ par cp countries.parquet countries-copy.parquet
$ par schema countries-copy.parquet
message schema {
  ...
  optional group bbox {
    optional DOUBLE xmin;
    optional DOUBLE ymin;
    optional DOUBLE xmax;
    optional DOUBLE ymax;
  }
}
```

The struct must follow these rules:

- Its fields are `xmin`, `ymin`, `xmax` and `ymax`, with `zmin` and `zmax` optional, and no other.
- Its fields are all `FLOAT` or all `DOUBLE`.
- It has the repetition of the geometry column.

If the struct breaks a rule, the copy declares no covering, and `par` prints the reason. Without a `bbox` struct, `auto` derives new `bbox` columns, as in the African copy above. `float` and `double` force the type of derived columns, and `none` writes no covering.

A covering declared by the source is kept whatever the flag says, under the same rules. When the copy cannot keep it, `par` prints the reason and follows the flag.

## Under the hood

- [Write files](../guides/write.md) describes the writer used by `par cp`, and its defaults.
- [Spatial filtering](../concepts/spatial-filtering.md) explains how the `bbox` columns skip row groups and pages.
