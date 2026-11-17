# Lay out files for selective queries

You write GeoParquet files that will be queried by small bounding boxes, or Parquet files queried by narrow ranges of a key. With the layout below, such a query fetches the few pages near its answer instead of whole blocks of the file. Parquetry's writer produces this layout by default, except for the order of the rows.

## The recommended layout

A Parquet file stores its rows in row groups, blocks of rows described in the footer at the end of the file. Inside a row group, the values of a column sit together in a column chunk, cut into pages. [Selective queries read little](../index.md#selective-queries-read-little) shows these parts in a diagram.

- **Write a page index.** The page index records the position, minimum and maximum of the pages of a column chunk. A read uses it to skip the pages unable to match the filter, and fetches only the pages left. Without a page index, a first read by Parquetry fetches the whole column chunks of the row groups left. Later reads can prune pages learned from their headers, as described in [Without a page index](../concepts/read-path.md#without-a-page-index).
- **Keep the pages small on the filter columns.** A read skips whole pages, and a small page lets it skip closer to the answer. For a spatial query, the filter columns are the `bbox` covering columns, four columns holding the bounding box of each geometry. Their values are short, four or eight bytes, and small pages on them add little to the size of the file.
- **Write the rows in spatial order.** The minimum and maximum of a page rule it out only when its rows lie close together. Sorting the rows along a space-filling curve, such as a Hilbert curve, puts nearby geometries in the same pages and row groups. When the rows are in random order, the pages span most of the map and the page index rules out nothing.
- **Size row groups by bytes.** The footer describes the row groups and their column chunks, and a reader parses the whole footer before it can run a query. Large row groups keep the footer small, and the page index provides the fine pruning. A size in bytes gives row groups of large polygons and row groups of points a similar weight.

A query on a range of keys follows the same rules, with the key column as the filter column: sort the rows by the key, and keep its pages small when the ranges are narrow.

## Write the layout with the `par` command line tool

[`par cp`](../cli/copy.md) copies a file into this layout. The copy has a page index, covering columns in pages of at most 1,024 values, and row groups of about 128 MB of uncompressed data:

```bash
par cp places.parquet places-indexed.parquet
```

A source without `bbox` covering columns gets them in the copy. [Write a GeoParquet file](write.md#write-a-geoparquet-file) describes the covering written by Parquetry.

The options of `par cp` change the layout:

- `--covering-page-values <n>` sets the number of values in a page of the covering columns. Smaller pages narrow a small box to fewer rows, and make the file and its page index slightly larger.
- `--row-group-bytes <size>` sets the size of a row group in uncompressed bytes, such as `256MB`. `--row-group-rows <n>` sets a row count instead.

`par cp` keeps the order of the source rows. Sort the rows before the copy, for example with DuckDB 2.0.0 and its spatial extension:

```sql
INSTALL spatial;
LOAD spatial;
COPY (
    SELECT * EXCLUDE (bbox) FROM 'places.parquet'
    ORDER BY ST_Hilbert(geometry, ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90)))
) TO 'sorted.parquet';
```

The envelope is the extent of the coordinates, here longitude and latitude. The query leaves out the `bbox` column of the source, and `par cp` derives new covering columns for the copy. A kept `bbox` struct becomes the covering of the copy only when it follows the GeoParquet rules ([Choose the GeoParquet metadata](../cli/copy.md#choose-the-geoparquet-metadata)). For a source without a `bbox` column, remove `EXCLUDE (bbox)`. `par cp sorted.parquet places-indexed.parquet` then writes the copy.

`par explain` shows the effect of the layout. With a filter such as `--filter "ST_Intersects(geometry, ST_MakeEnvelope(10, 45, 11, 46))"`, its `ColIdx` column gives, per row group, the rows left after the page index.

## Write the layout from Java

The default write options produce the layout for a geometry column declared with a coordinate reference system:

```java
import io.tileverse.parquetry.data.WriteOptions;

WriteOptions options = WriteOptions.builder()
        .crsEpsg("geometry", 4326)
        .coveringPageValueLimit(1024)
        .build();
```

- `crsEpsg` declares `geometry` as a geometry column. The writer adds the `bbox` covering columns for it.
- `coveringPageValueLimit(n)` sets the number of values in a page of the covering columns. The call repeats the default, 1,024. `pageValueLimit(column, n)` sets the page size of another column, such as a key column.
- The writer writes a page index for Parquet 2.0 files, the default. A file written with `parquetVersion(ParquetVersion.V1_1)` has no page index.
- Row groups hold about 128 MB of uncompressed data when the options give no expected row count. `expectedRowCount(n)` lets the writer pick larger row groups for large files, and `rowGroupSize(RowGroupSize.bytes(n))` sets the size.
- The writer keeps the order of the appended rows. Sort them before writing.

[Write files](write.md) shows a complete program, and [its options table](write.md#options-that-matter) lists the other settings.

## Write the layout with other tools

A copy through `par cp` gives a file written by another tool a page index and covering columns. Other tools can also write them directly:

- pyarrow 26.0.1 writes no page index by default, and writes one with `write_page_index=True`.
- Writers should leave geometry out of dictionary encoding. Dictionary encoding stores the distinct values of a column once, and WKB geometries rarely repeat. pyarrow 26.0.1 tries a dictionary on each column by default (`use_dictionary=True`). In a file of building footprints, the dictionary of the geometry chunk holds one entry per row, and the chunk then falls back to plain encoding. `use_dictionary` takes a list of columns, and the columns with repeated values keep their dictionary. Parquetry's writer writes geometry columns without a dictionary unless told otherwise.

## Why advice elsewhere centers on row-group size

Guides written for other tools often recommend small row groups for selective queries. The advice follows from the readers targeted by those guides. A reader without page index support skips whole row groups. In a row group kept by the footer statistics, it fetches the column chunks of the queried columns whole. For such a reader, a smaller row group is the only way to fetch less under a selective query.

DuckDB 2.0.0, its latest release when this page was written, reads files this way. It read a file written by `par cp` from an HTTP server: it skipped row groups with the footer statistics, and fetched the whole column chunks of the row group left, although the file had a page index. DuckDB 2.0.0 writes files without a page index. DuckDB merged page index reading and writing after that release, in [pull request 27326](https://github.com/duckdb/duckdb/pull/27326).

Small row groups have a cost for any reader. The footer grows with the number of row groups, and a reader parses the whole footer when it opens the file. A page index narrows a query below the row group without growing the footer. It sits in its own section of the file, and Parquetry loads it only for the row groups left by the footer statistics.

If your files are read mostly by a reader without page index support, smaller row groups help that reader. `--row-group-bytes` and `--row-group-rows` set them, and the page index still serves the readers able to use it.

## Under the hood

- [The read path](../concepts/read-path.md#4-filtering-the-five-tiers) describes the pruning tiers, and how a read handles a file [without a page index](../concepts/read-path.md#without-a-page-index).
- [Spatial filtering](../concepts/spatial-filtering.md) explains how the covering columns prune row groups and pages.
