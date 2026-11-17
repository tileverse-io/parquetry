# Filter rows with par

!!! warning "Experimental"
    `par` works, but its commands, options and output may change between releases.

You want the rows matching a condition, such as the countries of a continent or the features inside a box, without writing code. `--filter` takes a SQL `WHERE` expression and applies it inside the read: `par` skips the parts of the file unable to match, and tests the remaining rows one by one. `par explain` shows what was skipped.

## Filter a read

`cat`, `head`, `row-count`, `explain` and `cp` take `--filter`. With `countries.parquet` from [Getting started](../getting-started.md#read-a-file):

```console
$ par cat countries.parquet --columns name,pop_est --filter "pop_est > 50000000"
{"pop_est":5.8005463E7,"name":"Tanzania"}
{"pop_est":3.28239523E8,"name":"United States of America"}
$ par row-count countries.parquet --filter "pop_est > 50000000"
2
```

The columns of the filter need not be among the columns printed:

```console
$ par cat countries.parquet --columns name,iso_a3 \
    --filter "continent = 'Africa' OR pop_est BETWEEN 1000000 AND 40000000"
{"name":"Tanzania","iso_a3":"TZA"}
{"name":"W. Sahara","iso_a3":"ESH"}
{"name":"Canada","iso_a3":"CAN"}
```

## The expressions

`--filter-help` prints the supported expressions. In short:

| Kind | Expressions |
| :--- | :--- |
| Comparisons | `=` `!=` `<>` `<` `<=` `>` `>=` |
| Logic | `AND`, `OR`, `NOT`, parentheses |
| Sets and ranges | `IN (...)`, `NOT IN (...)`, `BETWEEN x AND y`, `IS NULL`, `IS NOT NULL` |
| Spatial relations | `ST_Intersects`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps`, `ST_Disjoint`, `ST_Equals`, `ST_Contains`, `ST_Within`, `ST_Covers`, `ST_CoveredBy`, `ST_DWithin` |
| Query geometries | `ST_GeomFromText('<WKT>')`, `ST_MakeEnvelope(minx, miny, maxx, maxy)` |
| Bounding boxes | `ST_Extent(<geometry column>)`, `ST_Envelope(<geometry column>)` |

The rules:

- The left side of a comparison is a column. The right side is a literal: a number, a `'string'`, `true` or `false`.
- A null value matches no comparison, and no negation of a comparison: `x <> 1`, `NOT (x = 1)` and `x NOT IN (1)` skip it. `IS NULL` selects it.
- A column named after a SQL keyword, or with a space in its name, goes in double quotes: `"from" > 1`, `"my column" = 2`. A double quote inside the name is written twice: `"a""b" = 4`.
- A filter on a nested column names its path, such as `"outer".code = 3`.

An expression outside this list fails with exit code 5:

```console
$ par cat countries.parquet --filter "upper(name) = 'FIJI'"
par: unsupported in --filter: upper (run with --filter-help to list the supported predicates)
```

## Filter by location

The spatial functions test the exact geometries of the rows against a query geometry:

```console
$ par cat countries.parquet --columns name \
    --filter "ST_Intersects(geometry, ST_MakeEnvelope(-100, 30, -90, 40))"
{"name":"United States of America"}
$ par cat countries.parquet --columns name \
    --filter "ST_DWithin(geometry, ST_GeomFromText('POINT(178 -17)'), 2)"
{"name":"Fiji"}
```

- The tests run in the coordinate reference system of the file. `par` does not reproject the query geometry: write its coordinates in that system.
- `ST_Contains` and `ST_Within`, and `ST_Covers` and `ST_CoveredBy`, depend on the order of their arguments. `ST_Within(geometry, <query>)` keeps the rows inside the query geometry.
- `ST_DWithin` takes a distance as its third argument, in the units of the coordinates.

`ST_Extent(geometry)` stands for the bounding box of the geometry. With it, the test compares two rectangles and skips the exact geometry test. Edges count as inside, and Z is ignored:

```console
$ par cat countries.parquet --columns name \
    --filter "ST_Intersects(ST_Extent(geometry), ST_MakeEnvelope(-100, 30, -90, 40))"
{"name":"United States of America"}
```

The rectangle form accepts `ST_Intersects`, `ST_Covers`, `ST_CoveredBy`, `ST_Equals` and `ST_Disjoint`, and a query written as `ST_MakeEnvelope`, `ST_Extent` or `ST_Envelope`. The other relations reject it and say what to use instead:

```console
$ par cat countries.parquet \
    --filter "ST_Contains(ST_Extent(geometry), ST_MakeEnvelope(-20, 20, -10, 30))"
par: ST_Contains has no bounding-box form: box relations include their edges, and a geometry on the query edge answers differently under ST_Contains; use ST_Covers for the box relation, or remove ST_Extent(geometry) to test the exact geometry (run with --filter-help to list the supported predicates)
```

`ST_Envelope` is a synonym of `ST_Extent`. It is the spelling understood by PostGIS for the box of one geometry, where `ST_Extent` aggregates over rows.

[Filter by location](../guides/spatial-filter.md) explains the same tests in the Java API.

## See what a filter skips

`par explain` prints the decisions taken on the metadata of the file, without reading the rows. A Parquet file keeps its rows in row groups, blocks of rows described in the footer at the end of the file. The sample file has a single row group. This copy splits it into row groups of two rows ([Copy files](copy.md)):

```bash
par cp countries.parquet countries-rg.parquet --row-group-rows 2
```

The copy also has `bbox` columns, holding the bounding boxes of the geometries. A spatial filter uses their statistics to skip row groups:

```console
$ par explain countries-rg.parquet \
    --filter "ST_Intersects(geometry, ST_MakeEnvelope(-100, 30, -90, 40))"
predicate: GeometryFilterPredicate[filter=io.tileverse.parquetry.geo.JtsGeometryFilter@76a36b71]
1 kept, 0 skipped  (0 rows pruned without opening a file)

location                                 outcome         rows   reason
countries-rg.parquet                     KEEP               5   kept

countries-rg.parquet:
Tiers: STATS, SPATIAL, COLUMN_INDEX, BLOOM_FILTER, RECORD_LEVEL
RG  Rows  Stats  Spatial  ColIdx  Bloom  Record  Outcome
0   2     ELIM   -        -       -      -       skip   
1   2     n/a    n/a      passed  n/a    -       all    
2   1     n/a    n/a      passed  n/a    -       all    
```

The first line shows the filter as Parquetry received it. The file table comes next: for a directory or an Iceberg table, it lists the files, kept or skipped with the reason. The last table has one line per row group of a kept file, and one column per tier, in the order of the read:

- **Stats**: the minimum and maximum of the column chunks, from the footer. Row group 0 holds Fiji and Tanzania, far from the query box, and its `bbox` statistics rule it out.
- **Spatial**: the bounding box of the geometries of the row group, recorded in the footer of files with native geometry types, such as GeoParquet 2.0 files.
- **ColIdx**: the page index. A column chunk is cut into pages, and the page index records the minimum and maximum of the pages. It skips pages inside a row group.
- **Bloom**: the bloom filters, compact sets answering whether a value may be present in a column chunk.
- **Record**: the test of the rows one by one. `explain` reads no rows, and prints `-`.

A cell holds `ELIM` when the tier ruled the row group out, `NARROW n/m` when it kept n rows out of m, `kept` or `passed` when the row group went on, and `n/a` when the tier had no data or did not apply. A `-` follows an `ELIM`: the later tiers did not run. The `Outcome` column gives the result: `skip`, `all` for rows still to test, `matched` when the statistics proved that all rows match, or a number of rows left.

`--analyze` runs the read as well, and adds per row group the rows matched, the bytes read and the time spent, then the totals and the requests sent to storage. `-o json` prints the plan as JSON.

[Lay out files for selective queries](../guides/file-layout.md) shows how the order of the rows and the page size make these tiers skip more.

## Under the hood

- [The read path](../concepts/read-path.md#4-filtering-the-five-tiers) describes the tiers.
- [Spatial filtering](../concepts/spatial-filtering.md) explains how the `bbox` columns and the bounds of GeoParquet 2.0 files skip row groups and pages.
