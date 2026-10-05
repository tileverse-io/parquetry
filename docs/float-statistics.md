# Float statistics in parquetry

How parquetry writes and reads the min/max statistics of `FLOAT`, `DOUBLE` and
`FLOAT16` columns, what it does with `NaN` and the two zeros, and when to ask
for IEEE 754 total order. Read [The parquetry read path](read-path.md) first for
the pruning tiers fed by these statistics.

The guiding rule: **the readers tested open the default output and prune it
like the files of other writers; IEEE 754 total order is a choice for files with
known readers.**

---

## 1. Why float bounds need rules

Min and max statistics let a reader skip a row group or a page without decoding
it. Two kinds of floating-point values break the plain "smallest and largest
value" contract:

- **`NaN`** is unordered: `<`, `<=`, `>`, `>=` and `=` are false between `NaN`
  and any value, `NaN` included. Bounds computed over `NaN` cells hide the
  numbers of the column, and readers disagree on whether `NaN` sorts above or
  below the numbers.
- **`-0.0` and `+0.0`** are equal under IEEE 754 comparison and differ as bit
  patterns. A minimum of `+0.0` hides a `-0.0` cell from a reader ordering the
  two.

The Parquet format answers with a `nan_count` statistic and with a *column
order* declared per column in the footer. The order tells a reader how the
bounds of the column were computed.

---

## 2. What parquetry writes

Float columns declare the **type-defined order** by default. A write can ask for
**IEEE 754 total order** instead (section 4). Both orders record a NaN count.

| | Type-defined order (default) | IEEE 754 total order |
|---|---|---|
| `nan_count` | chunk statistics, page headers, column index | the same |
| Bounds of a chunk or page | its smallest and largest number; `NaN` cells are left out | the same |
| A zero bound | `-0.0` as a minimum, `+0.0` as a maximum | the zero of the cell: `-0.0` orders before `+0.0` |
| Chunk or page of only `NaN` | no bounds | its smallest and largest `NaN`, sign and payload bits included |
| Chunk with a page of only `NaN` | no column index, as required by the format | keeps its column index |
| Deprecated `min` / `max` | written for `FLOAT` and `DOUBLE` | the same |

A `FLOAT` or `DOUBLE` column annotated `UNKNOWN` keeps the type-defined order in
either case.

Dictionary pages keep the bit patterns of `NaN` cells and of the two zeros: a
cell reads back with its sign and payload intact.

---

## 3. How parquetry reads them

`FLOAT` and `DOUBLE` predicates compare as IEEE 754 numbers: `<`, `<=`, `>` and
`>=` never match `NaN`, and `-0.0` equals `+0.0`. `NaN` literals are the one
extension: `x = NaN` and `IN` with a `NaN` match the `NaN` cells, and
`x <> NaN` matches the other non-null cells. `NOT` over a comparison reads as
the complementary operator: `NOT (x > 5)` is `x <= 5`, and matches no `NaN`.
Pruning has to agree with that row by row. `FLOAT16` cells compare as bytes,
and their bounds and NaN counts do not prune.

- **Bounds** prune comparisons with numbers. A `NaN` bound is ignored, and a
  zero bound matches either zero.
- **NaN counts** rule on the `NaN` cells:
  - `x = NaN` skips a row group or page with a NaN count of zero;
  - a comparison with a number, and `x <> NaN`, skip a row group or page holding
    nothing but `NaN`, known from NaN and null counts adding up to its values;
  - a row group with a NaN count of zero and no null can match a comparison as a
    whole, and [count](counting.md) then answers from metadata.
- **Without NaN counts** (files from older writers) `NaN` cells may sit
  anywhere: `x = NaN` prunes nothing, and no float row group matches as a whole.

The rules are the same in both column orders: what is known of `NaN` cells comes
from the counts, never from a `NaN` bound. Only a total-order column index lists
a page of only `NaN` (section 2), and only such an index lets a read skip that
page.

`par explain` shows these decisions per row group and tier.

---

## 4. Asking for IEEE 754 total order

Total order is the order recommended by the format for float columns: it leaves
no ambiguity on `NaN` and on the zeros, and a chunk with a page of only `NaN`
keeps its column index. The cost is compatibility: the order entered the format
in parquet-format 2.13.0 (June 2026), and several readers in use today cannot
open a file written in it or do not prune its float columns (section 5).

```java
WriteOptions options = WriteOptions.builder()
        .floatColumnOrder(WriteOptions.FloatColumnOrder.IEEE_754_TOTAL_ORDER)
        .build();
```

```
par cp --float-column-order ieee_754_total_order in.parquet out.parquet
```

The option applies to the float columns of the file, the `bbox` covering columns
of a GeoParquet file included.

---

## 5. Reader compatibility

Measured in October 2026 on `par cp` copies of a 1M-row file with float32
columns, `NaN` cells and one row group of only `NaN`, against the same queries
on the source file written by pyarrow. "Prunes" means the reader skipped the row
groups ruled out by a filter on a float column: it still answered with their
column chunks overwritten by garbage. The parquet-java row comes from the
parquetry conformance tests, not from this probe.

| Reader | Type-defined order (default) | IEEE 754 total order |
|---|---|---|
| Arrow C++ / pyarrow 16 to 24 | reads, prunes | reads, prunes |
| Arrow C++ / pyarrow 25.0 | reads, prunes | reads, **does not prune** float columns |
| arrow-rs / DataFusion 40 to 50 | reads, prunes | **cannot open the file** |
| arrow-rs / DataFusion 51 to 54 | reads, prunes | reads, prunes |
| Polars 1.0 to 1.40 | reads | **cannot open the file** |
| Polars 1.44 | reads | reads |
| DuckDB 0.10 to 1.5 | reads, prunes | reads, prunes |
| parquet-java 1.18 (conformance tests) | reads | reads |

Notes:

- GDAL and geopandas read through Arrow C++. Their bbox filters on float32
  covering columns were not measured separately.
- The Polars rows say "reads" alone: Polars 1.30 and later did not skip the row
  groups of this query on the pyarrow source either. Polars 1.0 and 1.20
  skipped them on the default copy as on the source.
- parquet-java 1.18 is the oracle of the parquetry conformance tests. For the
  same cells it writes the same bounds and NaN counts in either order, and the
  same column indexes in total order. In the type-defined order it writes no
  column index for a chunk holding a `NaN`; parquetry keeps the index unless a
  page holds only `NaN`. Its filtered reads of parquetry files return the same
  rows with and without pruning.
- parquet-java 1.17 and earlier ignore the statistics of a column in an order
  unknown to them, except for a chunk with equal min and max (per its sources;
  not run here): rows stay correct, with little pruning on those columns.
- pyarrow 25.0, Polars 1.44, DataFusion 54 and DuckDB 1.5 write the type-defined
  order for float columns, as read back from a file written by each.
  parquet-java 1.18 writes total order by default.

---

## 6. Where to look

| Concern | Start in |
|---------|----------|
| Write option | `data/WriteOptions.java` (`FloatColumnOrder`) |
| Order of a column's bounds | `internal/write/BoundsOrder.java` |
| Statistics rules per order | `internal/write/StatisticsAccumulator.java` |
| Footer column orders | `internal/write/FooterColumnOrders.java` |
| Row-group pruning with NaN counts | `internal/filter/StatsEvaluator.java`, `internal/filter/NaNCells.java` |
| Page pruning with NaN counts | `internal/filter/ColumnIndexEvaluator.java` |
| Conformance with parquet-java (tests) | `internal/write/conformance/FloatStatisticsConformanceIT.java`, `FloatTotalOrderConformanceIT.java` |

---

*Scope: the statistics of floating-point columns in a single Parquet /
GeoParquet file. The proof of a whole row group matching, and counting from it,
is in [counting.md](counting.md). The pruning tiers are in
[read-path.md](read-path.md).*
