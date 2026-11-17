# Filter rows

You want the rows matching a condition without reading the whole file. Pass a predicate to the read: Parquetry skips the parts of the file unable to match, and returns the matching rows. This guide builds predicates, counts the matching rows and shows what a filter skips.

It needs `parquetry-core`. [Getting started](../getting-started.md) adds the dependency and downloads the `countries.parquet` file used below.

## Filter a read

The program prints the countries in Africa or with at least 100 million inhabitants:

```java
import static io.tileverse.parquetry.filter.Pred.col;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class FilterCountries {

    private static final ColumnPath NAME = ColumnPath.of("name");

    public static void main(String[] args) {
        Path path = Path.of("countries.parquet");
        Predicate africa = col("continent").eq("Africa");
        Predicate populous = col("pop_est").gtEq(100_000_000.0);
        Predicate filter = africa.or(populous);
        Projection columns = Projection.ofPhysical(List.of(NAME));

        try (ByteRangeSource source = ByteRangeSource.ofFile(path)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(filter, columns, ReadOptions.DEFAULTS)) {
                rows.forEach(row -> System.out.println(row.getString(NAME)));
            }
        }
    }
}
```

It prints `Tanzania`, `W. Sahara` and `United States of America`.

The predicate is evaluated inside the read. A Parquet file stores its rows in blocks called row groups. The footer of the file records the minimum and maximum of the columns of a row group. A row group cuts its columns into pages, and the page index of the file records their minimum and maximum. Parquetry uses these records to skip the row groups and pages that cannot match, then tests the remaining rows one by one. The stream returns only the matching rows. The filter columns do not have to be in the projection.

## Build a predicate

`Pred.col(name)` starts a predicate on a column. `Pred.col("a", "b")` names the `b` field of the `a` struct.

| Predicate | Builder |
| :--- | :--- |
| equal, not equal | `eq(v)`, `notEq(v)` |
| less, less or equal | `lt(v)`, `ltEq(v)` |
| greater, greater or equal | `gt(v)`, `gtEq(v)` |
| in a list of values | `inInts(...)`, `inStrings(...)`, `inUuids(...)` |
| null, not null | `isNull()`, `isNotNull()` |
| bounding box relations | see [Filter by location](spatial-filter.md) |

Combine predicates with `and`, `or` and `negate` on a predicate, or with `Pred.and(...)`, `Pred.or(...)` and `Pred.not(...)`. `Predicate.ALWAYS_TRUE` selects the whole file.

The type of the value must match the type of the column:

- an `int` or a `long` for an `INT32` or `INT64` column;
- a `float` or a `double` for a `FLOAT` or `DOUBLE` column;
- a `String` for a string column;
- a `LocalDate` for a `DATE` column, a `UUID` for a `UUID` column.

A value of another type fails the read with a `ParquetSchemaException`: `col("pop_est").gtEq(100_000_000)` passes an `int` to a `DOUBLE` column and fails. A predicate on a column absent from the file fails the same way.

The builders take these Java types:

- `eq` and `notEq` take an `int`, a `long`, a `double` or a `String`. `eq` also takes a `float`, a `boolean`, a binary value, a `LocalDate`, a `LocalDateTime` with a flag telling whether the column is adjusted to UTC, and a `UUID`.
- `lt`, `ltEq`, `gt` and `gtEq` take an `int`, a `long` or a `double`. `lt` and `gtEq` also take a `UUID`.

The other comparisons, such as a range of strings or of dates, are records of `Predicate`, built with a `Value`:

```java
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;
import java.time.LocalDate;

Predicate fromT = new Predicate.GtEq(ColumnPath.of("name"), new Value.StringVal("T"));
Predicate since2020 = new Predicate.GtEq(ColumnPath.of("opened"), new Value.DateVal(LocalDate.of(2020, 1, 1)));
```

On `countries.parquet`, `fromT` selects `Tanzania`, `United States of America` and `W. Sahara`. `since2020` selects the rows opened in 2020 or later, on a file with a `DATE` column named `opened`. `Value` also has records for timestamps, times and decimals.

[Float statistics](../concepts/float-statistics.md#3-how-parquetry-reads-them) describes how comparisons treat `NaN` and the two zeros.

## Filter on a list

A column inside a list holds several values per row, and a plain comparison has no single answer for them. Wrap the comparison in `Predicate.Quantified` and say how many elements must match:

```java
import static io.tileverse.parquetry.filter.Pred.col;

import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;

Predicate taggedPort = new Predicate.Quantified(MatchAction.ANY, col("tags", "list", "element").eq("port"));
```

`MatchAction.ANY` keeps a row when at least one element matches, `ALL` when each element matches, and `ONE` when exactly one element matches. The path names the leaf of the list in the standard layout: `tags.list.element` for a list of strings named `tags`. A comparison on such a column without `Quantified` fails the read with a `ParquetSchemaException`.

## Count the matching rows

`count` returns the number of matching rows without building them:

```java
long matches = reader.count(filter, ReadOptions.DEFAULTS);
```

With the predicate of the first sample, it returns 3. [Counting](../concepts/counting.md) explains how a count answers from statistics where it can.

## See what a filter skips

`explain` runs the pruning tiers on the footer and the indexes, without reading data, and returns the decisions of the tiers, row group by row group:

```java
import io.tileverse.parquetry.filter.explain.ExplainPlan;

ExplainPlan plan = reader.explain(filter, columns, ReadOptions.DEFAULTS);
System.out.println(plan.toAsciiTable());
```

`plan.toJson()` renders the same plan as JSON. The `par explain` command prints it from the command line ([Filter rows with par](../cli/filtering.md#see-what-a-filter-skips)).

## Turn a tier off

`ReadOptions.DEFAULTS` turns the pruning tiers on. The builder turns a tier off for one read, for example to measure what it saves or to work around a file with wrong statistics:

```java
import io.tileverse.parquetry.data.ReadOptions;

ReadOptions options = ReadOptions.builder().useColumnIndexFilter(false).build();
```

| Switch | What it turns off |
| :--- | :--- |
| `useStatsFilter` | Pruning row groups by the min/max statistics of their columns. |
| `useColumnIndexFilter` | Pruning pages by the page index. A read with this switch off also learns no pages from page headers, and uses none of the pages learned by earlier reads. |
| `useBloomFilter` | Pruning row groups by bloom filters, for `eq` and `in` predicates. |
| `useRecordLevelFilter` | Testing the rows one by one. The read then returns the rows of the row groups and pages kept by the other tiers, matching or not. |
| `usePageNarrowedFetch` | Fetching only the surviving pages of a column. The read fetches whole column chunks, the values of a column in a row group, and still skips the pruned pages when decoding. |

The pruning tiers skip work. They never change which rows a read returns, as long as the record-level filter stays on.

## Write files that filter well

Pruning works from the metadata of the file. A file prunes well when similar values sit together, in the same row groups and pages:

- Sort the rows by the columns used in filters before writing them.
- Write a page index. Parquetry's writer writes one by default. Without one, a read learns the pages from their headers, as described in [Without a page index](../concepts/read-path.md#without-a-page-index).
- Write bloom filters on the columns compared with `eq` or `in` against many distinct values. [Write files](write.md#options-that-matter) shows how.

[Lay out files for selective queries](file-layout.md) gives the recommended layout and how to write it.

## Where to go next

- [Filter by location](spatial-filter.md) filters GeoParquet rows by bounding box and by geometry.

## Under the hood

- [The read path](../concepts/read-path.md#4-filtering-the-five-tiers) describes the pruning tiers.
- [Counting](../concepts/counting.md) explains why a count costs less than a read.
