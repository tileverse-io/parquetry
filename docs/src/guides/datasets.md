# Multi-file datasets

You have a directory, a bucket prefix or a Hive-partitioned tree of Parquet files sharing one schema, and want to query them as one table. `parquetry-catalog` reads them as one dataset: one count, one stream of rows, and the files unable to match the filter left unopened. This guide reads a directory of two files, then shows remote files, partitions, and one dataset per file.

It needs `parquetry-catalog`, with the BOM imported as in [Getting started](../getting-started.md#add-the-dependency):

```xml
<dependency>
  <groupId>io.tileverse.parquetry</groupId>
  <artifactId>parquetry-catalog</artifactId>
</dependency>
```

`parquetry-catalog` uses Java 25 preview features: start the JVM with `--enable-preview`, as listed in [Getting started](../getting-started.md#requirements).

## Read a directory

The directory `cities` holds two files written as in [Write files](write.md), with an `id` and a `name` column. `part-0.parquet` holds Rosario and Montevideo, with the ids 1 and 2. `part-1.parquet` holds Asuncion and Santiago, with the ids 3 and 4. The program counts and prints the cities with an id of 3 or more:

```java
import static io.tileverse.parquetry.filter.Pred.col;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.FileSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import java.nio.file.Path;
import java.util.stream.Stream;

public class ReadDirectory {

    private static final ColumnPath NAME = ColumnPath.of("name");

    public static void main(String[] args) {
        Path directory = Path.of("cities");
        Predicate filter = col("id").gtEq(3L);

        try (FileSource files = FileSource.directory(directory, "*.parquet");
                FilesetCatalog catalog = FilesetCatalog.open(files, CatalogOptions.defaults())) {
            String name = catalog.datasets().getFirst();
            ParquetDataset dataset = catalog.dataset(name);
            long matching = dataset.count(filter, ReadOptions.DEFAULTS);
            System.out.println(name + ": " + matching + " matching rows");
            try (Stream<ParquetRecord> rows = dataset.read(filter, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.forEach(row -> System.out.println(row.getString(NAME)));
            }
        }
    }
}
```

It prints:

```
cities: 2 matching rows
Asuncion
Santiago
```

- `FileSource.directory` lists the files of the directory matching the glob. `*.parquet` matches the files of the directory itself, and `**/*.parquet` the files of its subdirectories too.
- `FilesetCatalog.open` merges the listed files into one dataset, named after the directory. Opening the catalog reads no file. The first call to `dataset(name)` reads the footers of the files.
- The dataset has the read methods of a single file: `read`, `readBatches`, `count`, `bounds` and `explain`, with the same predicates, projections and read options.
- Closing the catalog closes the files and the source.

The files must have equal schemas. A file with another schema fails `dataset(name)` with a `ParquetSchemaException` naming the two files.

A read drains up to eight files at once, and the rows of different files come in no fixed order.

## Skip files

A dataset skips a file when the statistics in its footer rule it out, the way a single file skips row groups. `explain` shows the decisions on the files without reading the rows:

```java
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;

DatasetExplainPlan plan = dataset.explain(filter, Projection.ALL, ReadOptions.DEFAULTS);
System.out.println(plan.toAsciiTable());
```

For the filter of the sample, the plan starts with the files:

```
1 kept, 1 skipped  (2 rows pruned without opening a file)

location                                 outcome         rows   reason
part-0.parquet                           SKIP               2   GtEq id: value outside [min, max]
part-1.parquet                           KEEP               2   kept
```

The ids of `part-0.parquet` lie between 1 and 2, and the read never opens it. A kept file is read like a single file, skipping row groups and pages, and the plan lists these decisions after the files.

## Read files from object storage

`FileSource.open` takes the URI of a directory or a bucket prefix, and a glob:

```java
import java.net.URI;
import java.util.Properties;

URI cities = URI.create("s3://my-bucket/cities/");
FileSource files = FileSource.open(cities, "**/*.parquet", new Properties());
```

A `file:` URI, or a URI without a scheme, names a local directory. Another scheme opens a tileverse `Storage` with the backend modules of [Cloud storage](cloud-storage.md#choose-a-backend), configured by the `storage.*` entries of the properties. The byte-range cache stays off unless the properties set `storage.caching.enabled=true`.

`FileSource.openObject(uri, properties)` serves one known object without listing its prefix. A credential allowed to read the object, and not to list the bucket, is enough.

## Skip partitions

A Hive-partitioned tree puts partition keys and their values in the names of its directories, as `key=value`:

```
cities/
  country=AR/part-0.parquet
  country=UY/part-0.parquet
```

`FileSource.directory(directory, "**/*.parquet")` lists both files, and the catalog reads them as one dataset. A filter on the key skips the files of the other partitions without opening them. With `col("country").eq("UY")`, the plan shows:

```
location                                 outcome         rows   reason
country=AR/part-0.parquet                SKIP               2   Eq country: value outside [min, max]
country=UY/part-0.parquet                KEEP               2   kept
```

The files of the sample have no `country` column. The dataset then adds one, after the columns of the files. For the rows of a file, the added column holds the value written in the path of that file. Its type comes from the values in the paths: whole numbers, decimal numbers, dates written as `yyyy-MM-dd`, or strings otherwise. A file with a `country` column of its own keeps it, and the path value prunes that column.

Such an added column cannot hold null partitions: a path segment `country=__HIVE_DEFAULT_PARTITION__` fails `dataset(name)` with an `IllegalStateException`.

## One dataset per file

`FilesetCatalog.openPerFile(files)` makes each listed file a dataset of its own, named after the file without its extension. The files need not share a schema. Over a directory holding `rivers.parquet` and `lakes.parquet`, `catalog.datasets()` returns `[lakes, rivers]`.

## Where to go next

- [Filter rows](filter.md) builds the predicates, which work the same on a dataset.
- [Cloud storage](cloud-storage.md) lists the backends and their modules.
- [Read columnar batches](read-batches.md) reads column vectors, with `dataset.readBatches`.

## Under the hood

- [The read path](../concepts/read-path.md) follows the read of one file, as run by a dataset on the kept files.
