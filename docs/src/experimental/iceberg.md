# Apache Iceberg tables

!!! warning "Experimental"
    `parquetry-iceberg` and its GeoTools store work, but their API, options and behavior may change between releases.

You have an Apache Iceberg table on a file system or in object storage, and want to read it from Java or publish it in GeoServer, without the Iceberg and Hadoop libraries. `parquetry-iceberg` reads the table as one dataset: one count, one stream of rows, and the data files unable to match the filter left unopened. An Iceberg table is a set of Parquet data files, listed by metadata files: a `metadata.json` document names the snapshots of the table, and manifest files list the data files of a snapshot.

It needs `parquetry-iceberg`, with the BOM imported as in [Getting started](../getting-started.md#add-the-dependency):

```xml
<dependency>
  <groupId>io.tileverse.parquetry</groupId>
  <artifactId>parquetry-iceberg</artifactId>
</dependency>
```

Like `parquetry-catalog`, it uses Java 25 preview features: start the JVM with `--enable-preview` ([Getting started](../getting-started.md#requirements)).

## Read a table

The directory `orders` holds a table partitioned by `category`: the partitions `a`, `b` and `c` hold 100 rows each, with an `id` and an `amount` column. The program counts the rows, prints the first three ids of category `b`, and shows which data files the filter skips:

```java
import static io.tileverse.parquetry.filter.Pred.col;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.iceberg.IcebergOptions;
import io.tileverse.parquetry.iceberg.IcebergTableCatalog;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import java.nio.file.Path;
import java.util.stream.Stream;

public class ReadIcebergTable {

    private static final ColumnPath ID = ColumnPath.of("id");

    public static void main(String[] args) {
        Path tableDir = Path.of("orders");
        Predicate filter = col("category").eq("b");

        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            String name = catalog.datasets().getFirst();
            ParquetDataset table = catalog.dataset(name);
            long total = table.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
            long matching = table.count(filter, ReadOptions.DEFAULTS);
            System.out.println(name + ": " + total + " rows, " + matching + " in category b");

            try (Stream<ParquetRecord> rows = table.read(filter, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.limit(3).forEach(row -> System.out.println(row.getLong(ID)));
            }

            DatasetExplainPlan plan = table.explain(filter, Projection.ALL, ReadOptions.DEFAULTS);
            System.out.println(plan.toAsciiTable());
        }
    }
}
```

It prints, with the plan cut after its list of files:

```
orders: 300 rows, 100 in category b
1000
1001
1002
predicate: Eq[col=ColumnPath[dot=category], v=StringVal[value=b]]
1 kept, 2 skipped  (200 rows pruned without opening a file)

location                                 outcome         rows   reason
file:///iceberg-partitioned/by_category/data/category=a/00000-0-1f83630d-08c1-4462-a8ca-10b98714a70a.parquet SKIP             100   Eq category: value outside [min, max]
file:///iceberg-partitioned/by_category/data/category=b/00000-1-1f83630d-08c1-4462-a8ca-10b98714a70a.parquet KEEP             100   kept
file:///iceberg-partitioned/by_category/data/category=c/00000-2-1f83630d-08c1-4462-a8ca-10b98714a70a.parquet SKIP             100   Eq category: value outside [min, max]
```

- `IcebergTableCatalog.openLocal` finds the current `metadata.json` of the table: the version named by `metadata/version-hint.text`, or else the highest `vN.metadata.json`. It reads the current snapshot and its manifests.
- The catalog holds one dataset, named after the directory. The dataset has the read methods of [Multi-file datasets](../guides/datasets.md): `read`, `readBatches`, `count`, `bounds` and `explain`.
- The locations in the plan are the ones recorded in the metadata of the table, when it was written.
- The read skips a data file when the column bounds of its manifest entry, or its partition value, rule it out. It never opens such a file. In a kept file, the read skips row groups and pages as in a single file.
- Closing the catalog closes the data files.

The `par` command line tool reads a table directory the same way: `par head orders` prints its first rows ([The par command line tool](../cli/index.md#read-directories-and-iceberg-tables)).

## Pick a snapshot or a metadata file

`IcebergOptions.defaults()` reads the current snapshot. The builder pins another one, or a metadata document:

```java
IcebergOptions snapshot = IcebergOptions.builder().snapshotId(7189951345053510387L).build();
IcebergOptions metadata = IcebergOptions.builder()
        .metadataLocation(tableLocation + "/metadata/v3.metadata.json")
        .build();
```

## Read from object storage

`IcebergTableCatalog.openStorage` reads a table through a tileverse `Storage` rooted at the table, with the backends of [Cloud storage](../guides/cloud-storage.md#choose-a-backend):

```java
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import java.net.URI;
import java.util.Properties;

URI location = URI.create("s3://my-bucket/warehouse/sales/orders");
Storage storage = StorageFactory.open(location, new Properties());
try (IcebergTableCatalog catalog =
        IcebergTableCatalog.openStorage(location.toString(), storage, IcebergOptions.defaults())) {
    ParquetDataset table = catalog.dataset(catalog.datasets().getFirst());
}
```

The catalog takes ownership of the storage, and closes it with itself. An HTTP server cannot list the `metadata` directory: for a table served over HTTP, pin the metadata document with `metadataLocation`.

## Read a warehouse of tables

`IcebergWarehouseCatalog` reads a directory of tables as one catalog, with one dataset per table:

```java
import io.tileverse.parquetry.iceberg.IcebergWarehouseCatalog;

try (IcebergWarehouseCatalog warehouse = IcebergWarehouseCatalog.openLocal(Path.of("warehouse"))) {
    for (String name : warehouse.datasets()) {
        ParquetDataset table = warehouse.dataset(name);
    }
}
```

The catalog finds the tables below the directory, as `<namespace...>/<table>/metadata/*.metadata.json`, and names the tables after their paths: `warehouse/sales/orders` becomes the dataset `sales.orders`. A table opens on its first use, at its current snapshot. `IcebergWarehouseCatalog.open(location, storage)` reads a warehouse in object storage. For a store unable to list directories, `ofLocalTables` and `ofTables` take the names and locations of the tables instead.

## What it reads

- Iceberg format versions 1, 2 and 3.
- Copy-on-write and merge-on-read tables. Position deletes, equality deletes and deletion vectors apply during the scan, and a deleted row is never returned.
- The current schema of the table, over data files written with older schemas: renamed, added, dropped and reordered top-level columns. An added column reads as null, or as its `initial-default` value. [Limits](#limits) lists the exceptions.
- Type promotions: `int` to `long`, `float` to `double`, and a wider `decimal` precision.
- Data files without field ids, through the `schema.name-mapping.default` property of the table.
- The primitive types, `decimal`, `uuid`, `fixed` and the nanosecond timestamps included. The `geometry` and `geography` types of format version 3 read as WKB geometries, with the coordinate reference system declared in the type.
- The row lineage columns of format version 3, `_row_id` and `_last_updated_sequence_number`, when a projection names them.
- Partitioned tables. The value of an identity partition fills its column, also when the data files leave that column out.

## Limits

- The module reads tables. It does not write them.
- A table is found by its location. The module has no client for the Iceberg REST catalog or other catalog services.
- A read uses the current snapshot, or one pinned by its id. Branches, tags and timestamps do not select a snapshot.
- Only identity partitions skip files by their partition value. A filter on the source column of a `day`, `bucket` or `truncate` partition skips files by the column bounds of the manifests, not by the partition value.
- Nested columns are matched by name. A data file whose nested field ids disagree with the table, as after the rename of a nested field, fails with an error rather than return wrong values.
- An added `geometry` or `geography` column cannot read as null. A read of a data file written before the column was added fails with an error.
- A table with an `initial-default` on a struct, list or map column, or on a nanosecond timestamp column, fails to open.

## In GeoTools and GeoServer

The GeoTools store selected by the `iceberg` parameter opens a table directory, or a warehouse with one feature type per table, locally or in object storage. Its `fid` parameter names the column of the feature ids, and the `storage.*` parameters configure the backend as for [the "Parquet" store](../geoserver/geotools-datastore.md#connection-parameters). GeoServer Cloud ships this store disabled ([Experimental parts](index.md#in-geotools-and-geoserver)).
