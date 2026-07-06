# parquetry-iceberg

A clean-room Apache Iceberg table reader on the parquetry dataset/catalog API. It resolves a table from its metadata,
follows the pinned snapshot through the manifest list and manifests, and reads the data files as one queryable dataset.
No dependency on `iceberg-core`, `apache-avro`, or Hadoop: Iceberg's Avro metadata is read through the clean-room
`parquetry-avro` reader and the data files through the core Parquet engine. It plugs into the same `DatasetCatalog` ->
`ParquetDataset` API the pure-parquet and GeoTools paths use, and an Iceberg table reads like any other dataset.

It reads **v1, v2, and v3** tables, copy-on-write and merge-on-read alike: v2 positional and equality delete files and
v3 deletion vectors are applied during the scan, and a deleted row is never returned. The v3 row-lineage columns
(`_row_id`, `_last_updated_sequence_number`) read on request. The table below tracks what reads today and what is next.

## Capabilities

`Full` = implemented. `Partial` = works with the documented limit. `Planned` = not yet; the reader fails fast with a
clear message rather than returning wrong rows. The `Spec` column notes the Iceberg format version a feature belongs to
(`v1+` = since v1).

### Table resolution and I/O

| Feature | Spec | Status | Notes |
| --- | --- | --- | --- |
| Metadata resolution | v1+ | Full | explicit `metadataLocation`, else `version-hint.text`, else highest `vN.metadata.json` |
| Snapshot selection | v1+ | Full | current snapshot, or a pinned `snapshotId` |
| Branches, tags, and as-of-time travel | v2+ | Planned | named refs and timestamp-based selection are not yet wired |
| Manifest list + manifests | v1+ | Full | clean-room Avro reader |
| Data-file read, all format versions | v1-v3 | Full | copy-on-write and merge-on-read |
| Local + object-storage I/O | - | Full | `StorageIcebergFileIO` over tileverse-storage (local, S3, Azure, GCS, HTTP) |
| Warehouse of tables (multi-table catalog) | - | Full | `IcebergWarehouseCatalog`: recursive discovery under a warehouse root (`<root>/<ns...>/<table>/metadata/`), dotted dataset names, lazy per-table open at the current snapshot; explicit name-to-path registry for backends that cannot list |
| REST catalog | - | Planned | the [REST Catalog spec](https://iceberg.apache.org/rest-catalog-spec/) read slice: loadTable, namespace/table listing, OAuth2, vended credentials |

### Schema, types, and evolution

| Feature | Spec | Status | Notes |
| --- | --- | --- | --- |
| Presented schema = the table's current schema | v1+ | Full | from the metadata, not the first data file's footer |
| Primitive types | v1+ | Full | boolean, int, long, float, double, date, string, uuid, binary, ... |
| Native geometry / geography | v3 | Full | Parquet `Geometry` logical type, WKB |
| Variant | v3 | Partial | decoded by the core engine; no Iceberg-specific fixture yet |
| Nested struct / list / map | v1+ | Partial | read by name; field-id reconciliation within nesting is Planned (the main conformance gap) |
| Field-id reconciliation, top-level | v1+ | Full | rename, add (reads as null), drop, reorder |
| Type promotion `int`->`long`, `float`->`double` | v1+ | Full | filters correctly on a promoted column |
| Type promotion `decimal` precision widening | v1+ | Planned | `int`->`long` and `float`->`double` work; decimal widening does not yet |
| Added column of `binary` / `geometry` / `geography` | v1+/v3 | Planned | added scalar columns read as null; these fail fast |
| Column default values | v3 | Full | an added column's `initial-default` reads back for files written before the column existed (primitive types: `int`/`long`/`float`/`double`/`boolean`/`date`/`string`); a non-primitive default fails fast |
| Name mapping for id-less files | v1+ | Full | `schema.name-mapping.default` honored; implicit current-schema mapping when the property is absent |

### Reads and pruning

| Feature | Spec | Status | Notes |
| --- | --- | --- | --- |
| Full scan and count | v1+ | Full | |
| Record-level predicate filtering | v1+ | Full | |
| Bounding-box spatial predicates | v3 | Full | evaluated record-by-record through the engine's spatial contract |
| Manifest-bound file pruning (L3) | v1+ | Full | scalar bounds (`int`/`long`/`float`/`double`/`boolean`/`date`/`string`/`uuid`) + geometry bounds (`packed_xy`, `wkb_point`) |
| Partition-value file pruning | v1+ | Full | an equality or range on an identity-partition column skips whole files before opening them |
| Transform-partition pruning (`days`/`bucket`/`truncate`/...) | v1+ | Planned | a predicate on a transform's source column does not yet prune by partition; only identity-partition values prune |
| Manifest bounds for `timestamp`/`time`/`decimal`/`fixed`/`binary` | v1+ | Planned | a predicate on these does not prune; the file is kept and filtered |
| Row-group pruning inside a file (L4) | v1+ | Full | the engine's per-row-group tiers run on every kept file; a disjoint row group is skipped from the file's native geometry bounds or column statistics, when the writer recorded them |
| Dataset-level explain / analyze | - | Full | reports the file dimension: files kept/skipped, each skip reason, each kept file's row-group plan |
| Column projection on an evolved file | v1+ | Partial | an evolved file presents every table field |
| Exact engine-backed (JTS) geometry filter on a renamed column | v3 | Planned | the filter binds its column at construction; bbox predicates work across a rename |

### Deletes and partitioning

| Feature | Spec | Status | Notes |
| --- | --- | --- | --- |
| Copy-on-write tables | v1+ | Full | |
| Merge-on-read: positional + equality deletes | v2 | Full | applied during the scan; a delete applies only to the data files its sequence number covers |
| Merge-on-read: deletion vectors | v3 | Full | Puffin-serialized roaring bitmaps; a deletion vector supersedes positional deletes for its data file |
| Row lineage (`_row_id`, `_last_updated_sequence_number`) | v3 | Full | projected by name; absent from the schema and the default read. A materialized cell keeps its stored value; a null cell falls back to `first_row_id` + position (`_row_id`) or the file's data sequence number. Below v3 the reserved names are ordinary columns |
| Partitioned tables | v1+ | Full | identity-partition value reconstruction, transform partitions read as-is, partition-value file pruning; `decimal`/`timestamp` partition source types fail fast |

## Spatial grading (CARTO iceberg-geo-testbed)

The CARTO [`iceberg-geo-testbed`](https://github.com/jatorre/iceberg-geo-testbed) grades geospatial Iceberg support on a
five-rung ladder: L0 cannot read, L1 full scan, L2 spatial predicates return correct rows, L3 file-level pruning from the
manifest geometry bounds, L4 row-group pruning inside a file. This module is at **L3** on the testbed's V1/V2/V3
fixtures: on `v3_geometry` (10 files) a California-window query reads 1 file and skips 9, with results identical to a
brute-force scan. The L4 behavior is implemented and proven at the engine level (a kept file's disjoint row groups are
skipped from its native per-row-group geometry bounds); the testbed's data files hold a single row group each, on which
file-level and row-group-level pruning coincide, and a multi-row-group demonstration fixture is the remaining grading
step.

## Reading a table

```java
try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
    ParquetDataset table = catalog.dataset(catalog.datasets().get(0));

    long total = table.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);

    Bbox window = Bbox.of2d(-125.0, 32.0, -115.0, 42.0);
    Predicate inWindow = new Predicate.Spatial.BboxIntersects(ColumnPath.of("geom"), window);
    try (Stream<ParquetRecord> rows = table.read(inWindow, Projection.ALL, ReadOptions.DEFAULTS)) {
        rows.forEach(this::handle);
    }
}
```

`IcebergOptions.builder().snapshotId(id).build()` pins a specific snapshot; the default pins the current one. The catalog
owns the byte sources it opens; `close()` releases them.

A warehouse directory of many tables reads as one catalog, one dataset per table:

```java
try (IcebergWarehouseCatalog warehouse = IcebergWarehouseCatalog.openLocal(warehouseDir)) {
    for (String table : warehouse.datasets()) {
        ParquetDataset dataset = warehouse.dataset(table);
        // read as any other dataset
    }
}
```

`IcebergWarehouseCatalog` discovers every `<root>/<ns...>/<table>/metadata/*.metadata.json` under the warehouse root
and names each table by its path (`ns1/sales/orders` becomes the dataset `ns1.sales.orders`); each table opens lazily at
its current snapshot on first access. For a backend that cannot list a directory, an explicit name-to-path registry
(`ofLocalTables`, `ofTables`) skips discovery. Over object storage, `IcebergWarehouseCatalog.open(warehouseLocation,
storage)` takes ownership of the `Storage` and closes it in `close()`.

To read over object storage, supply a tileverse-storage `Storage` rooted at the table and pass the table location as the
logical root: `IcebergTableCatalog.open(tableLocation, StorageIcebergFileIO.over(storage, tableLocation), options)`. `over(...)`
borrows the `Storage` (the caller closes it); `owning(...)` hands its lifecycle to the catalog. Because an HTTP-served
table cannot list a directory, pin the metadata document with
`IcebergOptions.builder().metadataLocation(tableLocation + "/metadata/v3.metadata.json").build()`.

## License

Apache-2.0.
