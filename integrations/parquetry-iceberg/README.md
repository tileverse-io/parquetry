# parquetry-iceberg

A clean-room Apache Iceberg table reader on the parquetry dataset/catalog API. It resolves a table from its metadata,
follows the pinned snapshot through the manifest list and manifests, and reads the data files as one queryable dataset.
No dependency on `iceberg-core`, `apache-avro`, or Hadoop: Iceberg's Avro metadata is read through the clean-room
`parquetry-avro` reader and the data files through the core Parquet engine. It plugs into the same `DatasetCatalog` ->
`ParquetDataset` API the pure-parquet and GeoTools paths use, and an Iceberg table reads like any other dataset.

It reads **v1, v2, and v3** data files for copy-on-write tables, where an update rewrites the affected data file and the
data files alone represent the table at a snapshot. Merge-on-read deletes (v2 delete files, v3 deletion vectors) are not
yet applied; a snapshot that references them fails fast rather than returning deleted rows. The table below tracks what
reads today and what is next.

## Capabilities

`Full` = implemented. `Partial` = works with the documented limit. `Planned` = not yet; the reader fails fast with a
clear message rather than returning wrong rows. The `Spec` column notes the Iceberg format version a feature belongs to
(`v1+` = since v1).

### Table resolution and I/O

| Feature | Spec | Status | Notes |
| --- | --- | --- | --- |
| Metadata resolution | v1+ | Full | explicit `metadataLocation`, else `version-hint.text`, else highest `vN.metadata.json` |
| Snapshot selection | v1+ | Full | current snapshot, or a pinned `snapshotId` |
| Manifest list + manifests | v1+ | Full | clean-room Avro reader |
| Data-file read, all format versions | v1-v3 | Full | data files only (copy-on-write); merge-on-read deletes not applied |
| Local + object-storage I/O | - | Full | `LocalIcebergFileIO`; `StorageIcebergFileIO` over tileverse-storage (S3, Azure, GCS, HTTP) |
| REST catalog, multiple tables | - | Planned | the [REST Catalog spec](https://iceberg.apache.org/rest-catalog-spec/) read slice: loadTable, namespace/table listing, OAuth2, vended credentials. One table per catalog today; metadata resolved by listing or explicit location |

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
| Type promotion `decimal` widen, `date`->`timestamp` | v1+/v3 | Planned | |
| Added column of `binary` / `geometry` / `geography` | v1+/v3 | Planned | added scalar columns read as null; these fail fast |
| Name mapping for id-less files | v1+ | Partial | best-effort name fallback, not the spec's `name-mapping` document |

### Reads and pruning

| Feature | Spec | Status | Notes |
| --- | --- | --- | --- |
| Full scan and count | v1+ | Full | |
| Record-level predicate filtering | v1+ | Full | |
| Bounding-box spatial predicates | v3 | Full | evaluated record-by-record through the engine's spatial contract |
| Manifest-bound file pruning (L3) | v1+ | Full | scalar bounds (`int`/`long`/`float`/`double`/`boolean`/`date`/`string`/`uuid`) + geometry bounds (`packed_xy`, `wkb_point`) |
| Partition-value file pruning | v1+ | Full | an equality or range on an identity-partition column skips whole files before opening them |
| Manifest bounds for `timestamp`/`time`/`decimal`/`fixed`/`binary` | v1+ | Planned | a predicate on these does not prune; the file is kept and filtered |
| Row-group pruning inside a file (L4) | v1+ | Planned | file-level pruning skips whole files only |
| Dataset-level explain / analyze | - | Full | reports the file dimension: files kept/skipped, each skip reason, each kept file's row-group plan |
| Column projection on an evolved file | v1+ | Partial | an evolved file presents every table field |
| Exact engine-backed (JTS) geometry filter on a renamed column | v3 | Planned | the filter binds its column at construction; bbox predicates work across a rename |

### Deletes and partitioning

| Feature | Spec | Status | Notes |
| --- | --- | --- | --- |
| Copy-on-write tables | v1+ | Full | |
| Merge-on-read: positional + equality deletes | v2 | Planned | a snapshot referencing delete manifests fails fast |
| Merge-on-read: deletion vectors | v3 | Planned | |
| Partitioned tables | v1+ | Full | identity-partition value reconstruction, transform partitions read as-is, partition-value file pruning; `decimal`/`timestamp` partition source types fail fast |

## Spatial grading (CARTO iceberg-geo-testbed)

The CARTO [`iceberg-geo-testbed`](https://github.com/jatorre/iceberg-geo-testbed) grades geospatial Iceberg support on a
five-rung ladder: L0 cannot read, L1 full scan, L2 spatial predicates return correct rows, L3 file-level pruning from the
manifest geometry bounds, L4 row-group pruning inside a file. This module is at **L3** on the testbed's V1/V2/V3
fixtures: on `v3_geometry` (10 files) a California-window query reads 1 file and skips 9, with results identical to a
brute-force scan. L4 is the remaining rung.

## Reading a table

```java
try (IcebergCatalog catalog = IcebergCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
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

To read over object storage, supply a tileverse-storage `Storage` rooted at the table and pass the table location as the
logical root: `IcebergCatalog.open(tableLocation, StorageIcebergFileIO.over(storage, tableLocation), options)`. `over(...)`
borrows the `Storage` (the caller closes it); `owning(...)` hands its lifecycle to the catalog. Because an HTTP-served
table cannot list a directory, pin the metadata document with
`IcebergOptions.builder().metadataLocation(tableLocation + "/metadata/v3.metadata.json").build()`.

## License

Apache-2.0.
