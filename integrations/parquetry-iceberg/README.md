# parquetry-iceberg

A clean-room Apache Iceberg table reader built on the parquetry dataset/catalog API. It resolves a table from its
metadata, follows the pinned snapshot through the manifest list and manifests, and reads the data files as one queryable
dataset. It has no dependency on `iceberg-core`, `apache-avro`, or Hadoop: Iceberg metadata is Avro, read through the
clean-room `parquetry-avro` reader, and the data files are read through the core Parquet engine.

## Why it exists

Reading Iceberg on the JVM normally pulls `iceberg-core` and its transitive baggage (Avro, several Commons jars,
Hadoop). This module replaces the read slice parquetry needs - resolve a snapshot, list its data files, read them - with
focused code over the engine that is already present. It plugs into the same `DatasetCatalog` -> `Dataset` API the
pure-parquet and GeoTools paths use, and an Iceberg table is read the same way as any other dataset.

## Where this stands

The CARTO [`iceberg-geo-testbed`](https://github.com/jatorre/iceberg-geo-testbed) grades geospatial Iceberg support on a
five-rung ladder: L0 cannot read, L1 full scan works, L2 spatial predicates return correct rows, L3 file-level pruning
fires from the manifest geometry bounds, L4 row-group pruning inside a file. This module reads the testbed's V1/V2/V3
fixtures at **L3** today: full scans return every row, a bounding-box predicate over a native V3 `geometry` column
returns the correct rows, and a regional query skips the data files whose manifest bounds fall outside it (on the
`v3_geometry` fixture a California-window query reads 1 of the 10 files). The same read path serves a table over
`tileverse-storage` (S3, Azure Blob, GCS, HTTP), not only local files, and resolves the table's current metadata
document automatically.

Field-id-resolved reads (schema evolution by Iceberg field id) now work. The next work is read coverage rather than
another pruning rung: partitioned tables and merge-on-read deletes. L4 (row-group pruning inside a file from the manifest
bounds) is a later refinement. See "What it does not do yet".

## Reading a table

```java
try (IcebergCatalog catalog = IcebergCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
    Dataset table = catalog.dataset(catalog.datasets().get(0));

    long total = table.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);

    Bbox window = Bbox.of2d(-125.0, 32.0, -115.0, 42.0);
    Predicate inWindow = new Predicate.Spatial.BboxIntersects(ColumnPath.of("geom"), window);
    try (Stream<ParquetRecord> rows = table.read(inWindow, Projection.ALL, ReadOptions.DEFAULTS)) {
        rows.forEach(this::handle);
    }
}
```

`IcebergOptions.builder().snapshotId(id).build()` pins a specific snapshot; the default pins the table's current
snapshot. The catalog owns the byte sources it opens; `close()` releases them.

To read a table over object storage, supply a `tileverse-storage` `Storage` rooted at the table and pass the table's
location as the logical root:

```java
try (IcebergCatalog catalog = IcebergCatalog.open(
        tableLocation, StorageIcebergFileIO.over(storage, tableLocation), IcebergOptions.defaults())) {
    Dataset table = catalog.dataset(catalog.datasets().get(0));
    // ... read as above
}
```

`StorageIcebergFileIO.over(storage, ...)` borrows the `Storage` (the caller closes it); `owning(storage, ...)` hands its
lifecycle to the catalog. For an HTTP-served table, which cannot list a directory, pin the metadata document:
`IcebergOptions.builder().metadataLocation(tableLocation + "/metadata/v3.metadata.json").build()`.

## What it reads

- The table layout: `<table>/metadata/<version>.metadata.json` -> the pinned snapshot -> its manifest list -> the data
  manifests -> the data files.
- Format versions 1, 2, and 3, including native V3 `geometry` data files (Parquet's `Geometry` logical type, WKB).
- Copy-on-write tables (data files only).
- The table's current schema, presented from the metadata. Each data file's columns are matched to it by Iceberg field
  id, keeping a read correct across a rename, an added column (read as null), a dropped column, a reordered column, or an
  `int`-to-`long` promotion. A file whose schema already matches the table reads untouched (the common case); a file
  without field ids falls back to name matching.
- Bounding-box spatial predicates, evaluated record-by-record against the geometry column through the engine's existing
  spatial contract.
- Manifest-bound file pruning: a query's scalar and geometry bounds are matched against each data file's recorded
  manifest bounds, and files that cannot contribute a matching row are skipped before they are read. Scalar bounds
  decode for `int`, `long`, `float`, `double`, `boolean`, `date`, `string`, and `uuid` columns; geometry bounds decode
  in both the `packed_xy` and `wkb_point` encodings. A geography bound that wraps the antimeridian is kept
  conservatively. Pruning never changes results - a kept file is still filtered at row-group and record level.
- Dataset-level explain and analyze that report the file dimension: how many data files a query keeps and skips, with
  each skipped file's elimination reason and each kept file's row-group plan.
- The current-snapshot metadata document, resolved in order: an explicit `IcebergOptions.metadataLocation`, else
  `metadata/version-hint.text`, else the highest `vN.metadata.json` found by listing `metadata/`.
- Byte access through an `IcebergFileIO`: `LocalIcebergFileIO` serves a local table directory, and `StorageIcebergFileIO`
  serves a table over a `tileverse-storage` `Storage` - S3, Azure Blob, GCS, or HTTP. The catalog either borrows a
  caller-owned `Storage` or takes ownership of one it is given. A backend that cannot list a directory (HTTP) needs the
  explicit `metadataLocation`, since version resolution by listing is unavailable there.

## What it does not do yet

Where a feature is not implemented, the reader fails fast with a clear message rather than returning wrong rows.

- **Nested field-id reconciliation and some promotions.** Top-level columns reconcile by Iceberg field id. A nested
  struct, list, or map column is read by name, not reconciled by field id. The sanctioned type promotions are
  `int`-to-`long` and `float`-to-`double`; `decimal` widening and `date`-to-`timestamp` are not yet wired. An added
  column reads as null for `int`, `long`, `float`, `double`, `boolean`, `date`, and `string`; an added `binary`,
  `geometry`, or `geography` column fails fast. An exact engine-backed geometry filter (the JTS `GeometryFilter` SPI)
  pushed against a column that the table renamed is not yet supported, since the filter binds to its column at
  construction; the engine-free bounding-box predicates work across a rename. Restricting an evolved-file read to a
  caller's column projection is a later refinement; an evolved file presents every table field, and a column promoted
  `int`-to-`long` does not prune by manifest bounds (the recorded bound predates the promotion) and is kept and filtered
  row by row.
- **Partitioned tables.** A non-empty partition spec fails fast. Identity-partition value reconstruction is not yet
  implemented.
- **Merge-on-read.** A snapshot that references delete manifests (positional or equality deletes, deletion vectors)
  fails fast; copy-on-write tables read correctly.
- **Manifest bounds for some column types.** Bounds decode for the scalar and geometry types listed above. A bound on a
  `timestamp`, `time`, `decimal`, `fixed`, or `binary` column is not yet decoded. A predicate on such a column does not
  prune files; the file is kept and filtered row by row, which is correct but reads more than strictly necessary.
- **Row-group pruning inside a file from manifest bounds (L4).** File-level pruning skips whole files; the manifest
  bounds are not yet pushed down to skip individual row groups within a kept file.
- **Catalog services and multiple tables.** The current metadata document is resolved by listing or an explicit
  location, but REST or other catalog services are not wired, and one table is exposed per catalog.

## License

Apache-2.0.
