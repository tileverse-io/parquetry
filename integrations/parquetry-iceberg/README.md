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
`v3_geometry` fixture a California-window query reads 1 of the 10 files). L4 (row-group pruning inside a file from the
manifest bounds) is next; see the roadmap.

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

## What it reads

- The filesystem table layout: `<table>/metadata/v1.metadata.json` -> the pinned snapshot -> its manifest list -> the
  data manifests -> the data files.
- Format versions 1, 2, and 3, including native V3 `geometry` data files (Parquet's `Geometry` logical type, WKB).
- Copy-on-write tables (data files only).
- Data files are read by their own schema; the dataset's schema and field ids come from the data files.
- Bounding-box spatial predicates, evaluated record-by-record against the geometry column through the engine's existing
  spatial contract.
- Manifest-bound file pruning: a query's numeric and geometry bounds are matched against each data file's recorded
  manifest bounds, and files that cannot contribute a matching row are skipped before they are read. Geometry bounds in
  both the `packed_xy` and `wkb_point` encodings are decoded; a geography bound that wraps the antimeridian is kept
  conservatively. Pruning never changes results - a kept file is still filtered at row-group and record level.
- Byte access through an `IcebergFileIO`; the bundled `LocalIcebergFileIO` serves a local table directory, which is what
  the tests and a local lakehouse use.

## What it does not do yet

Where a feature is not implemented, the reader fails fast with a clear message rather than returning wrong rows.

- **Row-group pruning inside a file from manifest bounds (L4).** File-level pruning skips whole files; the manifest
  bounds are not yet pushed down to skip individual row groups within a kept file. A multi-file explain plan that names
  the skipped files is also future work.
- **Field-id-resolved projection and schema evolution.** Columns are matched by name, which is correct when a table's
  files match its schema. Reading across a rename, an added or dropped column, or a promoted type by Iceberg field id is
  not yet implemented.
- **Partitioned tables.** A non-empty partition spec fails fast. Identity-partition value reconstruction is not yet
  implemented.
- **Merge-on-read.** A snapshot that references delete manifests (positional or equality deletes, deletion vectors)
  fails fast; copy-on-write tables read correctly.
- **Cloud storage.** Only local files are served today. S3, Azure, GCS, and HTTP through `tileverse-storage` are a
  planned `IcebergFileIO`.
- **Catalog services and version resolution.** Only a direct `v1.metadata.json` is read; `version-hint.text`,
  highest-version metadata selection, and REST or other catalog services are not yet wired. One table is exposed per
  catalog.

## License

Apache-2.0.
