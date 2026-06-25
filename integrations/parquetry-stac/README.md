# parquetry-stac

A SpatioTemporal Asset Catalog (STAC) reader on the parquetry dataset/catalog API. It walks a STAC catalog, exposes each
collection as one queryable dataset over the collection's GeoParquet parts, and prunes whole files by item bounding box
before any footer opens. It plugs into the same `DatasetCatalog` -> `ParquetDataset` API the pure-parquet, Iceberg, and GeoTools
paths use, and a STAC collection reads like any other dataset. No dependency on a GeoTools or GeoServer API; the GeoTools
`StacDataStoreFactory` that turns a catalog into a multi-feature-type store lives in `parquetry-geotools`.

The module is split in two packages. `io.tileverse.stac` is backend-neutral: it models the catalog, collections, items,
assets, and extents as records and defines the `StacCatalogReader` enumeration SPI, with zero `io.tileverse.parquetry.*`
dependency (a test pins that wall, keeping the model portable to a future standalone library). `io.tileverse.parquetry.stac`
is the Parquet binding: it reads a collection's parts, builds per-item `FileStats` from the item bbox, and prunes.

## Capabilities

`Full` = implemented. `Partial` = works with the documented limit. `Planned` = not yet.

### Catalog sources

| Feature | Status | Notes |
| --- | --- | --- |
| Static JSON catalog tree | Full | `JsonStacReader` walks catalog -> collection -> item documents, lazily, over tileverse-storage |
| stac-geoparquet item-table as an index | Partial | `GeoParquetStacReader` maps item-table rows to the same model; flat bbox + `asset_href` columns, the nested bbox struct and assets map are Planned; reachable programmatically, not yet wired into the GeoTools factory |
| STAC API (HTTP) reader | Planned | only the static tree and the item-table index ship today |
| Non-data links retained | Full | every link is kept whatever its rel (for example `pmtiles`); a consumer can walk the tree to a theme-level link |

### I/O and storage

| Feature | Status | Notes |
| --- | --- | --- |
| Local + object-storage I/O | Full | reads through tileverse-storage (S3, Azure, GCS, HTTP) over a `Storage` rooted at the catalog |
| Source ownership | Full | the catalog opens each part's byte source once and owns it; a per-query dataset borrows the survivor subset and never closes the shared sources; `close()` releases every source and the Storage |

### Reads and pruning

| Feature | Status | Notes |
| --- | --- | --- |
| One dataset per collection | Full | the collection id is the dataset and feature-type name |
| Full scan, count, record-level filtering | Full | each survivor is filtered at row-group and record level during the read |
| Whole-file pruning by item bbox | Full | an item whose 2D bbox is disjoint from a spatial query is skipped before its footer opens, reusing the core `FilePruner` STATS tier; the result is identical to scanning every part |
| 3D item bbox | Full | a `[minx,miny,minz,maxx,maxy,maxz]` bbox keeps the XY rectangle for pruning and reports Z |
| Missing or unrecognized bbox | Full | the part is always a pruning survivor (best-effort: pruning never drops a part that could match) |
| Collection with no GeoParquet parts | Full | skipped, not exposed as a dataset; a pmtiles-only or empty collection does not fail the catalog open |
| Dataset-level explain / analyze | Full | reports files kept/skipped, each skip reason, and each kept file's row-group plan |
| Datetime pre-pruning | Planned | needs a STAC-datetime to parquet-column mapping STAC does not standardize |

## Reading a collection

```java
URI catalog = Path.of("/data/overture/catalog.json").toUri();
try (Storage storage = StorageFactory.open(catalog.resolve("."));
        StacDatasetCatalog stac =
                StacDatasetCatalog.open(catalog, storage, new JsonStacReader(), StacCatalogOptions.defaults())) {

    ParquetDataset buildings = stac.dataset("building");

    Bbox window = Bbox.of2d(-125.0, 32.0, -115.0, 42.0);
    Predicate inWindow = new Predicate.Spatial.BboxIntersects(ColumnPath.of("geometry"), window);
    try (Stream<ParquetRecord> rows = buildings.read(inWindow, Projection.ALL, ReadOptions.DEFAULTS)) {
        rows.forEach(this::handle);
    }
}
```

`StacCatalogOptions` selects which asset media types count as GeoParquet data (default: the parquet media types plus a
`.parquet` href fallback) and names the primary geometry column the item bbox is attached to. Through GeoTools, the
`StacDataStoreFactory` in `parquetry-geotools` opens the catalog as a `DataStore` whose every collection is a feature type.

## Deferred

- An item-table whose rows are feature data, not an index of external parts.
- Wiring the item-table index reader into the GeoTools factory (a reader-selection parameter).
- Extraction of `io.tileverse.stac` to its own standalone library (the package wall keeps this open).
- A shared prunable-fileset dataset base across the Iceberg, Fileset, and STAC bindings.
- A live-Overture opt-in integration test against a public STAC endpoint (excluded from the default build).

## License

Apache-2.0.
