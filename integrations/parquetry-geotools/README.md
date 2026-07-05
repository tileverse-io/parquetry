# parquetry-geotools

A read-only GeoTools `DataStore` for GeoParquet, backed by the parquetry dataset
catalog. It serves a GeoParquet file (local, S3, Azure, GCS, or HTTP) as GeoTools
`SimpleFeature`s, pushing spatial and attribute filtering, column projection, a
feature cap, and counting down into the parquetry read path.

## Store family

This module registers three read-only DataStore factories, each selected by its
own required URI parameter:

| Store | Param key | Opens |
|-------|-----------|-------|
| **GeoParquet** | `geoparquet` | a GeoParquet file or a directory of them |
| **STAC GeoParquet** | `geoparquet-stac` | a STAC `catalog.json` or a stac-geoparquet item table (`*.parquet`), auto-detected by extension |
| **Apache Iceberg** | `iceberg` | a single Iceberg table directory or a whole warehouse root, auto-detected |

The rest of this document covers the **GeoParquet** store; the other two share
its query pushdown, cloud-storage and feature-id behavior over their own catalog
source.

## What it does

- Registers a `DataStoreFactorySpi` (`GeoParquetDataStoreFactory`,
  `DataStoreFinder`-discoverable) that opens a GeoParquet URI as a read-only
  `ContentDataStore`.
- Maps the GeoParquet schema to a `SimpleFeatureType` (`GeoParquetSchemaMapper`):
  the primary geometry column becomes the default geometry, other leaves become
  attributes, structs flatten to dotted names, and the CRS resolves EPSG-first
  from the GeoParquet metadata (`ProjJsonCrsConverter`, defaulting to
  `OGC:CRS84`).
- Decodes WKB geometries to JTS through `parquetry-core`'s `JtsMaterializer`.
- Pushes query work down (see below) rather than scanning every row and column.

## Connection parameters

`GeoParquetDataStoreFactory` (display name **GeoParquet**) exposes:

| Param        | Required | Meaning |
|--------------|----------|---------|
| `geoparquet` | yes      | URI of a GeoParquet file (local path, `s3://`, `gs://`, `https://`, ... per the tileverse storage backends). Its presence is what selects this factory. |
| `namespace`  | no       | Feature type namespace |
| `fid`       | no       | Column to use as the feature id (see [Feature ids](#feature-ids)) |
| `layer-grouping` | no  | For a directory URI: `merged` (default) reads all files as one layer (files must share a schema); `file` publishes each top-level `.parquet` file as its own layer |

```java
Map<String, Object> params = Map.of("geoparquet", "s3://bucket/roads.parquet");
DataStore store = DataStoreFinder.getDataStore(params);
SimpleFeatureSource roads = store.getFeatureSource(store.getTypeNames()[0]);
```

## Directories and layers

A directory (or glob) URI resolves through the `layer-grouping` parameter:

- `merged` (default) - every matched file, recursively, including a
  Hive-partitioned tree, is one dataset and one layer; all files must share a
  schema by equality.
- `file` - each top-level `.parquet` file in the directory is its own layer,
  named by its file name without the extension. Files need not share a schema
  (e.g. a directory of Natural Earth themes, one file per theme). The listing
  never recurses and drops any glob in the URI; a Hive-partitioned tree has no
  top-level files and fails with "no files found" - serve it with
  `layer-grouping=merged`.

A single-file URI is always exactly one layer; `layer-grouping` has no effect on it.

## Cloud storage

Reading from S3, Azure, GCS, or HTTP is configured through the tileverse
`storage.*` parameters. The provider is auto-detected from the URI scheme; set
`storage.provider` to force it. Each backend has its own key family:

| Family             | Examples |
|--------------------|----------|
| `storage.s3.*`     | `storage.s3.region`, `storage.s3.aws-access-key-id`, `storage.s3.aws-secret-access-key`, `storage.s3.anonymous` |
| `storage.azure.*`  | `storage.azure.account-key`, `storage.azure.sas-token`, `storage.azure.connection-string` |
| `storage.gcs.*`    | `storage.gcs.project-id`, `storage.gcs.default-credentials-chain` |
| `storage.http.*`   | `storage.http.username`, `storage.http.password`, `storage.http.bearer-token`, `storage.http.api-key` |
| `storage.caching.enabled` | turn the storage memory cache on or off |

```java
Map<String, Object> params = Map.of(
        "geoparquet", "s3://bucket/roads.parquet",
        "storage.s3.region", "eu-central-1");
DataStore store = DataStoreFinder.getDataStore(params);
```

Secret parameters (keys, tokens, passwords) are masked in the GeoServer store
form. See the `parquetry-geoserver` module for the provider-driven edit panel.

## Query pushdown

`GeoParquetFeatureSource` translates a GeoTools `Query` (`QueryTranslator`,
`FilterToPredicate`) into a parquetry `Predicate` pushed into the read, a residual
`Filter` GeoTools applies on the materialized features, and a `Projection`:

- **Spatial filters** push exactly. A `BBOX` lowers to a bounding-box
  intersection; `Intersects` / `Contains` / `Within` / `Crosses` / `Overlaps` /
  `Touches` / `Disjoint` / `Equals` / `DWithin` push as exact JTS geometry tests
  with sound bounding-box pruning (`parquetry-core`'s `JtsGeometryFilter`). A
  renderer's multi-bbox cross-dateline filter (`OR(BBOX, BBOX)`) pushes as a
  disjunction and prunes each box independently, never scanning the region
  between them. A spatial literal whose CRS differs from the dataset's native CRS
  is rejected.
- **Attribute filters** push comparisons, `BETWEEN`, `IS NULL`, and
  `AND` / `OR` / `NOT` where the column type supports them (`Pred`). Anything not
  pushable (`LIKE`, functions, temporal ranges) becomes the residual filter.
- **Projection** (`canRetype`): only the requested attributes plus any the
  residual filter needs are decoded; the columnar read never materializes columns
  a query does not use.
- **Feature cap** (`canLimit`): `maxFeatures` stops the lazy read early.
- **Count** (`getCount`): a fully pushable filter counts through
  `GeoParquetDataset.count(predicate)` (statistics-based, no record materialization);
  a query with a residual falls back to iterating.

The pushed predicate is always a sound necessary condition of the full filter,
and the residual catches anything it over-accepts. Results therefore match an
unfiltered scan exactly.

## Spatial decimation (ScreenMap)

At overview zoom the renderer attaches a `ScreenMap` to the query under
`Hints.SCREENMAP`: once a feature paints an output pixel, later features in that
pixel are redundant. `GeoParquetFeatureSource` reads the hint and, when the query
pushes down fully (no residual filter remains), builds a `ScreenMapReadProbe` and
attaches it to the read. The probe rides the read down three levels and drops
already-covered units by bounding box, without decoding their geometry:

- **File** - a file whose geometry bounds fall in painted pixels is never read;
  with a probe the multi-file read also visits files in spatial order, which lets
  painting accumulate coherently.
- **Row group** - a painted row group is skipped before its bytes are fetched.
- **Row** - a painted feature is dropped after the predicate test, before output.

The coarse levels (file, row group) consult the probe read-only and never paint;
only the per-row level records coverage, which keeps a painted region from
dropping the very features that should fill it. A read without the hint, or a
query that keeps a residual filter, reads with the default options unchanged.

The probe drops a redundant feature; it does not yet substitute a synthetic
pixel-sized representative for the first feature kept per cell (the renderer's own
`ScreenMap` still draws the kept feature at full resolution). Page-level skipping
and decimation counts in explain-analyze are deferred.

## Feature ids

Each feature needs an id. The store resolves it in this order:

1. The column named by the `fid` connection parameter, when set. It must be a
   non-geometry string or numeric column, or opening the store fails.
2. Otherwise a column literally named `id`, when present and usable.
3. Otherwise a synthetic per-read sequence number.

When a feature id column resolves, that column's value is the feature id, and
`Id` filters work (GeoTools applies them on the materialized features). When it
does not (case 3), the synthetic ids are not stable across reads. An `Id` filter
against them would match nothing useful, and is rejected rather than failing
silently. The feature id column is always decoded for id assignment even when a
query does not request it as an output attribute.

## Where it fits

```
GeoTools Query (filter, propertyNames, maxFeatures)
        |
        v
GeoParquetFeatureSource --QueryTranslator--> (Predicate, residual Filter, Projection)
        |
        v
FilesetCatalog (a DatasetCatalog) -> GeoParquetDataset.read(predicate, projection, JtsMaterializer)
        |
        v
Stream<SimpleFeature> -> FilteringFeatureReader(residual) -> ReTypeFeatureReader -> MaxFeatureReader
```

## Runtime requirement

`parquetry-core` is Java 25 bytecode compiled with `--enable-preview`. This module
therefore loads only on a **Java 25 JVM started with `--enable-preview`** (plus
the foreign-memory native-access flags parquetry uses). A Java 17
GeoTools / GeoServer cannot load it. The deployment target is GeoServer Cloud on
Java 25; the `parquetry-geoserver` module wires this store into GeoServer.
