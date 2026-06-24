# parquetry-geotools

A read-only GeoTools `DataStore` for GeoParquet, backed by the parquetry dataset
catalog. It serves a GeoParquet file (local, S3, Azure, GCS, or HTTP) as GeoTools
`SimpleFeature`s, pushing spatial and attribute filtering, column projection, a
feature cap, and counting down into the parquetry read path.

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

| Param       | Required | Meaning |
|-------------|----------|---------|
| `filetype`  | yes      | Must be `geoparquet` |
| `uri`       | yes      | URI of a GeoParquet file (local path, `s3://`, `gs://`, `https://`, ... per the tileverse storage backends) |
| `namespace` | no       | Feature type namespace |
| `fid`       | no       | Column to use as the feature id (see [Feature ids](#feature-ids)) |

```java
Map<String, Object> params = Map.of("filetype", "geoparquet", "uri", "s3://bucket/roads.parquet");
DataStore store = DataStoreFinder.getDataStore(params);
SimpleFeatureSource roads = store.getFeatureSource(store.getTypeNames()[0]);
```

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
        "filetype", "geoparquet",
        "uri", "s3://bucket/roads.parquet",
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
