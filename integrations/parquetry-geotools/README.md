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
- Decodes WKB geometries to JTS through `parquetry-geo-jts`'s `JtsMaterializer`.
- Pushes query work down (see below) rather than scanning every row and column.

## Connection parameters

`GeoParquetDataStoreFactory` (display name **GeoParquet**) exposes:

| Param       | Required | Meaning |
|-------------|----------|---------|
| `filetype`  | yes      | Must be `geoparquet` |
| `uri`       | yes      | URI of a GeoParquet file (local path, `s3://`, `gs://`, `https://`, ... per the tileverse storage backends) |
| `namespace` | no       | Feature type namespace |

```java
Map<String, Object> params = Map.of("filetype", "geoparquet", "uri", "s3://bucket/roads.parquet");
DataStore store = DataStoreFinder.getDataStore(params);
SimpleFeatureSource roads = store.getFeatureSource(store.getTypeNames()[0]);
```

## Query pushdown

`GeoParquetFeatureSource` translates a GeoTools `Query` (`QueryTranslator`,
`FilterToPredicate`) into a parquetry `Predicate` pushed into the read, a residual
`Filter` GeoTools applies on the materialized features, and a `Projection`:

- **Spatial filters** push exactly. A `BBOX` lowers to a bounding-box
  intersection; `Intersects` / `Contains` / `Within` / `Crosses` / `Overlaps` /
  `Touches` / `Disjoint` / `Equals` / `DWithin` push as exact JTS geometry tests
  with sound bounding-box pruning (`parquetry-geo-jts`'s `JtsGeometryFilter`). A
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
  `ParquetDataset.count(predicate)` (statistics-based, no record materialization);
  a query with a residual falls back to iterating.

The pushed predicate is always a sound necessary condition of the full filter,
and the residual catches anything it over-accepts. Results therefore match an
unfiltered scan exactly.

## Where it fits

```
GeoTools Query (filter, propertyNames, maxFeatures)
        |
        v
GeoParquetFeatureSource --QueryTranslator--> (Predicate, residual Filter, Projection)
        |
        v
ParquetDatasetCatalog -> ParquetDataset.read(predicate, projection, JtsMaterializer)
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
