# parquetry-geo-jts

JTS-backed geometry materialization for parquetry. Decodes GeoParquet WKB columns into `org.locationtech.jts.geom.Geometry` instances.

## What it does

- Provides `JtsMaterializer` - a `Materializer<Map<ColumnPath, Object>>` that plugs into `ParquetDataset.read(predicate, projection, materializer, options)`.
- Scans the projected schema at construction; identifies leaves tagged with `LogicalType.Geometry` or `LogicalType.Geography`; rewrites their WKB `ByteBuffer` values to JTS `Geometry` on the way out. Everything else passes through unchanged.
- Stamps an EPSG SRID on each decoded `Geometry` when the leaf's typed `CoordinateReferenceSystem` carries an EPSG `Identifier`. Columns whose CRS is `Optional.empty()` (spec default OGC:CRS84) or whose `Identifier` is not EPSG keep JTS's default SRID (0); consumers that need a specific SRID for those cases set it themselves.
- Works uniformly across GeoParquet 1.x and 2.0: parquetry-format's `SchemaBuilder` folds the `"geo"` JSON into the schema at footer-read time on 1.x files (synthesizing the Geometry / Geography logical type on matching WKB leaves), so this module never needs to look at the `"geo"` JSON itself.

## Where it fits

```
                    ParquetDataset.open(reader)
                          |
                          v
                       ParquetSchema  <-- bridge has annotated geo leaves
                          |
                          v
        ParquetDataset.read(predicate, projection, JtsMaterializer, opts)
                          |
                          v
                  Stream<Map<ColumnPath, Object>>
                  with JTS Geometry at geo column paths
```

The GeoTools adapter session (parallel work stream) consumes this module: it wraps each per-row map in a `SimpleFeature`.

## Public API

```java
import io.tileverse.parquetry.geo.jts.JtsMaterializer;

ParquetDataset dataset = ParquetDataset.open(reader);
JtsMaterializer materializer = new JtsMaterializer(dataset.schema());
// materializer.geometryColumns() lists the column paths that will hold JTS Geometry instances.

try (Stream<Map<ColumnPath, Object>> rows =
        dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, materializer, ReadOptions.DEFAULTS)) {
    rows.forEach(row -> {
        Geometry geom = (Geometry) row.get(ColumnPath.of("geometry"));
        ...
    });
}
```

`JtsMaterializer` is thread-confined (one `WKBReader` instance per materializer). Pass one materializer per reader thread.

## Out of scope

- The GeoParquet 2.0 `_encoding_native` shape (group with `x` / `y` coordinate fields, no WKB). Today's materializer only decodes WKB-encoded columns.
- Non-EPSG SRID derivation. Only EPSG `Identifier` entries on the typed CRS map to SRIDs; PROJ4 / WKT / other authorities are out of scope. The full typed CRS is still available on `LogicalType.Geometry.crs()` / `LogicalType.Geography.crs()` for consumers that need to reach further.

## Dependencies

- `parquetry-core` (the `Materializer` SPI and the `ParquetDataset` entry point).
- `parquetry-format` (transitively via core; supplies the schema model and the typed CRS surface).
- `org.locationtech.jts:jts-core:1.20.0` (managed via the `jts.version` property in the parent POM).
