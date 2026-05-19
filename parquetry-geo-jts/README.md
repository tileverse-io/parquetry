# parquetry-geo-jts

JTS-backed geometry materialization for parquetry. Decodes GeoParquet WKB columns into `org.locationtech.jts.geom.Geometry` instances.

## What it does

- Provides `JtsMaterializer` - a `Materializer<Map<ColumnPath, Object>>` that plugs into `ParquetDataset.read(predicate, projection, materializer, options)`.
- Scans the projected schema at construction; identifies leaves tagged with `LogicalType.Geometry` or `LogicalType.Geography`; rewrites their WKB `ByteBuffer` values to JTS `Geometry` on the way out. Everything else passes through unchanged.
- Works uniformly across GeoParquet 1.x and 2.0: parquetry-core's `GeoMetadataBridge` synthesizes the Geometry / Geography logical type on 1.x files at `ParquetDataset.open()` time, so this module never needs to look at the `"geo"` JSON itself.

## Where it fits

```
                    ParquetDataset.open(reader)
                          |
                          v
                       ParquetSchema  <-- GeoMetadataBridge has annotated geo leaves
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
- ProjJSON parsing. `LogicalType.Geometry.crs()` returns the raw PROJJSON JSON string; consumers (e.g. GeoTools) own the conversion to a `CoordinateReferenceSystem`.

## Dependencies

- `parquetry-core` (the `Materializer` SPI and the schema model).
- `org.locationtech.jts:jts-core:1.20.0` (managed via the `jts.version` property in the parent POM).
