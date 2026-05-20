# parquetry-format

The whole format and schema layer of parquetry: hand-rolled Java bindings for the Apache Parquet on-disk format, plus the typed schema model and the typed GeoParquet / PROJJSON ADTs that sit immediately above the wire records. The only place in parquetry that touches Parquet's Thrift Compact Protocol bytes; every other module consumes the immutable records this module produces.

## What it does

- Decodes Parquet's `FileMetaData`, `RowGroup`, `ColumnChunk`, `PageHeader`, `ColumnIndex`, `OffsetIndex`, `Statistics`, `GeospatialStatistics`, and per-leaf logical types from a stream of Thrift-compact bytes.
- Surfaces them as Java 25 records that mirror [`parquet.thrift`](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift) field for field, in `io.tileverse.parquetry.format.*`.
- Builds a sealed-record schema tree (`Field`, `ParquetSchema`, `ColumnPath`, `SchemaBuilder`) from the flat `SchemaElement` stream, in `io.tileverse.parquetry.schema.*`.
- Models the typed PROJJSON CRS ADT (`CoordinateReferenceSystem` sealed interface with `GeographicCRS`, `GeodeticCRS`, `ProjectedCRS`, `CompoundCRS`, `BoundCRS`, `VerticalCRS`, `Transformation`, plus an `Unknown` forward-compat permit) in `io.tileverse.parquetry.schema.geo.projjson.*`.
- Models the typed GeoParquet `"geo"` key-value metadata (`GeoParquetMetadata` sealed interface with `V1_0` / `V1_1` / `V2` permits, plus `GeoColumn`, `Covering`, `BboxCovering`) in `io.tileverse.parquetry.schema.geo.geoparquet.*`.

## Layout

Two top-level packages reflect a clean wire-vs-model split:

| Package | Holds | Rule |
|---|---|---|
| `io.tileverse.parquetry.format.*` | Thrift wire records + codec | "if it's in `parquet.thrift`, it's here" |
| `io.tileverse.parquetry.schema.*` | Typed Java API surface | "if it's a model we built on top, it's here" |

The one intentional `format -> schema` reference: `LogicalType.Geometry.crs()` returns a `CoordinateReferenceSystem` from `schema.geo.projjson`. That's the one Thrift record where parquetry eagerly parses the wire string into a typed value at footer-read time.

## Where it fits

```
                  parquet bytes
                       |
                       v
       +----------------------------------+
       | parquetry-format                 |  <- you are here
       | ParquetFormat (RangeReader API)  |
       | ParquetFormatDeserializer        |
       | + typed schema model             |
       | + typed PROJJSON / GeoParquet    |
       +----------------------------------+
                       |
                       v immutable records
                  parquetry-core / -geo-jts / -encryption / -variant
```

## Public API

Two decoder facades, both in `io.tileverse.parquetry.format`:

- `ParquetFormat` - high-level. Methods take an `io.tileverse.storage.RangeReader` and return decoded records (`readFooter`, `readPageHeader`, `readColumnIndex`, `readOffsetIndex`). All exceptions are runtime: `ParquetFormatException` for spec violations, `UncheckedIOException` for transport failures.
- `ParquetFormatDeserializer` (in subpackage `format.codec`) - low-level. Same operations against a raw `InputStream` of Thrift-compact bytes, for callers that have already pulled the bytes off a custom transport.

Everything else in `format.codec` is package-private mechanics (the Thrift Compact Protocol reader, the per-struct deserializers).

Above the wire layer, the schema and geo packages are direct value types - consumers use them as plain records and call `GeoParquetMetadata.parse(geoJson)` (or wire `ProjJsonModule` + `GeoParquetModule` into their own Jackson mapper) to parse JSON-encoded geo metadata.

Neither `ProjJsonModule` nor `GeoParquetModule` ships a `META-INF/services` descriptor: they are not auto-registered. Callers that want them must register the modules by hand. This avoids re-interpreting unrelated PROJJSON or `"geo"`-keyed JSON inside any consumer mapper that calls `findAndRegisterModules()`.

## Dependencies

Compile:
- `io.tileverse.storage:tileverse-storage-core` (for `RangeReader`).
- `tools.jackson.core:jackson-databind` (Jackson 3) for the typed PROJJSON and GeoParquetMetadata deserializers.

Test: JUnit 5 + AssertJ; the OGC `geoparquet` test corpus's PROJJSON fixtures (committed under `src/test/resources/`).

No `libthrift`, `parquet-*`, `hadoop-*`, or `avro` at compile or runtime - that's the whole point of this module.
