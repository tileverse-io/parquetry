# parquetry-format

Hand-rolled Java bindings for the Apache Parquet on-disk format. The only place in parquetry that touches Parquet's Thrift Compact Protocol bytes; every other module consumes the immutable records this module produces.

## What it does

- Decodes Parquet's `FileMetaData`, `RowGroup`, `ColumnChunk`, `PageHeader`, `ColumnIndex`, `OffsetIndex`, and per-leaf logical types from a stream of Thrift-compact bytes.
- Surfaces them as Java 25 records that mirror [`parquet.thrift`](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift) field for field.
- Exposes a `LogicalType` sealed interface covering every variant of the Thrift `LogicalType` union (String, Int, Decimal, Date, Time, Timestamp, Json, Bson, Uuid, Float16, Variant, Geometry, Geography). `Geometry(crs)` and `Geography(crs, algorithm)` carry their Thrift payload directly; `VariantStub` is a placeholder pending the variant binary format implementation.

## Where it fits

```
                  parquet bytes
                       |
                       v
       +----------------------------------+
       | parquetry-format                 |  <- you are here
       | ParquetFormat (RangeReader API)  |
       | ParquetFormatDeserializer (Stream API)
       +----------------------------------+
                       |
                       v immutable records
                  parquetry-core / -geo-jts / -encryption / -variant
```

## Public API

Two facades, both in `io.tileverse.parquetry.format`:

- `ParquetFormat` - high-level. Methods take an `io.tileverse.storage.RangeReader` and return decoded records (`readFooter`, `readPageHeader`, `readColumnIndex`, `readOffsetIndex`). All exceptions are runtime: `ParquetFormatException` for spec violations, `UncheckedIOException` for transport failures.
- `ParquetFormatDeserializer` (in subpackage `format.codec`) - low-level. Same operations against a raw `InputStream` of Thrift-compact bytes, for callers that have already pulled the bytes off a custom transport.

Everything else in `format.codec` is package-private mechanics (the Thrift Compact Protocol reader, the per-struct deserializers).

## Dependencies

Compile: `io.tileverse.storage:tileverse-storage-core` (for `RangeReader`). Test: JUnit 5 + AssertJ.

No `libthrift`, `parquet-*`, `hadoop-*`, or `avro` at compile or runtime - that's the whole point of this module.
