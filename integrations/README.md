# integrations

Ecosystem adapters: the parquetry engine wired into other libraries and runtimes. Each module is a published library
under the flat `io.tileverse.parquetry` groupId (the directory grouping is independent of the Maven coordinates).
Parent POM: `parquetry-integrations` (published).

The table lists each module's direct parquetry dependencies only (compile scope, no transitive). Every module also
imports the `parquetry-dependencies` BOM and test-depends on `parquetry-testkit`; those are omitted below.

| Module | Purpose | Direct parquetry dependencies |
|---|---|---|
| `parquetry-arrow` | Encode a record-batch stream as the Apache Arrow IPC streaming format | `parquetry-core` |
| `parquetry-avro` | Clean-room Avro Object Container File reader and writer | `parquetry-io`, `parquetry-compression` |
| `parquetry-jackson` | JSON encoder and materializers for nested and Variant values | `parquetry-core` |
| `parquetry-tileverse-storage` | Adapt parquetry's `ByteRangeSource` / `SegmentPool` SPIs onto tileverse-storage (S3 / Azure / GCS / HTTP) | `parquetry-io`, `parquetry-core`, `parquetry-catalog` |
| `parquetry-geotools` | A GeoTools `DataStore` reading GeoParquet | `parquetry-core`, `parquetry-catalog`, `parquetry-tileverse-storage` |
| `parquetry-geoserver` | A GeoServer community plugin for the GeoParquet store | `parquetry-geotools` |
| `parquetry-iceberg` | Clean-room read-only Apache Iceberg reader | `parquetry-core`, `parquetry-catalog`, `parquetry-avro` |
