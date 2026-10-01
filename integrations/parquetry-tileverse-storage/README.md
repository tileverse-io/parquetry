# parquetry-tileverse-storage

The tileverse-storage side of parquetry's dataset access: discovers Parquet files in a tileverse `Storage` (S3, Azure, GCS, HTTP, or local), opens that Storage, and registers parquetry's buffer pool as the process-wide `ByteBufferPool` for every tileverse reader.

Reading one object through a tileverse `RangeReader` needs none of this module: `parquetry-io` adapts a `RangeReader` directly with `ByteRangeSource.of(reader)` (borrowed) or `ByteRangeSource.owning(reader)` (closed with the source). What this module adds is the provider modules for S3, Azure and GCS, and the listing and opening of whole datasets.

## What it does

- **`ParquetFileSources.open(URI, glob, properties)`** yields a `FileSource` for a dataset container, routing local directories to parquetry's own filesystem source and remote URIs to **`StorageFileSource`**, which lists the blobs matching a shell-style glob and opens each through the Storage's range reader. `StorageFileSource.object(...)` opens one known key without ever listing, for credentials that may GET but not LIST.
- **`ParquetStorage.open(URI, properties)`** opens a tileverse `Storage` from the given `storage.*` properties; parquetry adds no defaults of its own.
- **`ParquetryByteBufferPool`** is a `ByteBufferPool` provider, discovered through `ServiceLoader`, that serves tileverse's direct borrows from parquetry's `SegmentPool` under parquetry's fetch budget. On a GeoServer instance running parquetry next to a PMTiles or COG store, both then draw from one off-heap pool instead of two.

## Where it fits

```
  Storage (S3 / Azure / GCS / HTTP / local)
      |  StorageFileSource.list(glob) -> FileEntry.open()
      |                                   = ByteRangeSource.of(storage.openRangeReader(key))
      v
  FileSource -> FilesetCatalog / ParquetDataset      (parquetry-catalog)
      |
      v
  ParquetSource.open(source) -> read(predicate, projection, options)   (parquetry-core)

  tileverse ByteBufferPool.getDefault()
      |  ServiceLoader
      v
  ParquetryByteBufferPool -> SegmentPool + FetchBudget of the default ParquetRuntime
```

## Public API

```java
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.tileverse.ParquetStorage;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;

// One object, through a Storage opened for the given properties:
try (Storage storage = ParquetStorage.open(URI.create("s3://bucket/"), properties);
        RangeReader reader = storage.openRangeReader("data.parquet");
        ByteRangeSource source = ByteRangeSource.of(reader)) {

    ParquetSource dataset = ParquetSource.open(source);
    try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
        rows.forEach(...);
    }
}
// ByteRangeSource.of borrows the reader; the try-with-resources closes the reader and the storage after the read.
```

A whole dataset goes through `ParquetFileSources.open(containerUri, "**/*.parquet", properties)` and `FilesetCatalog`; see the `parquetry-catalog` README.

## Out of scope

- **Caching and block alignment.** Compose tileverse's `CachingRangeReader` and `BlockAlignedRangeReader` decorators on the `RangeReader` before wrapping it. This module adds no caching `ByteRangeSource`.

## Dependencies

- `parquetry-io`, `parquetry-core` and `parquetry-catalog` (compile): the `FileSource` and `ByteRangeSource` SPIs, the runtime pool and budget served by the provider, and the catalog fed by the file sources.
- `tileverse-storage-all` (compile): every storage provider, pulling in `tileverse-storage-core` (local + HTTP) and the `tileverse-storage-s3` / `-azure` / `-gcs` provider modules. A consumer adding this module gets cloud reads without choosing a provider artifact; a consumer that reads through its own `RangeReader` needs only `parquetry-io` and a single provider. Versions come from the `io.tileverse:tileverse-bom` imported by `parquetry-dependencies`.
- Test scope: `parquetry-testkit` (bundled corpora) and TestContainers/LocalStack for the `CloudStorageIT` end-to-end check.

## Maven

```xml
<dependency>
  <groupId>io.tileverse.parquetry</groupId>
  <artifactId>parquetry-tileverse-storage</artifactId>
</dependency>
```
