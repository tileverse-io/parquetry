# parquetry-tileverse-storage

Adapts [tileverse-storage](https://tileverse.io/storage/) to parquetry's read SPIs. Lets parquetry read Parquet from S3, Azure, GCS, HTTP, or local files through a tileverse `RangeReader`, and lets a co-resident reader share one buffer pool with parquetry.

`parquetry-core` itself has zero `io.tileverse.*` runtime dependencies; it reads through the pure-JDK `ByteRangeSource` and `SegmentPool` SPIs in `parquetry-io`. This module is the opt-in bridge back to tileverse-storage and is the promoted path for cloud reads.

## What it does

- **`ByteRangeSources.from(RangeReader)`** wraps a tileverse `RangeReader` as a parquetry `ByteRangeSource`. The adapter reads positionally and loops over `readRange` to fully satisfy in-bounds reads (a cloud backend may return a short count for a single range request).
- **`SegmentPools.backedBy(ByteBufferPool)`** exposes a tileverse `ByteBufferPool` as a parquetry `SegmentPool`. Each borrow takes a direct buffer from the pool and views it as a `MemorySegment`; closing the handle returns the buffer to the pool.

### Ownership

`ByteRangeSources.from(reader)` **borrows** the reader: the returned source's `close()` is a no-op, and the caller still closes the `RangeReader` after the last read. This mirrors `ByteRangeSource.ofChannel(...)` and matches tileverse's model, where a `RangeReader` is typically shared and cached across reads. A `ParquetDataset` never owns its source either.

### One pool for two readers

On a single GeoServer instance the PMTiles `DataStore` (tileverse-storage, Java 17) leans on a `ByteBufferPool`. Pointing `ReadOptions.segmentPool(...)` at a `SegmentPool` built over that **same** `ByteBufferPool` means one physical pool serves both PMTiles and parquetry, instead of two pools competing for the same heap and native budget. Without this module, parquetry uses its own JDK `SegmentPool.getDefault()`.

## Where it fits

```
  tileverse-storage RangeReader        (S3 / Azure / GCS / HTTP / local;
            |                            caching + block-alignment decorators compose here)
            |  ByteRangeSources.from(reader)   -- borrows the reader
            v
       ByteRangeSource                  (parquetry-io SPI)
            |
            v
   ParquetDataset.open(source) -> read(predicate, projection, options)
                                         (pure-JDK parquetry-core)

  tileverse ByteBufferPool
            |  SegmentPools.backedBy(pool)
            v
       SegmentPool  -> ReadOptions.builder().segmentPool(...)
```

## Public API

```java
import io.tileverse.parquetry.tileverse.ByteRangeSources;
import io.tileverse.parquetry.tileverse.SegmentPools;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.io.ByteBufferPool;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

// A tileverse-storage RangeReader over S3 (compose caching / block-alignment decorators on it as needed):
try (Storage storage = StorageFactory.open(URI.create("s3://bucket/"));
        RangeReader reader = storage.openRangeReader("data.parquet");
        ByteRangeSource source = ByteRangeSources.from(reader)) {

    ParquetDataset dataset = ParquetDataset.open(source);
    try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
        rows.forEach(...);
    }
}
// The source borrows the reader; the try-with-resources closes the reader (and storage) after the read.
```

Share one pool with a co-resident reader:

```java
ByteBufferPool shared = ByteBufferPool.getDefault(); // the same instance the other reader uses
ReadOptions options = ReadOptions.builder()
        .segmentPool(SegmentPools.backedBy(shared))
        .build();
```

`ByteRangeSources.from(reader)` throws `IllegalStateException` when the `RangeReader` cannot report its size (parquetry needs a known length to locate the footer).

## Out of scope

- **Caching and block-alignment.** Compose tileverse's `CachingRangeReader` / `BlockAlignedRangeReader` / `DiskCachingRangeReader` decorators on the `RangeReader` before wrapping it. This module does not add a caching `ByteRangeSource`.
- **A native shared pool.** A future tileverse multi-release jar could expose a `MemorySegment`-native pool from its Java 17 codebase, letting Java 25 parquetry and Java 17 PMTiles share one pool instance natively rather than through this `ByteBuffer`-view adapter. The runtime adapter covers the shared-pool need today.

## Dependencies

- `parquetry-io` (compile) - the `ByteRangeSource` / `SegmentPool` SPIs this module implements.
- `tileverse-storage-all` (compile) - `RangeReader` and `ByteBufferPool` plus every storage provider, pulling in `tileverse-storage-core` (local + HTTP) and the `tileverse-storage-s3` / `-azure` / `-gcs` provider modules. A consumer adding this module gets cloud reads without choosing a provider artifact themselves; for a leaner footprint, depend on `parquetry-io` and a single provider directly. Versions come from the imported `io.tileverse:tileverse-bom`.
- Test scope: `parquetry-core` (the `ParquetDataset` entry point), `parquetry-testkit` (bundled corpora), and TestContainers/LocalStack for the `CloudStorageIT` end-to-end check (the S3 provider arrives via `tileverse-storage-all`).

## Maven

```xml
<dependency>
  <groupId>io.tileverse.parquetry</groupId>
  <artifactId>parquetry-tileverse-storage</artifactId>
</dependency>
```
