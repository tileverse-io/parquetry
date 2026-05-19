# parquetry-core

The schema model, codecs, page decoders, filter pushdown, and row-API `ParquetDataset` facade. This is the module most consumers actually depend on.

## What it does

- Builds a sealed-record schema tree from the format-module's flat `SchemaElement` stream (`schema/`).
- Resolves data-page bytes from a `RangeReader` and threads them through per-column streaming decoders (`read/`).
- Implements the four-tier filter pushdown: stats, dictionary, column-index, record-level (`filter/`).
- Implements the Dremel walker for repeated / list / map / nested-struct columns (`read/RecordAssembler`, `read/ValueBuilder`).
- Hooks the GeoParquet 1.x metadata bridge that synthesizes `Geometry` / `Geography` logical types on WKB columns at `ParquetDataset.open()` (`dataset/GeoMetadataBridge`).
- Exposes the `Materializer<T>` SPI for callers that want a custom row shape (Apache Arrow, GeoTools features, custom records).

## Where it fits

```
        parquetry-format records                tileverse-storage RangeReader
                  \                                       /
                   v                                     v
                  +------------------------------------------+
                  | parquetry-core                           |  <- you are here
                  |   schema   filter   page   codec   read  |
                  |              dataset/ParquetDataset             |
                  +------------------------------------------+
                                    |
                  +-----------------+-----------------+
                  v                 v                 v
              row API          materializer SPI    GeoMetadataBridge
              Stream<           Stream<T> via         (1.x "geo" KV)
              ParquetRecord>    Materializer<T>            |
                                    |                      v
                                    v                  Geometry / Geography
                            parquetry-geo-jts          logical types
                            (JtsMaterializer)
```

## Public API surface

The intended entry point is `io.tileverse.parquetry.dataset.ParquetDataset`:

```java
try (Storage storage = StorageFactory.open(file.getParent().toUri());
     RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
    ParquetDataset dataset = ParquetDataset.open(reader);
    try (Stream<ParquetRecord> stream = dataset.read(
            col("year").gtEq(2020).and(col("country").eq("AR")),
            Projection.of(Set.of(ColumnPath.of("year"), ColumnPath.of("country"))),
            ReadOptions.DEFAULTS)) {
        stream.forEach(System.out::println);
    }
}
```

Eight public packages: `dataset`, `filter`, `materializer`, `page` (only `PageDecoder` + `LevelDecoder`), `read` (only `ReadOptions` + `ConcurrencyMode` + `PruningDecision` + `DecryptionKeyRetriever`), `record`, `schema`, `codec` (only `Codec` + `CodecRegistry`). Everything else is package-private.

## Dependencies

- `parquetry-format` (provides the Thrift records and the `ParquetFormat` facade).
- `io.airlift:aircompressor-v3` (Snappy / Zstd / Lz4Raw decompression + future bloom-filter xxHash64).
- `org.brotli:dec` (Brotli decompression).
- `tools.jackson.core:jackson-databind` (Jackson 3) - only for the GeoMetadataBridge's `"geo"` JSON parse.
- `io.tileverse.storage:tileverse-storage-core` (transitively via parquetry-format).

No `parquet-*`, `hadoop-*`, `libthrift`, or `avro` at compile or runtime.
