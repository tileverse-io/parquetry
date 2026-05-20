# parquetry-core

Page decoders, codecs, filter pushdown, the Dremel walker, and the row-API `ParquetDataset` facade. This is the module most consumers actually depend on. The schema model and wire records live in `parquetry-format` (one JAR upstream).

## What it does

- Resolves data-page bytes from a `RangeReader` and threads them through per-column streaming decoders (`read/`).
- Implements the four-tier filter pushdown: stats, dictionary, column-index, record-level (`filter/`).
- Implements the Dremel walker for repeated / list / map / nested-struct columns (`read/RecordAssembler`, `read/ValueBuilder`).
- Exposes the `Materializer<T>` SPI for callers that want a custom row shape (Apache Arrow, GeoTools features, custom records).
- Resolves the file schema via `SchemaBuilder.build(elements, keyValueMetadata)` at `ParquetDataset.open()`. On GeoParquet 1.x files, the two-argument overload folds the `"geo"` JSON into the schema by synthesizing `Geometry` / `Geography` logical types on the matching WKB columns. Native (2.0) annotations always win.

## Where it fits

```
        parquetry-format records + schema/* + schema.geo.*    tileverse-storage RangeReader
                              \                                       /
                               v                                     v
                              +------------------------------------------+
                              | parquetry-core                           |  <- you are here
                              |   filter   page   codec   read   record  |
                              |              dataset/ParquetDataset      |
                              +------------------------------------------+
                                                |
                                       +--------+--------+
                                       v                 v
                                   row API          materializer SPI
                                   Stream<           Stream<T> via
                                   ParquetRecord>    Materializer<T>
                                                          |
                                                          v
                                                  parquetry-geo-jts
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

Public packages: `dataset`, `filter`, `materializer`, `page` (only `PageDecoder` + `LevelDecoder`), `read` (only `ReadOptions` + `ConcurrencyMode` + `PruningDecision` + `DecryptionKeyRetriever`), `record`, `codec` (only `Codec` + `CodecRegistry`). Everything else is package-private. The `schema` and `schema.geo.*` packages are public API but they ship in `parquetry-format`.

## Dependencies

- `parquetry-format` (provides the Thrift records, the `ParquetFormat` facade, the schema model, the typed PROJJSON / GeoParquetMetadata ADTs, and `SchemaBuilder` which folds `"geo"` JSON into the schema at footer-read time).
- `io.airlift:aircompressor-v3` (Snappy / Zstd / Lz4Raw decompression + bloom-filter xxHash64).
- `org.brotli:dec` (Brotli decompression).
- `io.tileverse.storage:tileverse-storage-core` (transitively via parquetry-format).

No `parquet-*`, `hadoop-*`, `libthrift`, or `avro` at compile or runtime.
