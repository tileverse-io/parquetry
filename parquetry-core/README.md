# parquetry-core

Page decoders + encoders, codecs, filter pushdown, the Dremel walker, the `ParquetFileReader` read entry, and the `ParquetFileWriter` write entry. This is the module most consumers actually depend on. The schema model and Thrift wire records live in `parquetry-format` (one JAR upstream).

## Public API

The entry points all live in `io.tileverse.parquetry.data`:

```java
// Read one local file:
try (ByteRangeSource source = ByteRangeSource.ofFile(path)) {
    ParquetFileReader reader = ParquetFileReader.open(source);
    try (Stream<ParquetRecord> stream = reader.read(
            col("year").gtEq(2020).and(col("country").eq("AR")),
            Projection.of(Set.of(ColumnPath.of("year"), ColumnPath.of("country"))),
            ReadOptions.DEFAULTS)) {
        stream.forEach(System.out::println);
    }
}

// Write one file (channel-primary):
try (FileChannel sink = FileChannel.open(out, CREATE, WRITE, TRUNCATE_EXISTING);
     ParquetFileWriter writer = ParquetFileWriter.create(sink, schema, WriteOptions.defaults())) {
    rows.forEach(writer::write);
}
```

The read source is a `ByteRangeSource` (the [`parquetry-io`](../parquetry-io/) SPI). `ofFile(Path)` / `ofChannel(FileChannel)` cover local files; `of(RangeReader)` / `owning(RangeReader)` read through any tileverse-storage `RangeReader`. The S3 / Azure / GCS readers come from tileverse's provider modules, added by the application: `tileverse-storage-all`, or `tileverse-storage-s3` / `-azure` / `-gcs` alone. Any of them reads through `ParquetFileReader.open(ByteRangeSource.of(rangeReader))`.

The write sink is a `WritableByteChannel`. It does not have to be seekable: any genuinely streaming output target (an HTTP request body, a blob-storage upload, anything that exposes `WritableByteChannel`) works -- rows are encoded into per-column temp files first and only consolidated onto the sink at row-group flush. An `OutputStream` overload is provided as a convenience and shims through `Channels.newChannel(...)`.

## Package layout

Direction rule: **cross-cutting capabilities stay at the top level; direction-specific machinery lives under `data.read` or `data.write`**. New code follows this rule too.

| Package | Role |
|---|---|
| `data` | Public entry points (`ParquetFileReader`, `ParquetFileWriter`) and their option types (`ReadOptions`, `WriteOptions` with nested `ParquetVersion`, `RowGroupSize`, `EncodingPolicy`, `BloomFilterConfig`, `GeoParquetMetadataMode`; `WriteRow`). The neutral read/write observability vocabulary (`QueryObserver`, `WriteObserver`, and the stats records) lives in `observe`. |
| `data.read` | Read internals: per-column readers, batch pipeline, level resolver, nested vector assembler. |
| `data.read.page` | Page-level read mechanics: `PageDecoder`, the per-encoding decoders, `Dictionary`, `RleDictionaryPageDecoder`, `LevelDecoder`. |
| `data.write` | Write internals: `ColumnChunkWriter`, `RowGroupWriter`, `StatisticsAccumulator`, column / offset / bloom builders, `GeoMetadataWriter`. |
| `data.write.page` | Page-level write mechanics: `PageWriter`, the `Encoder<T>` SPI + per-encoding encoders, `DictionaryAttempt`, `LevelEncoder`. |
| `batch` | `ParquetRecordBatch` + `ColumnVector` subtypes. Read fills these; write consumes them. |
| `record` | `ParquetRecord` and friends. Row-API output type. |
| `filter` | Predicate pushdown: stats / column-index / bloom / dictionary tiers, `Pred` algebra, `ExplainPlan`. |
| `materializer` | The `Materializer<T>` SPI for callers that want a custom row shape (Arrow, GeoTools features, custom records). |

## Where it fits

```
        parquetry-format records + schema/* + schema.geo.*    parquetry-io ByteRangeSource + SegmentPool
                              \                                       /
                               v                                     v
                              +------------------------------------------+
                              | parquetry-core                           |
                              |   batch  record  filter                  |
                              |   materializer                           |
                              |   data/                                  |
                              |     ParquetFileReader  ParquetFileWriter |
                              |     read/{page/}        write/{page/}    |
                              +------------------------------------------+
                                                |
                                       +--------+--------+
                                       v                 v
                                   row API          materializer SPI
                                   Stream<           Stream<T> via
                                   ParquetRecord>    Materializer<T>
                                                          |
                                                          v
                                                  io.tileverse.parquetry.geo
                                                  (JtsMaterializer)
```

## Dependencies

- `parquetry-format` (Thrift records, `ParquetFormat` facade, schema model, typed PROJJSON / GeoParquetMetadata ADTs, `SchemaBuilder` folding the `"geo"` JSON into the schema at footer-read time).
- `parquetry-io` (the `ByteRangeSource` read source and `SegmentPool` buffer-pool SPIs).
- `io.airlift:aircompressor-v3` (Snappy / Gzip / Lz4Raw / Zstd / Lzo / legacy LZ4 codecs + bloom-filter xxHash64).
- `org.brotli:dec` (Brotli decompression).

No `parquet-*`, `hadoop-*`, `libthrift`, or `avro` at compile or runtime. The one `io.tileverse.*` dependency is `tileverse-storage-core`, for the cache registry shared with the storage layer and the `ByteBufferPool` SPI implemented by the module's buffer pool; cloud reads need a tileverse provider module on the class path, chosen by the application.

The module registers its buffer pool as tileverse's `ByteBufferPool` through `ServiceLoader`. Tileverse readers in the process, a PMTiles or COG store next to parquetry included, then draw their direct buffers from parquetry's `SegmentPool` under its fetch budget: one off-heap pool for the process instead of two.
