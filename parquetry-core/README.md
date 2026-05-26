# parquetry-core

Page decoders + encoders, codecs, filter pushdown, the Dremel walker, the `ParquetReader` / `ParquetDataset` read entries, and the `ParquetWriter` write entry. This is the module most consumers actually depend on. The schema model and Thrift wire records live in `parquetry-format` (one JAR upstream).

## Public API

The entry points all live in `io.tileverse.parquetry.data`:

```java
// Read one local file (pure JDK, no third-party dependency):
try (ByteRangeSource source = ByteRangeSource.ofFile(path)) {
    ParquetDataset dataset = ParquetDataset.open(source);
    try (Stream<ParquetRecord> stream = dataset.read(
            col("year").gtEq(2020).and(col("country").eq("AR")),
            Projection.of(Set.of(ColumnPath.of("year"), ColumnPath.of("country"))),
            ReadOptions.DEFAULTS)) {
        stream.forEach(System.out::println);
    }
}

// Write one file (channel-primary):
try (FileChannel sink = FileChannel.open(out, CREATE, WRITE, TRUNCATE_EXISTING);
     ParquetWriter writer = ParquetWriter.create(sink, schema, WriteOptions.defaults())) {
    rows.forEach(writer::write);
}
```

The read source is a `ByteRangeSource` (the [`parquetry-io`](../parquetry-io/) SPI). `ofFile(Path)` / `ofChannel(FileChannel)` cover local files on the JDK alone -- parquetry-core carries no `io.tileverse.*` runtime dependency. Reading from S3 / Azure / GCS / HTTP goes through the optional [`parquetry-tileverse-storage`](../integrations/parquetry-tileverse-storage/) adapter: `ParquetDataset.open(ByteRangeSources.from(rangeReader))`.

The write sink is a `WritableByteChannel`. It does not have to be seekable: any genuinely streaming output target (an HTTP request body, a blob-storage upload, anything that exposes `WritableByteChannel`) works -- rows are encoded into per-column temp files first and only consolidated onto the sink at row-group flush. An `OutputStream` overload is provided as a convenience and shims through `Channels.newChannel(...)`.

## Package layout

Direction rule: **cross-cutting capabilities stay at the top level; direction-specific machinery lives under `data.read` or `data.write`**. New code follows this rule too.

| Package | Role |
|---|---|
| `data` | Public entry points (`ParquetReader`, `ParquetDataset`, `FilesetReader`, `ParquetWriter`) and their option types (`ReadOptions`, `WriteOptions` with nested `ParquetVersion`, `RowGroupSize`, `EncodingPolicy`, `BloomFilterConfig`, `GeoParquetMetadataMode`; `WriteRow`, `WriteProgressListener`). |
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
                              |     ParquetDataset      ParquetWriter    |
                              |     ParquetReader                        |
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
                                                  parquetry-geo-jts
                                                  (JtsMaterializer)
```

## Dependencies

- `parquetry-format` (Thrift records, `ParquetFormat` facade, schema model, typed PROJJSON / GeoParquetMetadata ADTs, `SchemaBuilder` folding the `"geo"` JSON into the schema at footer-read time).
- `parquetry-io` (the `ByteRangeSource` read source and `SegmentPool` buffer-pool SPIs; pure JDK).
- `io.airlift:aircompressor-v3` (Snappy / Gzip / Lz4Raw / Zstd / Lzo / legacy LZ4 codecs + bloom-filter xxHash64).
- `org.brotli:dec` (Brotli decompression).

No `parquet-*`, `hadoop-*`, `libthrift`, or `avro` at compile or runtime, and **no `io.tileverse.*` runtime dependency**: local-file reads run on the JDK alone; cloud reads come from the optional `parquetry-tileverse-storage` adapter.
