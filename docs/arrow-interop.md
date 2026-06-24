# Arrow interop

## Why parquetry exports Arrow

parquetry reads Parquet and GeoParquet and does the work that lives close to the
file format: row-group and page pruning, spatial pruning, dictionary and
column-index filtering, WKB geometry tests. It is not a query engine and does not
try to be. [Apache Arrow](https://arrow.apache.org/docs/format/Columnar.html) is
the common in-memory columnar format the analytics ecosystem already speaks.
Exporting Arrow lets parquetry hand its pruned, filtered read output to an engine
that does have a query layer - DuckDB, Polars, DataFusion, pandas - without
parquetry reimplementing SQL and without the consumer reimplementing a Parquet
reader.

parquetry's read path already produces columnar batches (`ParquetRecordBatch`).
The Arrow exporters reshape those batches into Arrow's layout. Both forms are
read-path only; the Parquet writer is not involved.

## Two forms: IPC and the C Data Interface

Arrow defines two unrelated ways to move columnar data, and parquetry implements
both because they answer different questions.

- **Arrow IPC stream** (`ArrowIpcWriter`) is the *serialization* format: a framed
  byte stream of [flatbuffer](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication)
  messages - one Schema message, then one RecordBatch message per batch, then an
  end-of-stream marker. It is bytes you write to a file, a socket, or a pipe and
  read back somewhere else: another process, another language, or later in time.
  The write is zero-copy: parquetry sends the live column buffers straight to the
  channel. See the [IPC streaming format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).
- **Arrow C Data Interface** (`ArrowCDataExporter`) is an *in-process ABI*: a
  small set of C structs (`ArrowSchema`, `ArrowArray`, and the streaming
  `ArrowArrayStream`) that let two components in the same process share Arrow data
  by passing pointers, with no serialization at all. The consumer reads the
  producer's buffers directly by address; a `release` callback on each struct
  hands ownership back when the consumer is done. parquetry fills an
  `ArrowArrayStream` a native consumer pulls batches from (DuckDB
  `registerArrowStream`, Polars, or any C Data consumer). See the
  [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) and
  the [C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html).

The short version: reach for the **IPC stream** to persist or send Arrow across a
boundary, and the **C Data Interface** to hand Arrow to another library running in
the same JVM process with no copies and no serialization overhead. Both are driven
from the same columnar read path and the same Parquet-to-Arrow type mapping
below.

## The IPC writer

```java
// projectedSchema, geo, and batches come from the read path:
//   ParquetSchema projectedSchema           - the projected read schema
//   Optional<GeoParquetMetadata> geo        - present for a GeoParquet read
//   Stream<ParquetRecordBatch> batches      - the columnar batch stream
try (batches) {
    ArrowIpcWriter.write(projectedSchema, geo, batches, outputStream);
}
```

The projected schema is validated up front; an unsupported column fails before
any byte is written. The `OutputStream` overload flushes but does not close the
stream; the `WritableByteChannel` overload neither flushes nor closes.

From the CLI:

```bash
par cat data.parquet -o arrow > data.arrows
```

## The C Data Interface exporter

The caller provides an allocated `ArrowArrayStream` struct (for example
arrow-java's `ArrowArrayStream.allocateNew`, or a raw FFM segment) and parquetry
fills its callbacks:

```java
try (BufferAllocator allocator = new RootAllocator();
        ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
        Stream<ParquetRecordBatch> batches = openBatches()) {
    ArrowCDataExporter.export(projectedSchema, geo, batches, stream.memoryAddress());
    try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
        conn.registerArrowStream("scan", stream);
        // SELECT ... FROM scan
    }
}
```

`ArrowCDataExporter.export` fills an `ArrowArrayStream` struct given its address.
A caller that allocated the struct from its own FFM arena passes that
`MemorySegment` to the segment overload instead.

Running the C Data exporter requires native access for the module (the
`--enable-native-access` JVM flag); the project's `.mvn/jvm.config` sets it for
the build.

### Lifecycle and ownership

The stream pulls lazily. `get_next` pulls the next `ParquetRecordBatch` on
demand, copies it into pooled buffers, and returns it; the stream's `release`
closes the batch iterator, the reader, and the storage. A single sequential
stream still decodes ahead behind the `get_next` boundary.

The copy into pooled buffers is mandatory for the C Data Interface: a C consumer
needs a stable native address, while parquetry's binary, validity, and offset
buffers are heap-backed and recycled when the batch closes. It is exactly one
`MemorySegment.copy` per buffer into a segment borrowed from a pool; each
exported array's `release` returns its buffers to the pool. There is no heap
staging and no per-batch zeroing.

### Consumer contract

- A consumer drives `get_schema` once, then `get_next` until it returns an array
  whose `release` is null (end of stream), then `release` on the stream.
- `get_next` is serialized internally; a consumer that parallelizes downstream
  work and `release` (DuckDB does) is safe, since each array owns its buffers and
  releases touch disjoint segments.
- Releasing an array twice is a no-op.

## Parquet to Arrow type mapping

| Parquet | Arrow type | Notes |
|---------|-----------|-------|
| BOOLEAN | Bool | |
| INT32 | Int32 | signed |
| INT32 `INT(8/16/32, signed=…)` | Int32 | sign from the annotation, physical width kept |
| INT64 | Int64 | unsigned `INT(64,false)` maps to UInt64 |
| FLOAT / DOUBLE | Float32 / Float64 | |
| FIXED_LEN_BYTE_ARRAY `FLOAT16` | Float16 | |
| BYTE_ARRAY `STRING`/`ENUM`/`JSON` | Utf8 | |
| BYTE_ARRAY | Binary | |
| FIXED_LEN_BYTE_ARRAY | FixedSizeBinary(N) | |
| INT32 `DATE` | Date32 (days) | |
| INT32/INT64 `TIME` | Time32 / Time64 | unit from the annotation |
| INT64 `TIMESTAMP` | Timestamp | unit + UTC adjustment from the annotation |
| INT96 | Timestamp (micros, no tz) | 12-byte value converted to int64 micros |
| INT32/INT64/BYTE_ARRAY/FIXED `DECIMAL` | Decimal128 | unscaled value re-encoded 16-byte little-endian |
| LIST group | List | element recurses |
| MAP group | Map | `list<struct<key, value>>`; key field non-nullable |
| struct group | Struct | one child per field |
| Variant group | Struct&lt;metadata, value&gt; | tagged `arrow.variant` (see below) |
| geometry / geography leaf | Binary | tagged `geoarrow.wkb` with `crs`/`edges` metadata |

### Variant

A Parquet Variant exports as Arrow `struct<metadata: binary, value: binary>`
tagged with the `arrow.variant` extension name, following the `geoarrow.wkb`
precedent. A shredded Variant is reconstructed to its unshredded canonical form
on the way out. A consumer that does not interpret the extension (DuckDB) reads
the two binary fields directly; an Arrow-native consumer can recognize the
Variant from the tag.

### GeoArrow

A geometry or geography column exports as `geoarrow.wkb`: Arrow Binary holding
the WKB, with the `ARROW:extension:name` and an `ARROW:extension:metadata` JSON
document recording the CRS and edge interpretation. Geometry is detected from the
GeoParquet 2.0 logical type or from the GeoParquet 1.x `geo` metadata.

## Documented limits

- **Decimal256** (DECIMAL precision 39-76) is rejected; the Arrow target is
  Decimal128 (precision up to 38).
- **Deeply-nested-shredded Variant** (a Variant under a list or map) is rejected
  with a precise message. Top-level and struct-nested Variant are supported.
