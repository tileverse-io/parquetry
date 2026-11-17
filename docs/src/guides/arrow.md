# Arrow interop

You want to analyze the data of a Parquet file with an engine such as DuckDB,
Polars, DataFusion or pandas, and let Parquetry do the reading. Parquetry exports
a read as [Apache Arrow](https://arrow.apache.org/docs/format/Columnar.html), the
in-memory columnar format read by these engines. The engine receives the rows
left after Parquetry's pruning and filtering, and runs the query on them.

It needs `parquetry-arrow`. [Getting started](../getting-started.md#add-the-dependency)
imports the BOM, which sets the version of the module.

## The IPC writer

An Arrow IPC stream is a byte stream of Arrow data, to write to a file, a socket
or a pipe. `ArrowIpcWriter` writes the batches of a read to an output stream. The
program writes the `countries.parquet` file of
[Getting started](../getting-started.md#read-a-file) to `countries.arrows`:

```java
import io.tileverse.parquetry.arrow.ipc.ArrowIpcWriter;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

public class ExportCountries {

    public static void main(String[] args) throws IOException {
        Path path = Path.of("countries.parquet");
        Path output = Path.of("countries.arrows");

        try (ByteRangeSource source = ByteRangeSource.ofFile(path);
                OutputStream out = Files.newOutputStream(output)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            String geoJson = reader.keyValueMetadata().get("geo");
            Optional<GeoParquetMetadata> geo = Optional.ofNullable(geoJson).map(GeoParquetMetadata::parse);
            try (Stream<ParquetRecordBatch> batches =
                    reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                ArrowIpcWriter.write(reader.schema(), geo, batches, out);
            }
        }
    }
}
```

`write` takes the schema of the batches, the GeoParquet metadata of the file
when it has one, the batches, and the output. The sample reads the columns of
the file and passes the schema of the file. A read with a projection passes the
schema of the projected columns instead. The schema is checked before any byte
is written: an unsupported column fails the write at once. The `OutputStream` overload flushes but does not close the
stream; the `WritableByteChannel` overload neither flushes nor closes.

From the CLI:

```bash
par cat data.parquet -o arrow > data.arrows
```

## Feed an analytics engine

Parquetry is not an analytics engine: it reads, filters and writes files, and
runs no SQL, joins or aggregations. It can feed an engine. Its Arrow IPC output
gives an engine such as DuckDB columnar input, already narrowed to the rows and
columns of the read.

DuckDB reads Arrow IPC streams through the `nanoarrow` community extension. With
DuckDB 2.0.0, this command reads the `countries.arrows` file written above:

```bash
duckdb -c "install nanoarrow from community; load nanoarrow;
  install spatial; load spatial;
  select name, pop_est, ST_GeometryType(geometry) as type
  from read_arrow('countries.arrows') where pop_est > 100000000;"
```

It returns one row: `United States of America`, `328239523.0`, `MULTIPOLYGON`.
DuckDB reads the `geometry` column as geometries, from its `geoarrow.wkb` tag
([GeoArrow](#geoarrow)).

Through GeoServer, the same output reaches beyond GeoParquet files. The
`arrow-ipc` and `geoparquet` WFS output formats, added by Parquetry to GeoServer
Cloud, apply to any vector layer, whatever store is behind it: PostGIS,
shapefiles, GeoPackage or GeoParquet. The WFS request filters the features in
that store, and the engine receives the result. The GeoServer Cloud guide
[GeoParquet and Arrow IPC over WFS](https://geoserver.org/geoserver-cloud/user-guide/wfs-geoparquet-arrow-ipc/)
downloads a layer in both formats and reads it with DuckDB and pyarrow.

## Two forms: IPC and the C Data Interface

Arrow defines two unrelated ways to move columnar data, and Parquetry implements
both because they answer different questions.

- **Arrow IPC stream** (`ArrowIpcWriter`) is the *serialization* format: a framed
  byte stream of [flatbuffer](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication)
  messages - one Schema message, then one RecordBatch message per batch, then an
  end-of-stream marker. It is bytes you write to a file, a socket, or a pipe and
  read back somewhere else: another process, another language, or later in time.
  The writer converts the batches to Arrow's buffer layout, and writes those
  buffers to the channel without copying them again. See the [IPC streaming format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).
- **Arrow C Data Interface** (`ArrowCDataExporter`) is an *in-process ABI*: a
  small set of C structs (`ArrowSchema`, `ArrowArray`, and the streaming
  `ArrowArrayStream`) that let two components in the same process share Arrow data
  by passing pointers, without serialization. The consumer reads the
  producer's buffers directly by address; a `release` callback on each struct
  hands ownership back when the consumer is done. Parquetry fills an
  `ArrowArrayStream`, and a native consumer pulls batches from it (DuckDB
  `registerArrowStream`, Polars, or any C Data consumer). See the
  [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) and
  the [C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html).

The short version: reach for the **IPC stream** to persist or send Arrow across a
boundary, and the **C Data Interface** to hand Arrow to another library running in
the same JVM process without serialization. The C Data exporter converts each
batch to Arrow's buffer layout, as the IPC writer does, then copies each buffer
once into pooled native memory, held until the consumer releases the batch
([Lifecycle and ownership](#lifecycle-and-ownership)). Both
are driven from the same columnar read path and the same Parquet-to-Arrow type
mapping below.

Parquetry's read path produces columnar batches (`ParquetRecordBatch`). The
Arrow exporters reshape those batches into Arrow's layout. Both forms are
read-path only; the Parquet writer is not involved.

## The C Data Interface exporter

The caller provides an allocated `ArrowArrayStream` struct (for example
arrow-java's `ArrowArrayStream.allocateNew`, or a raw FFM segment) and Parquetry
fills its callbacks. The program hands the rows of `countries.parquet` to DuckDB
through its JDBC driver, and prints the countries with more than 100 million
inhabitants:

```java
import io.tileverse.parquetry.arrow.cdi.ArrowCDataExporter;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;

public class QueryWithDuckDb {

    public static void main(String[] args) throws SQLException {
        Path path = Path.of("countries.parquet");

        try (ByteRangeSource source = ByteRangeSource.ofFile(path);
                BufferAllocator allocator = new RootAllocator();
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
                DuckDBConnection connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            String geoJson = reader.keyValueMetadata().get("geo");
            Optional<GeoParquetMetadata> geo = Optional.ofNullable(geoJson).map(GeoParquetMetadata::parse);
            Stream<ParquetRecordBatch> batches =
                    reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS);
            ArrowCDataExporter.export(reader.schema(), geo, batches, stream.memoryAddress());
            connection.registerArrowStream("countries", stream);
            try (Statement statement = connection.createStatement();
                    ResultSet result =
                            statement.executeQuery("SELECT name FROM countries WHERE pop_est > 100000000")) {
                while (result.next()) {
                    System.out.println(result.getString("name"));
                }
            }
        }
    }
}
```

It prints `United States of America`. Beside `parquetry-arrow`, the sample
needs three modules, with their versions set by `parquetry-dependencies`
([Getting started](../getting-started.md#add-the-dependency)):
`org.apache.arrow:arrow-c-data`, an allocation manager of arrow-java such as
`org.apache.arrow:arrow-memory-unsafe`, and `org.duckdb:duckdb_jdbc`. Without
an allocation manager, `new RootAllocator()` fails.

On Java 25, the JVM of the application needs three flags:

```bash
java --enable-native-access=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --sun-misc-unsafe-memory-access=allow \
    -cp <class path> QueryWithDuckDb
```

- `--enable-native-access=ALL-UNNAMED` lets the exporter and DuckDB call native
  code. Without it, the JDK prints warnings.
- `--add-opens=java.base/java.nio=ALL-UNNAMED` is required by arrow-java.
  Without it, `ArrowArrayStream.allocateNew` fails.
- `--sun-misc-unsafe-memory-access=allow` lets `arrow-memory-unsafe` use
  `sun.misc.Unsafe`. Without it, the JDK prints deprecation warnings.

The export takes the batch stream over: the consumer's `release` of the
`ArrowArrayStream` closes it. The sample therefore leaves `batches` out of the
`try` resources.

`ArrowCDataExporter.export` fills an `ArrowArrayStream` struct given its address.
A caller that allocated the struct from its own FFM arena passes that
`MemorySegment` to the segment overload instead.

### Lifecycle and ownership

The stream pulls lazily. `get_next` pulls the next `ParquetRecordBatch` on
demand, converts it to Arrow's layout, copies it into pooled buffers, and
returns it; the stream's `release` closes the batch stream. A single sequential
stream still decodes ahead behind the `get_next` boundary.

A batch goes out in two steps. The exporter first builds the Arrow layout of
the batch on the heap: the validity bitmaps, the offsets and the padded values,
as the IPC writer does. It then copies each of these buffers once into a
segment borrowed from a pool. The second copy is mandatory for the C Data
Interface: a C consumer needs a stable native address, and heap buffers move
and are recycled when the batch closes. The `release` of an exported array
returns its buffers to the pool.

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
| INT32 `INT(8/16/32, signed or unsigned)` | Int32 | sign from the annotation, physical width kept |
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

## Under the hood

- [The read path](../concepts/read-path.md) describes how a read produces the
  columnar batches exported by both forms.
