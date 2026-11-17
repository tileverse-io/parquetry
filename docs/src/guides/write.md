# Write files

You have data in Java and want to store it as a Parquet or GeoParquet file, ready for other tools to read and filter. This guide writes a file from rows built in Java, with flat and nested columns, then a GeoParquet file with a geometry column.

It needs `parquetry-core`. [Getting started](../getting-started.md) adds the dependency and lists the JVM flags.

## Write a file

The program writes three cities to `cities.parquet`:

```java
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteSink;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

public class WriteCities {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath NAME = ColumnPath.of("name");

    public static void main(String[] args) {
        Path path = Path.of("cities.parquet");
        ParquetSchema schema = citySchema();

        try (ByteSink sink = ByteSink.ofFile(path);
                ParquetFileWriter writer = ParquetFileWriter.create(sink, schema, WriteOptions.defaults());
                ParquetRecordBatchBuilder rows = writer.appender()) {
            rows.setLong(ID, 1).setString(NAME, "Rosario").endRow();
            rows.setLong(ID, 2).setString(NAME, "Montevideo").endRow();
            rows.setLong(ID, 3).endRow();
        }
    }

    private static ParquetSchema citySchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive name = new SchemaNode.Primitive(
                "name",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, name), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
```

It prints nothing, and leaves `cities.parquet` with three rows. The third city has no name: the `name` column is optional, and a column left unset in a row is written as null.

## Describe the schema

A schema is a tree of `SchemaNode` records. The root is a `SchemaNode.Group`, and the columns are nodes under it:

- `SchemaNode.Primitive` is a column of values. Its arguments are the name, the repetition, the physical type, the byte length of a `FIXED_LEN_BYTE_ARRAY` column, the logical type, and the field id.
- `SchemaNode.Group` holds other nodes: a struct, a list or a map. Its arguments are the name, the repetition, the children, the logical type, and the field id.

The repetition is `REQUIRED`, `OPTIONAL` or `REPEATED`. The logical type refines the physical type: `LogicalType.StringType` marks a `BYTE_ARRAY` column as holding UTF-8 strings. A field id of `-1` means no field id.

## Add rows

`writer.appender()` returns a row builder bound to the writer:

- `setLong`, `setString`, `setDouble` and the other setters stage the value of a column. They take a `ColumnPath`, or the position of the column among the leaf columns of the schema.
- `endRow()` closes the row. An unset optional column is null. An unset required column fails the row with a `ParquetWriteException`.
- The builder sends a batch to the writer every 8,192 rows, and when its staged values reach 16 MiB. Closing the builder, or the writer, sends the last rows.

The writer starts a new row group, the next block of rows of the file, when the current one reaches its target size. `flushRowGroup()` closes the current row group earlier.

## Write nested columns

The writer writes structs, lists and maps, and containers nested in each other. A struct is a group with fields. A list is a group with the `LIST` logical type, holding a repeated group named `list`, which holds the `element` node:

```java
SchemaNode.Group address =
        new SchemaNode.Group("address", Repetition.OPTIONAL, List.of(city), Optional.empty(), -1);
SchemaNode.Group list =
        new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
SchemaNode.Group tags = new SchemaNode.Group(
        "tags", Repetition.OPTIONAL, List.of(list), Optional.of(new LogicalType.ListType()), -1);
```

Here `city` and `element` are optional string columns, declared like `name` above. The row builder opens a scope for a nested value:

```java
rows.setLong(ID, 1);
rows.beginStruct("address").setString(ColumnPath.of("address", "city"), "Rosario").endStruct();
rows.beginList("tags").addString("port").addString("river").endList();
rows.endRow();
```

- A struct opens with `beginStruct` and closes with `endStruct`. Its fields are named by their full path.
- A list opens with `beginList` and closes with `endList`. `addString`, `addInt` and the other `add` methods append a value element. A list of structs adds an element between `addElement()` and `endElement()`, and names the element fields by their path under the list, such as `ColumnPath.of("element", "city")`.
- A map opens with `beginMap` and closes with `endMap`. An entry sits between `putEntry()` and `endEntry()`, and its fields are `ColumnPath.of("key_value", "key")` and `ColumnPath.of("key_value", "value")`.
- A list or map with nothing added is empty. `setNull(path)` on the path of a struct, list or map makes it null.

The writer writes lists in the standard three-level form of the Parquet format. A list declared in an older form, such as a bare repeated column, is written in the standard form. Per-column options then name its leaf by its standard path, `tags.list.element`.

## Write batches

`writer.writeBatch(batch)` appends a `ParquetRecordBatch` in one call. The batch columns must match the schema of the writer. A batch comes from a read, such as the batches of [Read columnar batches](read-batches.md), or from `ParquetRecordBatchBuilder.forSchema(schema)`, which builds rows as the appender does and returns them through `build()`.

## Write a GeoParquet file

A column becomes a geometry column when the write options give it a coordinate reference system. Its values are WKB bytes. The program writes the location of a city:

```java
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.io.ByteSink;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBWriter;

public class WritePlaces {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    public static void main(String[] args) {
        Path path = Path.of("places.parquet");
        ParquetSchema schema = placeSchema();
        WriteOptions options = WriteOptions.builder().crsEpsg("geometry", 4326).build();
        GeometryFactory geometries = new GeometryFactory();
        WKBWriter wkb = new WKBWriter();

        try (ByteSink sink = ByteSink.ofFile(path);
                ParquetFileWriter writer = ParquetFileWriter.create(sink, schema, options);
                ParquetRecordBatchBuilder rows = writer.appender()) {
            Point rosario = geometries.createPoint(new Coordinate(-60.65, -32.95));
            MemorySegment rosarioWkb = MemorySegment.ofArray(wkb.write(rosario));
            rows.setLong(ID, 1).setBinary(GEOMETRY, rosarioWkb).endRow();
        }
    }

    private static ParquetSchema placeSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, geometry), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
```

The CRS options declare the geometry columns:

- `crsEpsg(column, code)` takes an EPSG code from the bundle shipped with Parquetry, and fails for a code missing from it.
- `crs(column, projjson)` takes a PROJJSON document, as a string or as a parsed `CoordinateReferenceSystem`.
- `crsUnknown(column)` declares the CRS unknown, written as `"crs": null` in the `geo` document.

With the default options, the file has three spatial additions:

- **GeoParquet 1.1 and 2.0 metadata.** The metadata mode `DUAL_V1_1_AND_V2_0` writes the native Parquet `GEOMETRY` logical type on the column, geometry statistics per row group, and a `geo` document of version 1.1.0. `geoParquetMetadata(GeoParquetMetadataMode.V2_0_ONLY)` writes the logical type, the geometry statistics and a `geo` document of version 2.0.0. `V1_1_ONLY` writes the `geo` document of version 1.1.0 alone. A file in Parquet version 1.1 is always written in `V1_1_ONLY`.
- **A `bbox` covering.** The writer adds a `bbox` struct column with the `xmin`, `ymin`, `xmax` and `ymax` of each geometry, and declares it in the `geo` document. A read uses these columns to skip row groups and pages, as described in [Filter by location](spatial-filter.md#make-files-that-filter-well). The covering is written for the primary geometry column, the first geometry column of the schema. Its values are `FLOAT` for a column in OGC:CRS84 or EPSG:4326, and `DOUBLE` otherwise. Its pages hold at most 1,024 values, and `coveringPageValueLimit(n)` changes that limit.
- **Plain geometry pages.** A geometry column is written without dictionary encoding, because geometries rarely repeat. Dictionary encoding stores the distinct values of a column once.

`bboxCovering(CoveringMode.NONE)` writes no covering, and `CoveringMode.FLOAT` or `CoveringMode.DOUBLE` forces the type of its values. The covering is written in any metadata mode, `V2_0_ONLY` included. A schema may already have a top-level `bbox` struct, as written by some GeoParquet 1.0 writers. The writer then declares that struct as the covering when it follows the GeoParquet rules for a covering column, and adds no other. `existingBboxCovering(...)` names the covering columns of a schema explicitly.

## Choose the order of float statistics

The min/max statistics of `FLOAT`, `DOUBLE` and `FLOAT16` columns follow the type-defined order by default. The IEEE 754 total order is the order recommended by the Parquet format, and some readers in use today cannot open a file written in it. [Float statistics](../concepts/float-statistics.md) compares the two orders and lists the readers tested.

```java
import io.tileverse.parquetry.data.WriteOptions;

WriteOptions options = WriteOptions.builder()
        .floatColumnOrder(WriteOptions.FloatColumnOrder.IEEE_754_TOTAL_ORDER)
        .build();
```

The [`par cp`](../cli/copy.md) command takes the same choice when it copies a file:

```bash
par cp --float-column-order ieee_754_total_order in.parquet out.parquet
```

The option applies to the float columns of the file, the `bbox` covering columns of a GeoParquet file included.

## Options that matter

`WriteOptions.defaults()` writes Parquet 2.0 pages, compressed with ZSTD at level 3, with a page index. The page index records the position, minimum and maximum of the pages, and lets a filtered read skip pages. `WriteOptions.builder()` changes one setting at a time:

| Option | Default | What it controls |
| :--- | :--- | :--- |
| `parquetVersion` | `V2_0` | The page format. `V1_1` writes the older data page headers, for readers that need them, and no page index. |
| `defaultCompression`, `compression(column, codec)` | `Compression.zstd(3)` | The codec of the columns, or of one column. The writer compresses with `uncompressed()`, `snappy()`, `gzip()`, `lz4Raw()`, `zstd(level)` and `lzo()`. `lzo()` writes raw LZO blocks ([Format support](../reference/format-support.md#compression)). Brotli and the deprecated LZ4 codec are read-only. |
| `rowGroupSize` | about 128 MB of uncompressed data | `RowGroupSize.bytes(n)` sets another size, `RowGroupSize.rows(n)` a row count. |
| `pageValueLimit`, `pageByteLimit` | 8,192 values, 64 KiB | The size of a data page. Smaller pages let a filter skip finer ranges of rows, and make the page index larger. |
| `encodingPolicy(column, policy)` | dictionary when it fits | `EncodingPolicy.FORCE_PLAIN` writes a column without a dictionary. A column falls back to plain encoding when its dictionary passes `dictionaryByteLimit`, 1 MiB by default. |
| `indexedColumns(columns...)`, `bloomFilter(column, config)` | none | Bloom filters on the named columns, used by reads to skip row groups on `eq` and `in` predicates. |
| `keyValueMetadata(key, value)` | none | Entries in the key/value metadata of the file. The `geo` key is reserved for the writer. |
| `tempDir` | `java.io.tmpdir` | The directory of the temporary files of the writer. |

Per-column options name a column by its dotted path, such as `address.city`. An option naming no column of the schema is ignored.

[Lay out files for selective queries](file-layout.md) explains how row group and page sizes, the page index and the order of the rows shape the bytes fetched by a filtered read.

## Finish the file

- **Close the writer, then the sink.** Closing the writer writes the last row group and the footer. The writer never closes the sink: closing the sink after the writer commits the file. The samples write a local file: they declare the sink first in the `try` block, and Java closes the writer, then the sink.
- **On failure.** A writer that fails writes no footer, and the output is not a valid Parquet file. For a local file, as in the samples, the `try` block still closes the sink: delete the file. A sink that commits on close behaves differently. `ByteSink.ofOutputStream` closes its stream, and closing an object-storage upload stream publishes the object. After a failure, do not close such a sink: abort the upload through the API of its storage instead. Keep that sink out of the `try` resources, and close it only after the writer closed without error. [Cloud storage](cloud-storage.md#write-to-object-storage) shows such an upload.
- **Temporary files.** The writer encodes the columns of the current row group into temporary files under `tempDir`, and appends the row group to the sink when it is complete. [Tune memory](tune-memory.md#temporary-files-of-the-writer) chooses their directory. The sink is written front to back only: `ByteSink.ofOutputStream` turns any `OutputStream` into a sink.
- **Threads.** A writer is not thread-safe. Write to it from one thread. The writer encodes the columns of a row group in parallel, on a shared thread pool. The output bytes do not depend on that parallelism.

## Where to go next

- [Read records](read-records.md) reads the file back.
- [Filter rows](filter.md#write-files-that-filter-well) describes the files that filter well.

## Under the hood

- [Float statistics](../concepts/float-statistics.md) explains the statistics of float columns.
