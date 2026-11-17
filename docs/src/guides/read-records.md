# Read records

You have a Parquet file and want its rows as objects of your own Java type, with only the needed columns. This guide reads three columns of a file into a Java record, then shows how to read the values of a row and how long they stay valid.

It needs `parquetry-core`. [Getting started](../getting-started.md) adds the dependency, lists the JVM flags, and downloads the `countries.parquet` file used below.

## Read the rows into your own type

The program reads three columns of the countries into a Java record:

```java
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class ReadCountries {

    record Country(String name, String continent, double population) {}

    private static final ColumnPath NAME = ColumnPath.of("name");
    private static final ColumnPath CONTINENT = ColumnPath.of("continent");
    private static final ColumnPath POPULATION = ColumnPath.of("pop_est");

    public static void main(String[] args) {
        Path path = Path.of("countries.parquet");
        Projection columns = Projection.ofPhysical(List.of(NAME, CONTINENT, POPULATION));
        Materializer<Country> toCountry = ReadCountries::toCountry;

        try (ByteRangeSource source = ByteRangeSource.ofFile(path)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<Country> rows = reader.read(Predicate.ALWAYS_TRUE, columns, toCountry, ReadOptions.DEFAULTS)) {
                List<Country> countries = rows.toList();
                countries.forEach(System.out::println);
            }
        }
    }

    private static Country toCountry(ParquetSchema schema, ParquetRecord row) {
        String name = row.getString(NAME);
        String continent = row.getString(CONTINENT);
        double population = row.getDouble(POPULATION);
        return new Country(name, continent, population);
    }
}
```

It prints the five countries of the file, from `Country[name=Fiji, continent=Oceania, population=889953.0]` to `Country[name=United States of America, continent=North America, population=3.28239523E8]`.

A `Materializer` turns a row into a value of your type. The reader calls it once per row, while the row is readable. Without a materializer, `read` returns a stream of `ParquetRecord`, as in [Getting started](../getting-started.md#read-a-file).

## Choose the columns

The projection names the columns to decode:

- `Projection.ALL` decodes the columns of the file, in schema order.
- `Projection.ofPhysical(List.of(...))` decodes the listed columns, in the listed order.

A column is a `ColumnPath`. A nested column is named by its full path: `ColumnPath.of("address", "city")` is the `city` field of the `address` struct. A read decodes the projected columns, and also the columns of the predicate, even when the projection leaves them out.

Projecting a column absent from the file fails the read with a `ParquetSchemaException`.

## Read the values of a row

`ParquetRecord` has two families of accessors:

- The typed `get` accessors return values that you can keep: `getInt`, `getLong`, `getFloat`, `getDouble`, `getBoolean`, `getString`, `getBinary` (a copy of the bytes) and `getUuid`. They take a `ColumnPath`, or the position of the column in the projection.
- The `read` accessors return views over the decoded batch: `readBinary`, `readStruct`, `readList` and `readMap`. A view is valid only while the stream hands you the row.

`readList` returns `ParquetRecord` elements for a list of structs, and each element names its fields by their name in the struct. For a list of primitive values, the elements are the values themselves: boxed numbers, or a `MemorySegment` for each string. Read such a list as a `List<?>`, for example with `get(column)`.

`getString` and `getBinary` return `null` for a null value. A primitive accessor such as `getDouble` has no null to return: check `isNull` first on an optional column. `isNull` also answers true when a struct holding the column is null, and an accessor called on such a column throws. An accessor of the wrong type for the column, such as `getInt` on a `DOUBLE` column, throws a `ParquetSchemaException`.

`get(column)` returns any value as an `Object`: a boxed primitive, a `MemorySegment` for a binary or string column, a `java.util.List` for a list column, or a `ParquetRecord` for a struct. Boxed primitives are yours to keep. The `MemorySegment`, list, map and struct values are views over the batch, like the values of the `read` accessors: they are valid only while the stream hands you the row. Use `getString` to read a string column as a `String`, and `getBinary` to copy the bytes of a binary column.

## Keep rows

A row is a view over a batch of decoded rows, and the batch is released as the stream moves on. A materializer that builds its result from owned values, such as the `String` and `double` of the sample, needs nothing more. A result holding a view returned by `get` or by a `read` accessor is not safe to keep: copy or decode the view inside the materializer. To keep the `ParquetRecord` itself, call `detach()`: it copies the values out of the batch.

## Look at the file before reading

`ParquetFileReader` answers from the footer of the file, without reading any data:

- `schema()` returns the schema of the file. `schema().leafColumns()` lists the paths of its columns.
- `rowGroups()` lists the row groups, the blocks of rows of the file, with their row counts and sizes.
- `keyValueMetadata()` returns the key/value metadata of the file, such as the GeoParquet `geo` document.

## Options that matter

- **Opening.** `ByteRangeSource.ofFile` opens a local file. [Cloud storage](cloud-storage.md) opens a file on an HTTP server or in object storage.
- **Sharing.** A `ParquetFileReader` is safe to share between threads. Open it once per file and run concurrent reads on it. Each `read` has its own state.
- **Closing.** Close the streams returned by `read`, to release the buffers of the read. Close the `ByteRangeSource` after the last stream, because the reader does not own it. [Tune memory](tune-memory.md) sizes the memory held by the reads.
- **Read options.** `ReadOptions.DEFAULTS` turns on the pruning tiers, the levels at which a filtered read skips parts of the file. [Filter rows](filter.md#turn-a-tier-off) lists the switches.

## Where to go next

- [Filter rows](filter.md) selects rows with a predicate.
- [Read columnar batches](read-batches.md) reads column vectors instead of rows.
- [Read GeoParquet](geoparquet.md) reads geometries as JTS objects.
- [Multi-file datasets](datasets.md) reads a directory of files as one dataset.
- [JSON](json.md) reads rows as JSON.

## Under the hood

- [The read path](../concepts/read-path.md) follows a read from the footer to the rows.
