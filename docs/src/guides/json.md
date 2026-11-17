# JSON

You want the rows of a Parquet file as JSON, for a web response, a log or a tool reading JSON. `parquetry-jackson` renders the rows of a read as JSON with Jackson 3, nested columns and Parquet Variant columns included. It is the JSON renderer of the `par` command line tool.

It needs `parquetry-jackson`, with the BOM imported as in [Getting started](../getting-started.md#add-the-dependency):

```xml
<dependency>
  <groupId>io.tileverse.parquetry</groupId>
  <artifactId>parquetry-jackson</artifactId>
</dependency>
```

The module brings Jackson 3 (`tools.jackson.core` and `tools.jackson.databind`).

## Read rows as JSON

The program prints the North American countries of the `countries.parquet` file of [Getting started](../getting-started.md#read-a-file), one JSON object per row:

```java
import static io.tileverse.parquetry.filter.Pred.col;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.jackson.JacksonMaterializers;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.schema.ColumnPath;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class CountriesAsJson {

    public static void main(String[] args) {
        Path path = Path.of("countries.parquet");
        Predicate filter = col("continent").eq("North America");
        Projection columns = Projection.ofPhysical(List.of(ColumnPath.of("name"), ColumnPath.of("pop_est")));
        Materializer<String> toJson = JacksonMaterializers.jsonString();

        try (ByteRangeSource source = ByteRangeSource.ofFile(path)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<String> rows = reader.read(filter, columns, toJson, ReadOptions.DEFAULTS)) {
                rows.forEach(System.out::println);
            }
        }
    }
}
```

It prints:

```
{"pop_est":3.7589262E7,"name":"Canada"}
{"pop_est":3.28239523E8,"name":"United States of America"}
```

`JacksonMaterializers.jsonString()` is a materializer: the read calls it for each row, as described in [Read records](read-records.md#read-the-rows-into-your-own-type), and it returns the row as a compact JSON string. The fields follow the order of the columns in the file. `JacksonMaterializers.jsonNode()` returns a Jackson `JsonNode` instead, to read or change before writing it out.

A [multi-file dataset](datasets.md) takes the same materializers in its `read` method.

## Write a stream of rows

`JsonRecordEncoder` writes the rows of a read to an `OutputStream`:

```java
import io.tileverse.parquetry.jackson.JsonRecordEncoder;
import io.tileverse.parquetry.record.ParquetRecord;

Stream<ParquetRecord> rows = reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS);
JsonRecordEncoder.writeNdjson(System.out, reader.schema(), rows);
```

- `writeNdjson` writes one JSON object per line (newline-delimited JSON), and `writeArray` writes the rows as one JSON array.
- Both read the stream to its end and close it. They leave the output stream open.
- The schema describes the columns of the rows: with `Projection.ALL`, it is the schema of the file. For a projection, use a materializer, which receives the projected schema from the read.

`JsonRecordEncoder.writeObject(generator, schema, row)` writes one row onto a Jackson `JsonGenerator` of your own, for example to place the rows inside a larger document.

## How values are written

| Parquet column | JSON |
| :--- | :--- |
| `BOOLEAN` | boolean |
| `INT32`, `INT64`, `FLOAT`, `DOUBLE` | number |
| binary with the `STRING`, `ENUM` or `JSON` logical type | string, the text of the value |
| binary with the `UUID` logical type | string, in the canonical UUID form |
| other binary columns | string, the bytes in Base64 |
| struct | object |
| list | array |
| map with string keys | object |
| map with other keys | array of `{"key": ..., "value": ...}` objects |
| Variant | the value itself: object, array, string, number, boolean or null |

A null field of an object is left out. A null inside a list, or a null map value, is written as `null`, because its position matters.

Apart from the string and UUID types of the table, values are written by their physical type, without their logical type. A `DATE` column is the number of days since 1970-01-01, a `TIMESTAMP` column is a number in the unit of the column, and a `DECIMAL` column is its unscaled number or, when stored as bytes, Base64. A WKB geometry column is Base64: the `countries.parquet` file prints its `geometry` column as a long Base64 string.

## Where to go next

- [Read records](read-records.md) reads rows into your own Java types.
- [Avro](avro.md) reads and writes Avro container files.
