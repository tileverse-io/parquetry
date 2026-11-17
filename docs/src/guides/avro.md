# Avro

You have Avro object container files, such as the manifest files of an Iceberg table, and want to read or write them from Java without the Apache Avro library. `parquetry-avro` reads and writes these files. It is the Avro reader used by the Iceberg support of Parquetry, and it knows nothing about Iceberg itself. This guide writes a small file and reads it back.

It needs `parquetry-avro`, with the BOM imported as in [Getting started](../getting-started.md#add-the-dependency):

```xml
<dependency>
  <groupId>io.tileverse.parquetry</groupId>
  <artifactId>parquetry-avro</artifactId>
</dependency>
```

The module depends on the byte sources and sinks of Parquetry, its compression codecs and `jackson-core`. It does not need `parquetry-core`.

## Write and read a file

The program writes two cities to `cities.avro`, then prints them:

```java
import io.tileverse.parquetry.avro.AvroDataFileReader;
import io.tileverse.parquetry.avro.AvroDataFileWriter;
import io.tileverse.parquetry.avro.AvroRecord;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.ByteSink;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

public class WriteAndReadCities {

    private static final String SCHEMA = """
            {"type": "record", "name": "City", "fields": [
              {"name": "id", "type": "long"},
              {"name": "name", "type": ["null", "string"]}
            ]}
            """;

    public static void main(String[] args) {
        Path path = Path.of("cities.avro");

        try (AvroDataFileWriter writer =
                AvroDataFileWriter.builder(SCHEMA).codec("deflate").build(ByteSink.ofFile(path))) {
            writer.write(Map.of("id", 1L, "name", "Rosario"));
            writer.write(Map.of("id", 2L, "name", "Montevideo"));
        }

        try (AvroDataFileReader reader = AvroDataFileReader.open(ByteRangeSource.ofFile(path));
                Stream<AvroRecord> records = reader.records()) {
            records.forEach(city -> System.out.println(city.get("id") + ": " + city.get("name")));
        }
    }
}
```

It prints:

```
1: Rosario
2: Montevideo
```

The writer and the reader own what they are given. Closing the writer writes the last block and closes the sink. Closing the reader closes the source.

## Read records

`reader.records()` returns a lazy stream of `AvroRecord`, read and decompressed one block of the file at a time. Each call returns a new stream starting at the first record. A record gives its values by field name, with `get("name")`, or by position, with `get(0)`.

The values have these Java types:

| Avro type | Java type |
| :--- | :--- |
| `boolean`, `int`, `long`, `float`, `double` | `Boolean`, `Integer`, `Long`, `Float`, `Double` |
| `string` | `String` |
| `bytes`, `fixed` | a read-only `MemorySegment` |
| `record`, `array`, `map` | `AvroRecord`, `List`, `Map` |
| the `null` branch of a union | `null` |
| `decimal`, `uuid` | `BigDecimal`, `UUID` |
| `date`, `time-millis`, `time-micros` | `LocalDate`, `LocalTime` |
| `timestamp-millis`, `timestamp-micros` | `Instant` |
| `local-timestamp-millis`, `local-timestamp-micros` | `LocalDateTime` |
| `duration` | `AvroDuration` |

An unknown logical type is read as its underlying type.

`reader.schema()` returns the schema written in the file, and `reader.metadata()` the metadata of its header, such as `avro.codec`. The schema keeps the custom attributes of the fields, such as the `field-id` of an Iceberg manifest: `AvroSchema.Field.attributes()` returns them as written. The `doc` attributes are not kept.

`records()` requires a record at the top level of the schema. `schema()` and `metadata()` work for any schema.

## Read with another schema

`reader.records(readerSchema)` reads the records with the schema expected by your code, following the schema resolution rules of the Avro specification. The fields missing from the reader schema are skipped, a field missing from the file takes its default, numbers are promoted, and aliases match renamed fields:

```java
import io.tileverse.parquetry.avro.AvroSchema;

AvroSchema names = AvroSchema.parse("""
        {"type": "record", "name": "City", "fields": [
          {"name": "name", "type": ["null", "string"]},
          {"name": "country", "type": "string", "default": "unknown"}
        ]}
        """);
try (Stream<AvroRecord> records = reader.records(names)) {
    records.forEach(city -> System.out.println(city.get("name") + ", " + city.get("country")));
}
```

On `cities.avro`, it prints `Rosario, unknown` and `Montevideo, unknown`. A field of the reader schema without a match and without a default raises an `AvroFormatException` when the read reaches it.

## Write records

`AvroDataFileWriter.builder(schemaJson)` takes the schema as JSON, and writes it unchanged into the header of the file. `write` takes a record as a `Map` keyed by field name, or as an `AvroRecord`. The schema decides the type of each value. The writer widens numbers, such as an `Integer` for a `long` field, and rejects a value of another kind, such as a `String` for a number, with an `AvroFormatException`.

- **Codec.** `codec(name)` compresses the blocks with `null` (no compression, the default), `deflate`, `snappy`, `zstandard`, `bzip2` or `xz`. The reader reads the six.
- **Blocks.** The writer writes a block when its records reach about 64 KiB. `blockSize(bytes)` changes the threshold.
- **Sync marker.** The 16-byte marker between blocks is random. `syncMarker(bytes)` fixes it, for files identical from run to run.
- **Sink.** `build(sink)` writes to any `ByteSink`. Closing the writer writes the last block and closes the sink.

The writer closes its sink, unlike the Parquet writer. For an upload to object storage, the sink is `ByteSink.ofOutputStream` over the upload stream of [Cloud storage](cloud-storage.md#write-to-object-storage), and closing it publishes the object. An Avro file has no footer: a writer closed after a failure publishes a valid file missing records. Keep such a writer out of the `try` resources. Close it after the last record, and call `abort()` on the upload stream after a failure.

## Limits

The module does not implement Parsing Canonical Form, CRC-64-AVRO fingerprints, single-object encoding, Avro RPC, or the container format written before Avro 1.3.

The reader checks the nesting depth, the block counts and the sync markers of a file, and rejects a corrupt or hostile file with an `AvroFormatException`.

## Where to go next

- [JSON](json.md) renders the rows of a Parquet read as JSON.
