# parquetry-avro

A clean-room Avro Object Container File (OCF) reader and writer. It decodes Avro binary data and exposes the embedded
write schema, including custom field attributes (for example Iceberg's `field-id`), for the caller to interpret, and it
authors OCF files from a schema and caller-supplied records. It is generic and Iceberg-agnostic: it knows nothing about
manifest semantics.

## Why it exists

Reading Iceberg metadata otherwise pulls `apache-avro` (and, through `parquet-avro`, Hadoop and Commons). This module
replaces that with a focused reader whose only dependencies are `parquetry-io` (`ByteRangeSource`),
`parquetry-compression`, and `jackson-core` (streaming parse of the header schema JSON). It is the same species as
`parquetry-format`: a hand-rolled wire reader that replaces a heavy external library rather than adapting to one.

## Scope

- The full Avro schema language: namespaces, recursive types, aliases, field defaults, and logical-type annotations.
- All six block codecs named by the spec: `null`, `deflate`, `snappy`, `zstandard`, `bzip2`, and `xz`.
- Faithful value decoding: the documented Java boxing per Avro type, plus logical values (`BigDecimal`, `UUID`,
  `java.time` types, `AvroDuration`) for the logical types the spec defines.
- Reader-schema resolution through `records(readerSchema)`: projection, defaults, spec promotions, and aliases;
  incompatibilities raise `AvroFormatException` only when decoding reaches them.
- Bounded and defensive: one OCF block is read, decompressed, and decoded at a time; `records()` is a lazy `Stream`;
  nesting depth, block counts, and sync markers are validated against corrupt or hostile input.
- Custom field attributes are exposed verbatim; `doc` attributes are not preserved.
- A deliberate API constraint: `records()` requires the file's top-level schema to be a record; `schema()` and
  `metadata()` work for any top-level schema.

Writing, through `AvroDataFileWriter`:

- General-purpose authoring where the schema is the authority: varied input is coerced to the types the schema declares.
- Record values supplied as a `Map` keyed by field name, or as an `AvroRecord`.
- The schema is supplied as a JSON string and embedded verbatim as the `avro.schema` metadata entry.
- All six block codecs named by the spec: `null`, `deflate`, `snappy`, `zstandard`, `bzip2`, and `xz`; the default is
  `null`.
- A block is flushed once its buffered data reaches about 64 KiB.
- The 16-byte sync marker is random by default, with a deterministic override for reproducible output.
- Output is written through `ByteSink`.

Not implemented:

- Parsing Canonical Form.
- CRC-64-AVRO fingerprints.
- Single-object encoding.
- RPC.
- The pre-1.3 container format (magic `Obj` followed by a zero byte).

## Usage

```java
    try (AvroDataFileReader reader = AvroDataFileReader.open(ByteRangeSource.ofFile(path))) {
        AvroSchema schema = reader.schema();
        reader.records().forEach(record -> {
            Object dataFile = record.get("data_file");
            // interpret field-ids and bounds downstream
        });
    }
```

Each call to `records()` or `records(readerSchema)` returns an independent stream that starts at the first data block.

```java
    try (AvroDataFileWriter writer = AvroDataFileWriter.builder(schemaJson)
            .codec("deflate")
            .build(ByteSink.ofFile(path))) {
        writer.write(Map.of("id", 1L, "name", "alice"));
    }
```
