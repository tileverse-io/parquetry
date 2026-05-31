# parquetry-jackson

A small, reusable adapter that renders parquetry reads as JSON, including nested (list / map / struct) and Parquet
Variant columns. It is the JSON engine behind the `par` CLI, and it is usable on its own by any consumer that wants
JSON out of a Parquet read without touching parquetry's `ParquetRecord` or `Variant` types.

The module is presentation-only and depends just on `parquetry-core` plus Jackson 3 (`tools.jackson.core` and
`tools.jackson.databind`); `parquetry-core` itself stays free of any JSON or formatting concern.

## What it provides

Two facets wrap one traversal, keeping the JSON mapping in exactly one place:

### `JsonRecordEncoder` - streaming encoder

Writes onto a caller-owned Jackson `JsonGenerator`:

```java
// One row as a JSON object on a generator you own.
static void writeObject(JsonGenerator generator, ParquetSchema schema, ParquetRecord row);

// One non-null value, resolved against its schema node (used for delimited/text cells).
static void writeValue(JsonGenerator generator, SchemaNode node, Object value);

// Whole-stream conveniences. Each fully drains and closes `rows`, and leaves `out` open for the caller.
static void writeNdjson(OutputStream out, ParquetSchema schema, Stream<ParquetRecord> rows); // object-per-line
static void writeArray(OutputStream out, ParquetSchema schema, Stream<ParquetRecord> rows);  // a single JSON array
```

### `JacksonMaterializers` - read-pipeline materializers

Plug straight into the core read overload `read(Predicate, Projection, Materializer<T>, ReadOptions)`; a read then
yields JSON directly:

```java
static Materializer<JsonNode> jsonNode();   // one Jackson JsonNode per row
static Materializer<String> jsonString();   // one compact JSON string per row
```

## JSON mapping

The encoder walks the projected schema in lockstep with the row:

- **Scalars** render by physical kind: `BOOLEAN` to a JSON boolean; `INT32`/`INT64`/`FLOAT`/`DOUBLE` to numbers; a
  binary column to a UTF-8 string when it has a string-like logical type (`String`/`Enum`/`Json`), otherwise to Base64.
- **Struct** to a JSON object.
- **List** to a JSON array; a null element is emitted as explicit JSON `null`.
- **Map** to a JSON object when its keys are strings, otherwise to an array of `{"key": ..., "value": ...}` entries.
  A null map value is emitted as explicit `null`.
- **Variant** renders inline by its own type: object, array, boolean, string, number, or null; no wrapper.
- **Nulls**: null object fields are omitted; null positions inside arrays and maps are emitted as explicit `null`
  (positions there cannot be dropped).

Scalar logical-type formatting is intentionally out of scope: Parquet-column scalars render by physical kind (DATE,
TIMESTAMP, DECIMAL, UUID are not specially formatted), and WKB geometry renders as its UTF-8 string, unchanged. A
future, separate effort can add typed scalar JSON.

## Usage

Read a Parquet file straight into `JsonNode`s:

```java
Materializer<JsonNode> toJson = JacksonMaterializers.jsonNode();
try (Stream<JsonNode> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, toJson, ReadOptions.DEFAULTS)) {
    rows.forEach(node -> System.out.println(node.get("name").stringValue()));
}
```

Stream newline-delimited JSON onto an `OutputStream`:

```java
try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
    JsonRecordEncoder.writeNdjson(System.out, dataset.schema(), rows);
}
```

Drive your own generator (e.g. to embed a row inside a larger document):

```java
JsonRecordEncoder.writeObject(generator, projectedSchema, record);
```

## Coordinates

`io.tileverse.parquetry:parquetry-jackson`. Depends on `parquetry-core` and Jackson 3; requires Java 25 (matching the
rest of parquetry).
