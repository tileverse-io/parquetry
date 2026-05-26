# Parquetry

A modern, streaming Apache Parquet and GeoParquet reader and writer for the JVM, built for bounded-memory cloud-native and containerized servers.

Parquetry is a clean-room implementation of the Parquet format. It depends on neither Hadoop nor parquet-java, reads bytes through a pluggable range-reader (local files, HTTP, S3, Azure, GCS), and keeps only the active row group resident. Memory use stays within tight container budgets no matter how large the file.

## Why Parquetry

- **Streaming and bounded-memory.** Resident bytes are capped at the active row group. Memory use does not grow with file size, which suits services like GeoServer running in pods with a gigabyte or two to spare.
- **Cloud-native I/O.** One code path, built on tileverse-storage's `RangeReader` SPI, serves a local file, an HTTPS URL, or an object store equally.
- **Geospatial-first.** Native GeoParquet 1.x and 2.0 support, including typed CRS and optional JTS geometry materialization.
- **Lean dependencies.** No Hadoop and no parquet-java. The dependency tree stays focused instead of carrying their transitive weight into your server.

## Features

- Reads Parquet 1.x and 2.x. Writes the Parquet 2.0 page format by default, or the 1.1 page format on request for legacy-reader round-tripping. Writing is limited to flat (non-nested) columns today.
- Full Dremel record assembly on read: repeated columns, LIST, MAP, and nested structs.
- All standard codecs: Snappy, Zstd, LZ4_RAW, GZip, Brotli.
- Multi-tier filter pushdown with an explain plan: row-group statistics, dictionaries, column index, bloom filter, and record-level predicates.
- Vectorized columnar batch API backed by off-heap `MemorySegment`.
- Parallel, coalesced range fetch and parallel row-group decode within a fixed memory budget.
- Native GeoParquet: Geometry and Geography logical types, typed PROJJSON and CRS models, and an optional JTS materializer.

## Requirements

- JDK 25. The library is compiled with preview features and must be built and run with `--enable-preview`.
- Maven (a wrapper is included; use `./mvnw`).

## Coordinates

Published under the `io.tileverse.parquetry` group. Current version is `1.0-SNAPSHOT`.

```xml
<dependency>
  <groupId>io.tileverse.parquetry</groupId>
  <artifactId>parquetry-core</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

Add `parquetry-geo-jts` to materialize geometries as JTS objects. A `parquetry-bom` is provided to align module versions.

## Usage

### Reading with filter pushdown and projection

```java
import static io.tileverse.parquetry.filter.Pred.col;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;

Path file = Path.of("buildings.parquet");
try (Storage storage = StorageFactory.open(file.getParent().toUri());
        RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {

    ParquetDataset dataset = ParquetDataset.open(reader);
    try (Stream<ParquetRecord> records = dataset.read(
            col("year").gtEq(2020).and(col("country").eq("AR")),
            Projection.of(Set.of(ColumnPath.of("year"), ColumnPath.of("country"))),
            ReadOptions.DEFAULTS)) {
        records.forEach(System.out::println);
    }
}
```

`StorageFactory.open(URI)` discovers the right `Storage` for `file://`, `s3://`, `https://`, `azure://`, and more through tileverse-storage's SPI. The reader is the caller's to close; the dataset does not own it.

### Reading vectorized batches

```java
import io.tileverse.parquetry.batch.ParquetRecordBatch;

try (Stream<ParquetRecordBatch> batches = dataset.readBatches()) {
    batches.forEach(batch -> {
        try (batch) {
            // columnar, off-heap access to one row group
        }
    });
}
```

Each batch owns its own `Arena` and must be closed once consumed.

### Writing

```java
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Iterator;

// Re-encode a file with the default profile (Parquet 2.0, ZSTD).
try (ParquetDataset source = ParquetDataset.open(reader);
        OutputStream out = Files.newOutputStream(Path.of("copy.parquet"));
        ParquetWriter writer = ParquetWriter.create(out, source.schema(), WriteOptions.defaults());
        Stream<ParquetRecordBatch> batches = source.readBatches()) {

    Iterator<ParquetRecordBatch> it = batches.iterator();
    while (it.hasNext()) {
        try (ParquetRecordBatch batch = it.next()) {
            writer.writeBatch(batch);
        }
    }
}
```

`WriteOptions.defaults()` emits the Parquet 2.0 page format. For older readers, build options with `parquetVersion(WriteOptions.ParquetVersion.V1_1)` to emit the 1.1 page format instead. The writer currently handles flat (non-nested) columns; nested and repeated-column writing is in progress.

## Modules

Modules are grouped by role. The foundation - the BOMs and the layered engine - sits flat at the root; three category directories absorb growth: ecosystem adapters, end-user applications, and modules the build needs but never publishes.

```
dependencies/, bom/                      # dependency and version management
parquetry-format                         # the Parquet and Thrift wire model (read + write)
parquetry-core                           # schema, codecs, reader/writer, filter pushdown, batch API
parquetry-encryption, parquetry-variant  # placeholders: modular encryption, the Variant logical type
integrations/                            # adapters to other ecosystems
  parquetry-geo-jts                      # materialize decoded geometries as JTS objects
apps/                                    # end-user applications (a CLI lands here), as they arrive
internal/                                # not published; build and dev only
  parquetry-coverage-report              # aggregated JaCoCo coverage (under the coverage profile)
  parquetry-testkit                      # bundled test corpora + classpath extractor (git submodules)
```

Directories are for navigation; published artifact ids stay flat regardless (`io.tileverse.parquetry:parquetry-core`). Select a module by id with `-pl :parquetry-core`.

## Building

The test corpora (`apache/parquet-testing` and `opengeospatial/geoparquet`) are git submodules under `parquetry-testkit`. Clone with them, or the conformance tests cannot run:

```bash
# Fresh clone, with submodules
git clone --recurse-submodules <repo-url>

# Already cloned without them
git submodule update --init --recursive
```

```bash
# Compile and run unit tests
./mvnw test

# Unit + integration tests (apache/parquet-testing corpus, LocalStack-backed S3 reader)
./mvnw verify

# Aggregated coverage at internal/parquetry-coverage-report/target/site/jacoco-aggregate/index.html
./mvnw -Pcoverage clean verify
```

## Status

The read path is complete: Parquet 1.x and 2.x, full Dremel assembly, every standard codec, five-tier filter pushdown, GeoParquet 1.x and 2.0, the vectorized batch API, and parallel fetch and decode. The write path covers flat columns with statistics, indexes, and bloom filters; nested and repeated-column writing and parallel encoding are next. Modular encryption and the Variant logical type are reserved for future work.

## License

Apache License 2.0.

Part of [Tileverse](https://tileverse.io).
