# Parquetry

Hadoop-free, Java-25, clean-room Apache Parquet library with native GeoParquet 2.0 support. Reads Parquet 1.x and 2.x files; writes Parquet 2.x.

**Status:** read-only core for Parquet 1.x and 2.x, full Dremel decoding, four-tier filter pushdown (stats, dictionary, column-index, bloom, record-level), native GeoParquet 1.x and 2.0 reads via the JTS materializer. Typed PROJJSON and GeoParquetMetadata models ship as part of the format layer (`io.tileverse.parquetry.schema.geo.*`). The vectorized batch API and the write path are not yet implemented.

## Build

Requires JDK 25.

```bash
./mvnw verify
```

Run only unit tests: `./mvnw test`.
Run unit + integration tests (including the apache/parquet-testing corpus and a LocalStack-backed S3 reader): `./mvnw verify`.
Generate an aggregated JaCoCo coverage report at `coverage-report/target/site/jacoco-aggregate/index.html`: `./mvnw -Pcoverage clean verify`.

## Usage

```java
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.read.ReadOptions;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import java.net.URI;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;

import static io.tileverse.parquetry.filter.Pred.col;

Path file = Path.of("data.parquet");
try (Storage storage = StorageFactory.open(file.getParent().toUri());
     RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
    ParquetDataset dataset = ParquetDataset.open(reader);
    try (Stream<ParquetRecord> stream = dataset.read(
            col("year").gtEq(2020).and(col("country").eq("AR")),
            Projection.of(Set.of(ColumnPath.of("year"), ColumnPath.of("country"))),
            ReadOptions.DEFAULTS)) {
        stream.forEach(System.out::println);
    }
}
```

`StorageFactory.open(URI)` discovers the right `Storage` implementation for `file://`, `s3://`, `https://`, `azure://`, etc. via tileverse-storage's SPI.

## Design


## License

Apache License 2.0.
