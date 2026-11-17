<img src="docs/src/assets/images/parquetry_banner.svg" alt="Parquetry" width="100%">

# Parquetry

Parquetry reads and writes Apache Parquet and GeoParquet files from Java. Give it a file and a filter, and it returns the matching rows. The file can sit on a local disk, an HTTP server, Amazon S3, Azure Blob Storage or Google Cloud Storage, and Parquetry fetches only the parts of it needed by the query.

Parquetry is a clean-room implementation of the Parquet format in Java. It depends on neither Hadoop nor parquet-java, and it runs no native engine: `parquetry-core` and its dependencies add 20 jars and 12 MB to an application. In a container, it sizes its memory from the memory limit of the container. It reads Parquet 1.x and 2.x files, and writes Parquet 2.0 files, or 1.1 on request, with flat and nested columns and GeoParquet metadata.

Parquetry runs in servers, desktop applications and command line tools. GeoServer Cloud 3.1.0 and newer include it and enable it by default. In JOSM, the Java editor of OpenStreetMap, the [JOSM GeoParquet](https://github.com/geoverse-dev/josm-geoparquet) plugin uses it to show GeoParquet files beside the OpenStreetMap data, and the [JOSM Overture Maps](https://github.com/overturemaps-ar-community/josm-overturemaps) plugin loads Overture Maps data through the GeoParquet plugin.

The documentation is at [parquetry.io](https://parquetry.io/).

## Add the dependency

Parquetry requires Java 25. Import the BOM, then declare `parquetry-core`, which reads and writes single Parquet and GeoParquet files:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.tileverse.parquetry</groupId>
      <artifactId>parquetry-bom</artifactId>
      <version>1.0-RC6</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>io.tileverse.parquetry</groupId>
    <artifactId>parquetry-core</artifactId>
  </dependency>
</dependencies>
```

Start the JVM with `--enable-preview --enable-native-access=ALL-UNNAMED`. [Getting started](https://parquetry.io/getting-started/) explains both flags, and [Modules](https://parquetry.io/reference/modules/) lists the other modules, such as multi-file datasets, the GeoTools data store and Arrow export.

## Read a file

The program reads the sample file of the GeoParquet specification, saved as `countries.parquet`, and prints the countries with at least 100 million inhabitants:

```bash
curl -L -o countries.parquet https://github.com/opengeospatial/geoparquet/raw/v2.0.0/examples/example.parquet
```

```java
import static io.tileverse.parquetry.filter.Pred.col;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class FirstRead {

    private static final ColumnPath NAME = ColumnPath.of("name");
    private static final ColumnPath POPULATION = ColumnPath.of("pop_est");

    public static void main(String[] args) {
        Path path = Path.of("countries.parquet");
        Predicate filter = col("pop_est").gtEq(100_000_000.0);
        Projection columns = Projection.ofPhysical(List.of(NAME, POPULATION));

        try (ByteRangeSource source = ByteRangeSource.ofFile(path)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(filter, columns, ReadOptions.DEFAULTS)) {
                rows.forEach(FirstRead::printCountry);
            }
        }
    }

    private static void printCountry(ParquetRecord row) {
        String name = row.getString(NAME);
        double population = row.getDouble(POPULATION);
        System.out.println(name + ": " + population);
    }
}
```

It prints `United States of America: 3.28239523E8`. The read uses the filter to skip the parts of the file unable to match, and returns only the matching rows.

## Documentation

- [Why Parquetry?](https://parquetry.io/why/) explains the deployment targeted by Parquetry, and when another tool fits better.
- [Getting started](https://parquetry.io/getting-started/) adds Parquetry to a project and walks through this read.
- [Guides](https://parquetry.io/guides/read-records/) read, filter and write files, read from object storage, read many files as one dataset, and tune memory.
- [GeoTools and GeoServer](https://parquetry.io/geoserver/) publishes GeoParquet files as layers in GeoServer and GeoServer Cloud.
- [par CLI](https://parquetry.io/cli/), experimental, inspects, filters and copies files from the command line.
- [Under the hood](https://parquetry.io/concepts/read-path/) explains the read path, memory budgets, spatial filtering and float statistics.
- [Reference](https://parquetry.io/reference/modules/) lists the modules and the [Parquet and GeoParquet features supported](https://parquetry.io/reference/format-support/).
- [Developer guide](https://parquetry.io/developer-guide/) covers the build, the architecture, the releases and the conventions of a contribution.

Parquetry has not reached 1.0. [Status](https://parquetry.io/#status) lists the stable and the experimental parts.

## Build

The tests read the `apache/parquet-testing` and GeoParquet test files, which are Git submodules. Clone with them:

```bash
git clone --recurse-submodules https://github.com/tileverse-io/parquetry.git

# In a clone made without them
git submodule update --init --recursive
```

Build with the Maven wrapper and JDK 25:

```bash
# Compile and run the unit tests
./mvnw test

# Unit and integration tests
./mvnw verify
```

[Build from source](https://parquetry.io/developer-guide/build/) covers single modules, test coverage and the documentation site.

## License

Apache License 2.0.

Part of [Tileverse](https://tileverse.io).
