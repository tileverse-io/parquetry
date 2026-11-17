# Getting started

You have a Java 25 project and a Parquet file, and want the rows matching a condition. This page adds Parquetry to a Maven project and runs a short program printing the countries of a sample file with at least 100 million inhabitants. The latest release is `1.0-RC6`, published on Maven Central.

## Requirements

Parquetry requires Java 25. The modules using preview features run on Java 25 only, and `parquetry-core` also runs on newer versions. Start the JVM of your application with these two flags:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED -jar your-application.jar
```

- `--enable-preview` is required by the modules compiled with Java 25 preview features. `parquetry-catalog` is one of them, and the GeoTools data store depends on it. A JVM started without the flag refuses to load these classes. These modules run on Java 25 only: a Java 26 JVM refuses their classes, even with the flag. `parquetry-core` alone has no such classes, and also runs on Java 26.
- `--enable-native-access=ALL-UNNAMED` lets Parquetry allocate its native buffers as memory mappings. Without it, the JDK prints a warning at the first mapping. [Memory](concepts/memory.md) explains why Parquetry maps its buffers.

Give the same flags to the JVM running your tests, for example in the `argLine` of the Maven Surefire plugin.

## Add the dependency

Import the Parquetry BOM, then declare the modules needed by your code without a version. `parquetry-core` reads and writes single Parquet and GeoParquet files:

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

The BOM pins the versions of the Parquetry modules. To also pin the third-party libraries used by Parquetry, such as tileverse-storage and JTS, import `io.tileverse.parquetry:parquetry-dependencies` the same way. [Modules](reference/modules.md) lists the published modules and the two BOMs.

## Read a file

The example reads the sample file of the GeoParquet specification, a few countries with their population and their geometry. Download it as `countries.parquet`:

```bash
curl -L -o countries.parquet https://github.com/opengeospatial/geoparquet/raw/v2.0.0/examples/example.parquet
```

The program prints the countries with at least 100 million inhabitants:

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

It prints `United States of America: 3.28239523E8`. The example follows the usual shape of a read:

- `ByteRangeSource.ofFile` opens the file. The source owns the open file, and closing it releases the file.
- `ParquetFileReader.open` reads the footer of the file. The reader is safe to share between threads.
- `read` takes three arguments: a predicate selecting the rows, a projection selecting the columns, and the read options. It returns a lazy stream of rows. Close the stream when you are done with it, to release the buffers of the read.
- The predicate is evaluated inside the read. Parquetry uses it to skip the row groups and pages that cannot match, and returns only the matching rows. A row group is a block of rows of the file, and a page is a smaller run of values of one column inside it.

To read the whole file, pass `Predicate.ALWAYS_TRUE` and `Projection.ALL`.

Read the values of a row while the stream hands it to you. A row is a view over a batch of decoded rows, and the batch closes as the stream moves on. The typed `get` accessors, such as `getString` and `getDouble`, return values that you can keep. Call `detach()` on a row to keep the row itself.

## Where to go next

- [Why Parquetry?](why.md) describes the deployment targeted by Parquetry.
- [Read records](guides/read-records.md) reads rows into your own types and chooses the columns.
- [Filter rows](guides/filter.md) builds predicates, counts rows and shows what a filter skips.
- [Write files](guides/write.md) writes Parquet and GeoParquet files.
- [Read GeoParquet](guides/geoparquet.md) reads geometries as JTS objects, with their coordinate reference system.
- [Filter by location](guides/spatial-filter.md) filters rows by bounding box and by geometry.
- [Cloud storage](guides/cloud-storage.md) reads files from HTTP servers and object storage.
- [Multi-file datasets](guides/datasets.md) reads a directory or a bucket prefix of files as one dataset.
- [Arrow interop](guides/arrow.md) exports a read as an Arrow IPC stream or through the Arrow C Data Interface.
- [JSON](guides/json.md) renders the rows of a read as JSON.
- [The GeoTools data store](geoserver/geotools-datastore.md) serves GeoParquet files as GeoTools features, in GeoServer or in your own service.

## Under the hood

- [The read path](concepts/read-path.md) follows a filtered read from the footer to the rows.
- [Spatial filtering](concepts/spatial-filtering.md) explains how a bounding box or geometry filter skips data.
- [Memory](concepts/memory.md) explains the memory budgets and how they follow a container limit.
