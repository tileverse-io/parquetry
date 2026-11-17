# Cloud storage

You have Parquet files on an HTTP server or in object storage, such as Amazon S3, Azure Blob Storage or Google Cloud Storage, and want to query them from Java without downloading them first. Parquetry reads them through [tileverse-storage](https://tileverse.io/storage/) and fetches only the byte ranges needed by the query. This guide reads the file of Getting started from its HTTPS address, then shows the other backends and how to write a file to object storage.

It needs `parquetry-core`, which reads local files and HTTP servers on its own. [Getting started](../getting-started.md) adds the dependency and lists the JVM flags.

## Read a remote file

The program runs the query of Getting started on the sample file of the GeoParquet specification, read from GitHub:

```java
import static io.tileverse.parquetry.filter.Pred.col;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Stream;

public class ReadRemote {

    private static final ColumnPath NAME = ColumnPath.of("name");
    private static final ColumnPath POPULATION = ColumnPath.of("pop_est");

    public static void main(String[] args) throws IOException {
        URI examples = URI.create("https://raw.githubusercontent.com/opengeospatial/geoparquet/v2.0.0/examples/");
        Predicate filter = col("pop_est").gtEq(100_000_000.0);
        Projection columns = Projection.ofPhysical(List.of(NAME, POPULATION));

        try (Storage storage = StorageFactory.open(examples);
                ByteRangeSource source = ByteRangeSource.owning(storage.openVersionedRangeReader("example.parquet"))) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(filter, columns, ReadOptions.DEFAULTS)) {
                rows.forEach(ReadRemote::printCountry);
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

It prints `United States of America: 3.28239523E8`, as the local read of Getting started does. Only the opening of the file differs:

- `StorageFactory.open` opens a tileverse `Storage` over a container: here the directory of the file on the server. Close it after the last stream of the read.
- `openVersionedRangeReader` opens one file of the container by its key, and asks the server for the version of the file, such as its ETag.
- `ByteRangeSource.owning` hands the file to Parquetry. Closing the source closes the tileverse reader.

From there, the read is the one of a local file. Parquetry reads the footer of the file, rules out the row groups and pages unable to match the filter, and fetches the rest with byte-range requests. A row group is a block of rows of the file, and a page is a smaller run of values of one column inside it.

## Choose a backend

The scheme of the URI selects the backend. Local files and HTTP come with `parquetry-core`, and the cloud backends have modules of their own:

| URI | Backend | Module |
| :--- | :--- | :--- |
| `file:` or a plain path | Local file system | included |
| `http:`, `https:` | HTTP server | included |
| `s3://my-bucket/countries/` | Amazon S3 and S3-compatible services | `tileverse-storage-s3` |
| `az://my-account/my-container/countries/`, `abfss:` | Azure Blob Storage, Azure Data Lake Storage | `tileverse-storage-azure` |
| `gs://my-bucket/countries/` | Google Cloud Storage | `tileverse-storage-gcs` |

Add the module of the backend to your dependencies. `tileverse-storage-all` adds the three. The `parquetry-dependencies` BOM, imported as in [Getting started](../getting-started.md#add-the-dependency), sets their version:

```xml
<dependency>
  <groupId>io.tileverse.storage</groupId>
  <artifactId>tileverse-storage-s3</artifactId>
</dependency>
```

The program stays the same: pass `URI.create("s3://my-bucket/countries/")` to `StorageFactory.open`. Without the module of its scheme, the open fails. `StorageFactory.open(uri, properties)` takes the credentials and the other parameters of the backend, listed by the tileverse guides [Authenticate](https://tileverse.io/storage/how-to/authenticate/) and [Use each backend](https://tileverse.io/storage/how-to/use-each-backend/).

## What Parquetry keeps between reads

Parquetry parses the footer of a file once and keeps it in a cache shared by the readers of the process. A later open of the same file reads no footer. The page indexes learned from the page headers of a file go in the same cache ([Without a page index](../concepts/read-path.md#without-a-page-index)).

The cache keys a file by its URI, its version and its length. A reader opened with `openVersionedRangeReader`, or from an entry of a listing, knows the version of the file. A reader opened with `openRangeReader(key)` has no version. If a file of that kind is rewritten under the same name at the same length, reads get the old footer until it leaves the cache.

The byte ranges of the data are kept only when the `Storage` caches them. A `Storage` opened with `storage.caching.enabled=true` caches the ranges read through its readers:

```java
import java.util.Properties;

Properties properties = new Properties();
properties.setProperty("storage.caching.enabled", "true");
Storage storage = StorageFactory.open(examples, properties);
```

The cache serves repeated reads of the same parts of a file, as made by a map server. The GeoTools data stores of Parquetry turn it on by default for remote files, and the store parameter `storage.caching.enabled=false` turns it off. `par` and the `FileSource.open` method of [Multi-file datasets](datasets.md) leave it off. The byte ranges of local files are not cached.

The tileverse guide [Caching layers](https://tileverse.io/storage/how-to/configure/#caching-layers) describes the caches and their settings. [Tune memory](tune-memory.md#size-the-caches) sizes them against the heap. A read hands the byte ranges of a row group to tileverse-storage in one call, and tileverse-storage decides how to group them into requests ([Batched reads](https://tileverse.io/storage/how-to/configure/#batched-reads)).

## Write to object storage

A `Storage` also opens an upload stream, and the writer writes the file to it:

```java
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.StorageOutputStream;
import io.tileverse.storage.WriteOptions;
import java.io.IOException;
import java.net.URI;

public class UploadCities {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath NAME = ColumnPath.of("name");

    public static void main(String[] args) throws IOException {
        URI exports = URI.create("s3://my-bucket/exports/");
        ParquetSchema schema = citySchema();

        try (Storage storage = StorageFactory.open(exports)) {
            StorageOutputStream upload = storage.openOutputStream("cities.parquet", WriteOptions.defaults());
            try (ParquetFileWriter writer = ParquetFileWriter.create(upload, schema);
                    ParquetRecordBatchBuilder rows = writer.appender()) {
                rows.setLong(ID, 1).setString(NAME, "Rosario").endRow();
                rows.setLong(ID, 2).setString(NAME, "Montevideo").endRow();
            } catch (RuntimeException e) {
                upload.abort();
                throw e;
            }
            upload.close();
        }
    }
}
```

`citySchema()` is the method of [Write files](write.md#write-a-file). The sample leaves `exports/cities.parquet` with two rows.

- **Closing the upload publishes the object.** The writer never closes the stream. The sample closes it after the writer closed without error, and the object appears complete.
- **On failure, abort.** `abort()` discards the upload, and no object appears. Closing the stream instead would publish a file without a footer, or with the rows written before the failure.
- **Two `WriteOptions`.** The `WriteOptions` of the sample are the options of the upload, from tileverse-storage. To pass Parquetry write options as well, write one of the two classes with its package name, such as `io.tileverse.parquetry.data.WriteOptions`.

The writer encodes the current row group into temporary files on the local disk, and appends it to the upload when it is complete. [Tune memory](tune-memory.md#temporary-files-of-the-writer) chooses their directory.

## Where to go next

- [Multi-file datasets](datasets.md) reads the files under a prefix of a bucket as one dataset.
- [Tune memory](tune-memory.md) sizes the memory of the reads and the caches.

## Under the hood

- [The read path](../concepts/read-path.md) follows a filtered read from the footer to the fetched pages.
