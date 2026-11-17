# Why Parquetry?

A Java application can read Parquet in two ways besides Parquetry. parquet-java is the Java implementation maintained by the Apache Parquet project. DuckDB, a native analytics engine, reads Parquet and has a JDBC driver for Java. Parquetry is a Parquet reader and writer written in Java: small enough to add to any application, and built to keep its memory within the limits of a container. This page compares the three, lists the applications Parquetry is built for, and says when another tool fits better.

## What each option adds to an application

The table counts the jars and the size added to an application by each option, with their runtime dependencies. The versions are the latest releases on Maven Central in November 2026.

| Dependencies | Jars | Size |
| :--- | ---: | ---: |
| `parquetry-core` 1.0-RC6 | 20 | 12 MB |
| `parquetry-core` 1.0-RC6 and `tileverse-storage-s3` 2.1.2, to read S3 | 57 | 44 MB |
| `parquet-hadoop` 1.18.2, `hadoop-common` and `hadoop-mapreduce-client-core` 3.5.0 | 153 | 88 MB |
| The same, and `hadoop-aws` 3.5.0, to read S3 | 157 | 746 MB |
| `duckdb_jdbc` 2.0.0.1 | 1 | 76 MB |

**parquet-java needs Hadoop, even for a local file.** Its file reader lives in the `parquet-hadoop` module, and its classes refer to Hadoop types. `parquet-hadoop` declares Hadoop as a `provided` dependency, and the application has to add it. A read of a local file through the `LocalInputFile` class of parquet-java 1.18.2 does not compile without `hadoop-common`, and fails with a `NoClassDefFoundError` without `hadoop-mapreduce-client-core`. parquet-java reads S3 through the S3A file system of Hadoop, in `hadoop-aws`, which depends on the AWS SDK bundle, a single jar of 654 MB.

**DuckDB runs a native engine inside the Java process.** Its JDBC driver is one jar holding the DuckDB library for each supported platform. The engine allocates its memory outside the Java heap, and its `memory_limit` setting bounds the buffers of one engine instance.

**Parquetry decodes Parquet in the JVM.** It depends on neither Hadoop nor parquet-java, and it runs no native engine. It reads files through [tileverse-storage](https://tileverse.io/storage/), and an application adds the tileverse-storage module of its cloud provider to read object storage.

## The audience

Parquetry is built for these applications:

- **Java applications of any kind.** A server, a desktop application or a command line tool adds Parquetry as a library. Geometries are [JTS](https://github.com/locationtech/jts) geometries.
- **Long-lived servers.** A service runs for days and answers many requests at the same time. A reader is opened once and shared by concurrent requests.
- **Containers with memory limits.** A Java service runs in a pod with a few gigabytes of memory, and a scheduler sets its memory limit.
- **Files on object storage.** Parquet and GeoParquet files sit on Amazon S3, Azure Blob Storage, Google Cloud Storage or an HTTP server. A request needs a small part of a file, and the reader fetches it with byte-range requests.

Servers and desktop applications use it today. GeoServer and GeoServer Cloud are servers: a [GeoTools data store](geoserver/geotools-datastore.md) publishes GeoParquet files as map layers. JOSM, the Java editor of OpenStreetMap, is a desktop application: the [JOSM GeoParquet](https://github.com/geoverse-dev/josm-geoparquet) plugin shows local and remote GeoParquet files beside the OpenStreetMap data, and the [JOSM Overture Maps](https://github.com/overturemaps-ar-community/josm-overturemaps) plugin loads Overture Maps data through the GeoParquet plugin.

If your workload does not share these constraints, Parquetry may not be the right tool: see [When not to use Parquetry](#when-not-to-use-parquetry).

## The constraints of a container

**The container limit counts all memory.** The JVM heap limit (`-Xmx`) covers only the heap. The container limit counts the heap, the native memory of the process, and everything else. When the process crosses the container limit, the kernel kills it with a signal. No `OutOfMemoryError` is thrown, no request gets an error response, and the requests in flight are lost while the container restarts.

**Memory outside the JVM is sized by hand.** A native engine in the process, or an external process such as a Python worker, has a memory pool of its own. The JVM does not see that pool, and an operator has to split the container memory between the heap and the pool.

**One limit for the whole process.** The container enforces one limit on the sum of the memory used by the datasets read by the process. A limit set per dataset does not bound that sum. An operator would have to split the container memory between the datasets by hand, and redo the split when a dataset is added.

**The pod size changes.** A scheduler can give pods different memory limits. A setting computed by hand for a 4 GB pod is too large for a 2 GB pod and wastes most of a 16 GB pod.

**Many requests read the same files at once.** A request must not hold its own copy of the file metadata. A reader that parses the footer of a file once per request multiplies its memory by the number of concurrent requests.

**Remote bytes are slow and cost money.** A request for a small area of a large file must not download the file. The reader must decide which parts of the file it needs before it fetches them.

## What Parquetry does about them

**It sizes its memory from the container limit.** At startup, Parquetry reads the memory limit of the container and derives its native memory budgets from the memory left beside the heap. Readers share these budgets. Adding a dataset adds nothing to tune, and a pod of another size gets budgets scaled to its own limit. [Memory](concepts/memory.md) describes the budgets and their defaults.

**A full budget slows a read down instead of killing the process.** When the fetch budget is full, a read waits for other reads to release room, up to one second by default. If no room frees up in that time, the read writes its buffers to a memory-mapped temporary file. When the disk budget is full too, the read fails with an exception, and the other requests keep running. [Native memory and spill](concepts/native-memory-and-spill.md) explains the spill path.

**It decodes Parquet in the JVM.** A dataset does not get an engine instance with a memory pool of its own, and no memory sits outside the budgets of Parquetry.

**It shares file metadata between readers.** A reader parses the footer of a file once and keeps a compact form of it. Readers opened on the same file share that form, and a reader is safe to share between threads.

**It fetches only the byte ranges needed by a query.** Parquetry discards the row groups and pages that cannot match the [filter](guides/filter.md), using column statistics, GeoParquet bounding boxes, page indexes and bloom filters. It fetches the remaining data with byte-range requests through tileverse-storage, from a local disk, an HTTP server or object storage ([Cloud storage](guides/cloud-storage.md)). [The read path](concepts/read-path.md) walks through a filtered read, and [Spatial filtering](concepts/spatial-filtering.md) covers bounding box and geometry filters.

## Background

Parquetry started as a GeoServer data store. Its author also wrote the DuckDB-based [GeoParquet store](https://docs.geoserver.org/3.0.x/en/user/community/geoparquet/) for GeoTools and GeoServer. That store still exists, and GeoServer Cloud runs it side by side with the Parquetry store. DuckDB 2.0 bounds the buffer memory of an engine instance with its `memory_limit` setting, and its threads with the `threads` setting. The documentation of the GeoParquet community module for GeoServer 3.0.x lists both settings as parameters of a store. Those limits work within one engine instance. A server with many stores runs many instances, and the operator has to split the container memory between them by hand. Parquetry was started for the deployment where one container hosts many stores and a scheduler assigns its memory limit.

## When not to use Parquetry

Pick something else when:

- **You work inside a Spark or Hadoop pipeline.** Use parquet-java, the Parquet implementation used by Spark and by the Hadoop ecosystem. Spark 4.3.0 reads Parquet through parquet-java 1.17.0.
- **You run SQL analytics over Parquet files.** Joins, aggregations and ad hoc queries belong to an analytics engine such as DuckDB. Parquetry is not a query engine. It can feed one: its [Arrow IPC output](guides/arrow.md) gives an engine columnar input, and the [`arrow-ipc` WFS output format](https://geoserver.org/geoserver-cloud/user-guide/wfs-geoparquet-arrow-ipc/) of GeoServer Cloud does the same for any vector layer.
- **You work in Python.** Use pyarrow, the Python library of Apache Arrow: pyarrow 26.0.1 reads and writes Parquet files. GeoPandas 1.2.0 reads and writes GeoParquet through pyarrow.
- **Your application runs on Java 24 or older.** Parquetry needs Java 25. The multi-file datasets and the GeoTools data store use preview features of Java 25, and run on that version only. [Getting started](getting-started.md#requirements) lists the requirements.
