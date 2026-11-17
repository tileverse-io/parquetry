# Modules

This page lists the artifacts published by Parquetry, with what each one is for and its status. Use it to choose the dependencies of your project.

The artifacts share one version number. [Getting started](../getting-started.md) gives the latest release and shows how to import the BOM.

## Artifacts

Declare the Parquetry modules called by your code. Maven resolves the modules below them, such as `parquetry-format` under `parquetry-core`.

| Group id | Artifact id | Use it to | Status |
| :--- | :--- | :--- | :--- |
| `io.tileverse.parquetry` | `parquetry-core` | Read and write single Parquet and GeoParquet files, filter rows, and decode geometries to JTS. | <span class="status-badge status-badge--stable">stable</span> |
| `io.tileverse.parquetry` | `parquetry-catalog` | Read a directory or a glob of Parquet files as one dataset. | <span class="status-badge status-badge--stable">stable</span> |
| `io.tileverse.parquetry` | `parquetry-geotools` | Serve GeoParquet files as GeoTools features, through a GeoTools data store. | <span class="status-badge status-badge--stable">stable</span> |
| `io.tileverse.parquetry` | `parquetry-arrow` | Export a read as an Arrow IPC stream or through the Arrow C Data Interface. | <span class="status-badge status-badge--stable">stable</span> |
| `io.tileverse.parquetry` | `parquetry-iceberg` | Read Apache Iceberg tables as datasets. | <span class="status-badge status-badge--experimental">experimental</span> |
| `io.tileverse.parquetry` | `parquetry-stac` | Read the GeoParquet files of a STAC collection as one dataset. | <span class="status-badge status-badge--experimental">experimental</span> |
| `io.tileverse.parquetry` | `parquetry-avro` | Read and write Avro container files, the format of Iceberg manifests and manifest lists. | supporting |
| `io.tileverse.parquetry` | `parquetry-jackson` | Render the rows of a read as JSON. | supporting |
| `io.tileverse.parquetry` | `parquetry-io` | Read byte ranges from files and from tileverse-storage, and pool the native buffers of reads. | foundation |
| `io.tileverse.parquetry` | `parquetry-compression` | Compress and decompress Parquet pages and Avro blocks. | foundation |
| `io.tileverse.parquetry` | `parquetry-format` | Decode and encode the Parquet footer and page headers, and model the schema and the GeoParquet metadata. | foundation |
| `io.tileverse.parquetry.apps` | `par` | Inspect, filter and copy files from the command line. | <span class="status-badge status-badge--experimental">experimental</span> |

- **Stable** parts are ready for production use. **Experimental** parts work, but their API and behavior may still change. The [home page](../index.md#status) explains both labels.
- **Supporting** modules have no status of their own. `parquetry-iceberg` depends on `parquetry-avro`, and the `par` tool depends on `parquetry-jackson`.
- **Foundation** modules sit under `parquetry-core`, which depends on all three. Declare `parquetry-core`, and Maven adds them.
- `parquetry-geotools` also registers the Iceberg and STAC GeoParquet stores. These two stores read through `parquetry-iceberg` and `parquetry-stac`, and are experimental like them.
- `par` is not published to Maven Central. It builds from the source repository, as [The par command line tool](../cli/index.md#build-par) shows.

## BOMs

Two BOMs keep versions in line. Import them in the `dependencyManagement` section of your POM, with `<type>pom</type>` and `<scope>import</scope>`.

| Group id | Artifact id | Pins the versions of |
| :--- | :--- | :--- |
| `io.tileverse.parquetry` | `parquetry-bom` | The Parquetry modules listed above, except `par`. |
| `io.tileverse.parquetry` | `parquetry-dependencies` | The third-party libraries used by Parquetry, such as tileverse-storage, JTS and Jackson. |

Import `parquetry-bom` alone to pin the Parquetry modules. Import `parquetry-dependencies` as well to also align the versions of the libraries shared by your application and Parquetry.

## Which module for which task

| You want to | Declare |
| :--- | :--- |
| Read or write one Parquet or GeoParquet file | `parquetry-core`, plus a tileverse-storage module for object storage ([Cloud storage](../guides/cloud-storage.md)) |
| Read many files as one dataset | `parquetry-catalog` |
| Publish GeoParquet through GeoTools or GeoServer | `parquetry-geotools` ([GeoTools and GeoServer](../geoserver/index.md)) |
| Hand rows to an analytics engine as Arrow | `parquetry-arrow` ([Arrow interop](../guides/arrow.md)) |
| Print rows as JSON | `parquetry-jackson` ([JSON](../guides/json.md)) |
| Read Avro container files | `parquetry-avro` ([Avro](../guides/avro.md)) |
| Read an Iceberg table or a STAC collection | `parquetry-iceberg` or `parquetry-stac` ([Experimental parts](../experimental/index.md)) |

Start the JVM of your application with the flags listed in [Getting started](../getting-started.md#requirements). Some modules use Java 25 preview features and fail to load without them.
