# GeoServer Cloud

You run GeoServer Cloud 3.1.0 or newer and want to publish GeoParquet files as layers, or to hand layers to an analytics tool. GeoServer Cloud includes Parquetry and enables it by default: a store created through the REST API gives layers that you can see in the layer preview, and WFS downloads any vector layer as a GeoParquet file or an Arrow IPC stream.

The GeoServer Cloud user guide covers both tasks step by step. This page says what they give you, and where Parquetry's own pages help.

## What is enabled

In GeoServer Cloud 3.1.0 and newer, these are enabled by default:

- the Parquetry extension and its "Parquet" data store, which reads GeoParquet files;
- the `geoparquet` and `arrow-ipc` output formats of WFS `GetFeature`.

The STAC GeoParquet and Iceberg stores of Parquetry are not ready for production yet, and ship disabled ([The experimental stores](#the-experimental-stores)).

## Publish GeoParquet files as layers

The guide [GeoParquet through the REST API](https://geoserver.org/geoserver-cloud/user-guide/geoparquet-rest-api/) creates a workspace, a "Parquet" store and its layers with `curl`. The store points at a file, a directory or a glob of files, on a local volume, an HTTP server, Amazon S3 or an S3-compatible service, Azure Blob Storage or Google Cloud Storage. Its layers are read-only.

The store has the parameters of [the GeoTools data store](geotools-datastore.md#connection-parameters): `geoparquet` for the location, `layer-grouping` to publish a directory as one layer or one layer per file, `fid` for the feature ids, and the `storage.*` parameters of the backend. The byte ranges read from remote storage are cached in memory by default.

The filters of WMS and WFS requests on these layers run inside the read, which skips the parts of the files unable to match ([Filters pushed into the read](geotools-datastore.md#filters-pushed-into-the-read)). Files sorted by location, with a page index and bounding box columns, filter best ([Lay out files for selective queries](../guides/file-layout.md)).

## Download layers as GeoParquet or Arrow IPC

The guide [GeoParquet and Arrow IPC over WFS](https://geoserver.org/geoserver-cloud/user-guide/wfs-geoparquet-arrow-ipc/) downloads layers with the `geoparquet` and `arrow-ipc` WFS output formats, and reads them with DuckDB, pyarrow and GeoPandas.

These formats apply to any vector layer, whatever its store: PostGIS, shapefiles, GeoPackage or GeoParquet. The WFS request filters the features in that store, and the download holds the result. An analytics engine such as DuckDB then receives columnar input, already narrowed to the features and attributes of the request. Parquetry is not an analytics engine itself; [Feed an analytics engine](../guides/arrow.md#feed-an-analytics-engine) explains the split.

## Next to the DuckDB-based store

GeoServer also has a GeoParquet community module, based on DuckDB, named "GeoParquet". The Parquetry store is independent of it, and the two can run side by side in GeoServer Cloud. [Why Parquetry?](../why.md#background) tells why Parquetry was started for this deployment.

## The experimental stores

The STAC GeoParquet and Iceberg stores are hidden while the system properties `parquetry.geotools.stac-geoparquet.disabled` and `parquetry.geotools.iceberg.disabled` are `true`. GeoServer Cloud sets both at startup, unless they are already set. Start the JVM with `-Dparquetry.geotools.iceberg.disabled=false`, or the STAC GeoParquet equivalent, to turn one of them on.

## Size the pods

Parquetry sizes the memory of its reads from the memory limit of the pod, and shares it between the layers. [Size a GeoServer pod](pod-sizing.md) gives two pod profiles and the settings beside them.
