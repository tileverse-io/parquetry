# GeoTools and GeoServer

You run GeoServer or GeoServer Cloud and want to publish GeoParquet files as map layers, or you build a service on GeoTools and want to read them. Parquetry provides a GeoTools data store for both. GeoServer Cloud 3.1.0 and newer include it and enable it by default.

## Pick your setup

<div class="grid cards" markdown>

-   **GeoServer Cloud**

    ---

    Publish GeoParquet files as layers through the REST API, and download any vector layer as GeoParquet or Arrow IPC through WFS.

    [GeoServer Cloud](geoserver-cloud.md)

-   **GeoServer**

    ---

    Install the plugin, then create a "Parquet" store from the web interface and publish its layers.

    [The GeoServer plugin](geoserver-plugin.md)

-   **A GeoTools application**

    ---

    Open the store with `DataStoreFinder` and query its feature sources from Java.

    [The GeoTools data store](geotools-datastore.md)

-   **Pods in Kubernetes**

    ---

    Choose the CPU and memory of a pod serving Parquetry layers.

    [Size a GeoServer pod](pod-sizing.md)

</div>

## What a layer gets

A layer served by the Parquetry store reads its files from a local volume, an HTTP server, Amazon S3, Azure Blob Storage or Google Cloud Storage. A file, a directory of files or a tree of Hive partitions can be one layer, and a directory can also give one layer per file.

The store pushes the filters of map and WFS requests into the read. A Parquet file stores its rows in blocks called row groups, and the values of a column inside a row group in pages. The read skips the row groups and pages unable to match the filter, and fetches the rest with byte-range requests. At small map scales, it also skips the parts of the files landing on pixels already drawn.

The reads of the layers share memory budgets sized from the memory limit of the container. A full budget slows a read down instead of killing the server ([Why Parquetry?](../why.md)).

## Prepare the files

The store reads any Parquet file. GeoParquet files sorted by location, with a page index and bounding box columns, filter best. Parquetry's writer and the `par` command line tool produce them ([Lay out files for selective queries](../guides/file-layout.md)).

## Status

The "Parquet" store is stable. The module also has a STAC GeoParquet store and an Apache Iceberg store, both experimental ([The other stores of the module](geotools-datastore.md#the-other-stores-of-the-module)).
