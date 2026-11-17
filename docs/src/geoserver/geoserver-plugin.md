# The GeoServer plugin

You run GeoServer and want to publish GeoParquet files as map layers. The Parquetry plugin, from the [tileverse-geoserver](https://github.com/tileverse-io/tileverse-geoserver) project, adds the Parquetry stores to GeoServer. Once it is installed, "Parquet" appears among the vector data sources of the new store page, and a layer published from it shows in the layer preview.

GeoServer Cloud 3.1.0 and newer include Parquetry, with the same stores and output formats: see [GeoServer Cloud](geoserver-cloud.md) instead.

## Install the plugin

The plugin needs GeoServer running on Java 25, started with `--enable-preview --enable-native-access=ALL-UNNAMED` ([Runtime requirements](geotools-datastore.md#runtime-requirements)).

The [releases of tileverse-geoserver](https://github.com/tileverse-io/tileverse-geoserver/releases) provide one plugin zip per GeoServer series. The notes of a release name the GeoServer version and the Parquetry version included in it. Unzip the plugin into `webapps/geoserver/WEB-INF/lib` and restart GeoServer. A release also has a demo zip: a GeoServer with the plugin and sample data, needing only Docker.

The [tileverse-geoserver README](https://github.com/tileverse-io/tileverse-geoserver#readme) describes the build from source and the Maven coordinates.

## What the plugin adds

The data access code is the [GeoTools data store](geotools-datastore.md) of Parquetry. The plugin adds what GeoServer needs around it:

- **Store pages.** The page of a store shows the storage settings: a choice of backend, the fields of the chosen backend, and secrets masked.
- **WFS output formats.** `geoparquet` and `arrow-ipc` download the result of a WFS `GetFeature` request as a GeoParquet file or as an Arrow IPC stream, for any vector layer of GeoServer.
- **Module status.** The stores and output formats have their own rows under About, Server Status, Modules.

Three more features are merged for the next plugin release, the first one built with Parquetry 1.0-RC6:

- **The byte-range cache on the store page.** The page shows the memory cache of the byte ranges read from remote storage, on by default.
- **A file browser.** A "Browse..." button next to the location lists one folder of the storage at a time. For a `.parquet` file it shows the rows, the row groups (blocks of rows of the file), the geometry column, the coordinate reference system and the GeoParquet version.
- **Cache reset.** A reset or a reload of GeoServer clears the caches of Parquetry: the file footers, the page indexes and the byte ranges.

The plugin is licensed under the GNU General Public License, version 2 or later, as GeoServer is. Parquetry itself is licensed under the Apache License, version 2.0.

## The stores

The plugin offers three stores:

- **Parquet** publishes Parquet and GeoParquet files and directories. It is the stable store of the family.
- The **Iceberg** and **STAC** stores read Apache Iceberg tables and STAC GeoParquet collections. They are experimental.

A system property hides a store from GeoServer ([The other stores of the module](geotools-datastore.md#the-other-stores-of-the-module)).

GeoServer also has a GeoParquet community module, based on DuckDB, named "GeoParquet" in the store list. The two are separate stores and can be installed together. The Parquetry store is the one named "Parquet".

## Where to go next

- [The GeoTools data store](geotools-datastore.md) lists the connection parameters and the filters pushed into the read.
- [Size a GeoServer pod](pod-sizing.md) gives the memory and CPU of a pod serving Parquetry layers.
- [Lay out files for selective queries](../guides/file-layout.md) describes the files that filter best.
