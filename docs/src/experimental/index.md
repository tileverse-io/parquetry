# Experimental parts

!!! warning "Experimental"
    The parts listed here work, but their API, options and behavior may change between releases.

Parquetry reads Apache Iceberg tables and STAC GeoParquet collections, and has a command line tool, `par`. These parts work and have tests, but they are labeled experimental. This page says what the label means.

## What experimental means

Parquetry has not reached 1.0, and its API may still change before then. The experimental parts change more freely than the others:

- Their Java API, their options and their output may change between releases.
- Their pages list known gaps. A gap may close, or the behavior around it may change.

The stable parts are listed on the [home page](../index.md#status).

## The experimental parts

- [Apache Iceberg tables](iceberg.md): `parquetry-iceberg` reads a table, or a warehouse of tables, as datasets.
- [STAC GeoParquet collections](stac.md): `parquetry-stac` reads the collections of a STAC catalog as datasets.
- [The `par` command line tool](../cli/index.md) inspects, filters and copies files.

## In GeoTools and GeoServer

The GeoTools module of Parquetry registers a STAC GeoParquet store and an Iceberg store next to the "Parquet" store ([The other stores of the module](../geoserver/geotools-datastore.md#the-other-stores-of-the-module)). A system property set to `true` hides a store: `parquetry.geotools.stac-geoparquet.disabled` or `parquetry.geotools.iceberg.disabled`.

GeoServer Cloud ships both stores disabled: it sets the two properties to `true` at startup, unless they are already set. Start the JVM with `-Dparquetry.geotools.iceberg.disabled=false`, or `-Dparquetry.geotools.stac-geoparquet.disabled=false`, to turn one on ([GeoServer Cloud](../geoserver/geoserver-cloud.md#the-experimental-stores)).
