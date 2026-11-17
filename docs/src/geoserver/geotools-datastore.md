# The GeoTools data store

You build a service on GeoTools and want to serve GeoParquet files as feature sources. The `parquetry-geotools` module registers a read-only GeoTools data store, named "Parquet", which `DataStoreFinder` opens from one parameter: the location of a file or a directory. GeoServer and GeoServer Cloud publish layers through the same store.

## Open a store

Add the module to a project importing the Parquetry BOM, as in [Getting started](../getting-started.md#add-the-dependency):

```xml
<dependency>
  <groupId>io.tileverse.parquetry</groupId>
  <artifactId>parquetry-geotools</artifactId>
</dependency>
```

The module declares GeoTools as a provided dependency: the application brings its own `gt-main`. The module is compiled against GeoTools 35. GeoTools is published to the OSGeo repository, `https://repo.osgeo.org/repository/release/`: declare it in the `repositories` of your POM. A file declaring its coordinate reference system with an EPSG code also needs an EPSG authority on the classpath, such as `gt-epsg-hsql`. GeoServer includes one.

The program opens the `countries.parquet` file of [Getting started](../getting-started.md#read-a-file) and prints the countries whose geometry crosses a box over Kansas:

```java
import java.io.IOException;
import java.util.Map;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;

public class CountriesInBox {

    public static void main(String[] args) throws IOException {
        Map<String, Object> params = Map.of("geoparquet", "file:///data/countries.parquet");
        DataStore store = DataStoreFinder.getDataStore(params);
        try {
            String typeName = store.getTypeNames()[0];
            SimpleFeatureSource countries = store.getFeatureSource(typeName);

            FilterFactory filters = CommonFactoryFinder.getFilterFactory();
            Filter inBox = filters.bbox("geometry", -100, 35, -95, 40, null);
            Query query = new Query(typeName, inBox, "name", "pop_est");

            SimpleFeatureCollection features = countries.getFeatures(query);
            try (SimpleFeatureIterator iterator = features.features()) {
                while (iterator.hasNext()) {
                    SimpleFeature country = iterator.next();
                    System.out.println(country.getAttribute("name"));
                }
            }
        } finally {
            store.dispose();
        }
    }
}
```

It prints `United States of America`. The store has one layer, named after the file: `countries`. The bounding box has no coordinate reference system, and the store takes it in the coordinate reference system of the file. The read decodes the columns needed by the filter and by the two requested attributes, and none of the others.

## Connection parameters

| Parameter | Required | Meaning |
| :--- | :--- | :--- |
| `geoparquet` | yes | The URI of a Parquet file or of a directory, such as `file:///data/countries.parquet` or `s3://my-bucket/countries/`, or a glob, such as `s3://my-bucket/countries/*.parquet`. Its presence selects this store. |
| `layer-grouping` | no | For a directory: `merged` (the default) or `file`, described below. |
| `fid` | no | The column holding the feature ids ([Feature ids](#feature-ids)). |
| `namespace` | no | The namespace URI of the feature types. |
| `storage.provider` | no | The storage backend, such as `file`, `http`, `s3`, `azure` or `gcs`. By default, the scheme of the location selects it. |
| `storage.caching.enabled` | no | Whether the store keeps the byte ranges read from remote storage in memory. On by default since Parquetry 1.0-RC6, off before. Local files have no byte-range cache. |

The other `storage.*` parameters hold the credentials and the settings of the backends, such as `storage.s3.region`. The tileverse guides [Authenticate](https://tileverse.io/storage/how-to/authenticate/) and [Use each backend](https://tileverse.io/storage/how-to/use-each-backend/) list them. The S3, Azure and Google Cloud Storage backends are separate modules: add `tileverse-storage-all`, or the module of one backend, as in [Cloud storage](../guides/cloud-storage.md#choose-a-backend). The parameter list of the store shows the backends found on the classpath.

A Parquet file without geometry columns opens too, as a layer without geometry.

### Directories and layers

A single file is always one layer. A directory gives layers by the `layer-grouping` parameter:

- `merged` reads the `.parquet` files under the directory, at any depth, as one layer. The files must have the same schema. A tree of Hive partitions, with directories such as `year=2024/`, fits this mode, and its partition values become columns ([Multi-file datasets](../guides/datasets.md#skip-partitions)).
- `file` publishes the `.parquet` files at the top of the directory as layers of their own, named after their files. The files may have different schemas, as in a directory holding one file per theme. This mode does not look into subdirectories: a tree of Hive partitions has no file at its top, and opening it fails with "no files found".

A store over files with different schemas in `merged` mode fails when a layer is first read, and the error names the `layer-grouping` parameter.

## Filters pushed into the read

The store translates the filter of a GeoTools query into a Parquetry predicate, and the read evaluates it. The read skips the row groups (blocks of rows of a file) and the pages (runs of values of one column inside a row group) unable to match, before fetching them ([Selective queries read little](../index.md#selective-queries-read-little)):

- **Spatial filters** are pushed exactly: `BBOX`, `Intersects`, `Contains`, `Within`, `Crosses`, `Overlaps`, `Touches`, `Disjoint`, `Equals`, and `DWithin` without distance units. The read tests the geometries with JTS, and GeoTools does not test them again. The pruning uses the bounding box of the query geometry ([Filter by location](../guides/spatial-filter.md#apply-an-exact-geometry-test)). A filter with two boxes, as sent by a map renderer for a map crossing the antimeridian, prunes the two boxes separately.
- **Attribute filters** are pushed: comparisons, `BETWEEN`, `IS NULL`, and their combinations with `AND`, `OR` and `NOT`. On a `FLOAT` attribute, only equality is pushed; other comparisons stay with GeoTools.
- **Temporal filters** on a timestamp attribute, such as `AFTER`, `BEFORE` and `DURING`, are pushed as comparisons with the instant or the bounds of the period.
- **Other filters**, such as `LIKE` and functions, stay with GeoTools, which tests the features returned by the read.

A spatial filter must be in the coordinate reference system of the data: the store rejects a geometry in another one. GeoServer reprojects the filters of its requests before they reach the store.

`DWithin` is pushed when it has no distance units, and its distance is then in the units of the data. A `DWithin` with units stays with GeoTools, which tests the features returned by the read. A query keeping such a filter is counted and bounded by reading its features, and gets no skipping of drawn pixels ([Maps at small scales](#maps-at-small-scales)).

The store also pushes the rest of the query:

- The read decodes the columns of the requested attributes, plus the columns needed by the filters left to GeoTools.
- A feature limit (`maxFeatures`) stops the read once enough features are returned.
- When the whole filter is pushed, a count comes from the read without building features, and the bounds of a query come from the statistics of the files when they have them.

### Maps at small scales

When GeoServer draws a map at a small scale, many features fall on the same pixel. The renderer passes the store a map of the pixels already drawn, and the store gives it to the read when the whole filter is pushed. The read skips the files, row groups and pages smaller than a pixel whose bounding box falls on drawn pixels, without fetching them. It drops the features smaller than a pixel that land on a drawn pixel. When the map needs the geometry alone, a small part of a file lying inside the query may be drawn as one pixel-sized shape instead of being fetched.

## Feature ids

The store takes the feature ids from:

1. The column named by the `fid` parameter, when set. It must be a string, numeric or UUID column, not a geometry, or the store fails to open the layer.
2. Otherwise, a column named `id`, when the file has one of those types.
3. Otherwise, sequence numbers given to the features by the read.

With a feature id column, `Id` filters work, and the read uses them to skip row groups and pages. Sequence numbers change between reads: the store rejects an `Id` filter on such a layer with an error, rather than returning wrong features.

## Attribute types

| Parquet column | Attribute type |
| :--- | :--- |
| `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE` | `Boolean`, `Integer`, `Long`, `Float`, `Double` |
| `DATE` | `LocalDate` |
| `TIMESTAMP`, adjusted to UTC or not | `Instant` or `LocalDateTime` |
| `TIME` | `LocalTime` |
| `DECIMAL` | `BigDecimal` at the scale of the column; `byte[]` for a `DECIMAL` stored as `BYTE_ARRAY` |
| `BYTE_ARRAY` holding strings, or other bytes | `String` or `byte[]` |
| `UUID` | `UUID` |
| A struct, a map or a list | `Map` or `List` |
| A geometry column | a JTS geometry, in the coordinate reference system declared by the file |

An `INT96` column, the legacy timestamp of Spark and Impala files, is left out of the feature type.

The `bbox` covering column of a GeoParquet file is not an attribute. A geometry column without a declared coordinate reference system is read as `OGC:CRS84`, the GeoParquet default. A coordinate reference system unknown to GeoTools leaves the attribute without one, and the store logs it.

A timestamp without a time zone is read as UTC. The store registers a GeoTools converter for the `java.time` types: filters can compare these attributes with a `Date`, an ISO 8601 text or an OGC temporal literal.

## Runtime requirements

The store reads its files through `parquetry-catalog`, compiled with Java 25 preview features. Run GeoTools or GeoServer on Java 25, with `--enable-preview --enable-native-access=ALL-UNNAMED`. A JVM of another version, or started without `--enable-preview`, cannot load these classes. [Getting started](../getting-started.md#requirements) explains both flags.

## The other stores of the module

The module registers two more stores. The STAC GeoParquet store, selected by the `stac-geoparquet` parameter, opens a STAC `catalog.json` or a stac-geoparquet table of items. The Apache Iceberg store, selected by the `iceberg` parameter, opens an Iceberg table or a warehouse of tables. Both are experimental, and GeoServer Cloud ships them disabled. [STAC GeoParquet collections](../experimental/stac.md) and [Apache Iceberg tables](../experimental/iceberg.md) describe what they read.

A system property set to `true` hides a store from `DataStoreFinder`: `parquetry.geotools.geoparquet.disabled`, `parquetry.geotools.stac-geoparquet.disabled` or `parquetry.geotools.iceberg.disabled`.

## Write features as GeoParquet

`GeoParquetExporter.export(features, out, options)` writes a GeoTools feature collection to an output stream as a GeoParquet file. The `geoparquet` WFS output format of the GeoServer plugin uses it. [Write files](../guides/write.md) describes the write options.

## Under the hood

- [Spatial filtering](../concepts/spatial-filtering.md) explains how a spatial filter prunes and tests rows.
- [The read path](../concepts/read-path.md) follows a filtered read from the footer to the rows.
- [Memory](../concepts/memory.md) explains the memory budgets shared by the layers of a server.
