# STAC GeoParquet collections

!!! warning "Experimental"
    `parquetry-stac` and its GeoTools store work, but their API, options and behavior may change between releases.

You have a STAC catalog whose items point at GeoParquet files, and want to query a collection as one table from Java or publish it in GeoServer. `parquetry-stac` reads the collections of the catalog as datasets, one per collection, and skips the files of the items outside a spatial filter without opening them. A STAC catalog (SpatioTemporal Asset Catalog) is a tree of JSON documents: a catalog links to collections, a collection links to items, and an item has a bounding box and assets, the links to its files.

It needs `parquetry-stac`, with the BOM imported as in [Getting started](../getting-started.md#add-the-dependency):

```xml
<dependency>
  <groupId>io.tileverse.parquetry</groupId>
  <artifactId>parquetry-stac</artifactId>
</dependency>
```

Like `parquetry-catalog`, it uses Java 25 preview features: start the JVM with `--enable-preview` ([Getting started](../getting-started.md#requirements)).

## Read a collection

The catalog `catalog.json` has one collection, `building`, with two items. The item `item-west` has the bounding box `[0, 0, 10, 10]` and points at `building/parts/west.parquet`. The item `item-east` has the bounding box `[100, 0, 110, 10]` and points at `building/parts/east.parquet`. Each file holds two points. The program counts the points of the collection inside a box around the east item, and prints the plan of the read:

```java
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.stac.ContainerStorages;
import io.tileverse.parquetry.stac.StacCatalogOptions;
import io.tileverse.parquetry.stac.StacDatasetCatalog;
import io.tileverse.stac.JsonStacReader;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;

public class ReadStacCollection {

    public static void main(String[] args) {
        URI catalogUri = Path.of("catalog.json").toAbsolutePath().toUri();
        Bbox window = Bbox.of2d(95, -5, 115, 15);
        Predicate filter = new Predicate.Spatial.BboxIntersects(ColumnPath.of("geometry"), window);

        try (ContainerStorages storages = new ContainerStorages(new Properties());
                StacDatasetCatalog catalog = StacDatasetCatalog.open(
                        catalogUri, storages, new JsonStacReader(), StacCatalogOptions.defaults())) {
            System.out.println("collections: " + catalog.datasets());
            ParquetDataset buildings = catalog.dataset("building");
            long matching = buildings.count(filter, ReadOptions.DEFAULTS);
            System.out.println(matching + " matching rows");

            DatasetExplainPlan plan = buildings.explain(filter, Projection.ALL, ReadOptions.DEFAULTS);
            System.out.println(plan.toAsciiTable());
        }
    }
}
```

It prints, with the plan cut after its summary:

```
collections: [building]
2 matching rows
predicate: BboxIntersects[col=ColumnPath[dot=geometry], bbox=Bbox[minX=95.0, minY=-5.0, maxX=115.0, maxY=15.0, minZ=0.0, maxZ=0.0, is3d=false]]
1 kept, 1 skipped
```

The list of files in the plan shows `west.parquet` skipped, with the reason `geometry row-group bbox excludes the query`, and `east.parquet` kept.

- `StacDatasetCatalog.open` reads the catalog and collection documents, and stops there. The item documents are read when a query needs them, and a GeoParquet file is opened on its first read.
- The catalog has one dataset per collection, named after the collection id. The dataset has the read methods of [Multi-file datasets](../guides/datasets.md): `read`, `readBatches`, `count`, `bounds` and `explain`.
- The bounding box of an item becomes the bounds of its file. A file whose item box lies outside a spatial filter is skipped before its footer is read. In a kept file, the read skips row groups and pages as in a single file.
- `ContainerStorages` opens one tileverse `Storage` per directory of documents or files, with the backend selected by its URI. A catalog served over HTTP can point at files in S3. The `Properties` give the credentials and settings of the backends ([Cloud storage](../guides/cloud-storage.md#choose-a-backend)).
- Closing the catalog closes the files and the storages.

## Read a stac-geoparquet table of items

A stac-geoparquet file stores STAC items as the rows of a Parquet file. `GeoParquetStacReader` reads such a file as the index of a catalog, in place of the JSON documents:

```java
import io.tileverse.parquetry.stac.GeoParquetStacReader;

URI items = URI.create("s3://my-bucket/stac/items.parquet");
try (ContainerStorages storages = new ContainerStorages(new Properties());
        StacDatasetCatalog catalog = StacDatasetCatalog.open(
                items, storages, new GeoParquetStacReader(), StacCatalogOptions.defaults())) {
    System.out.println(catalog.datasets());
}
```

The reader takes the `id`, `bbox`, `assets`, `collection` and `datetime` columns of the items, and leaves the geometry and the property columns undecoded. The `stac-geoparquet` metadata in the footer of the file declares the collections. A collection named in the `collection` column but not declared gets its extent from the bounding boxes of its items.

## Options

`StacCatalogOptions.builder()` sets two options:

- `parquetMediaTypes` sets the media types of the assets read as GeoParquet files. The default is `application/vnd.apache.parquet`, `application/x-parquet` and `application/parquet`. An asset without a media type counts when its link ends in `.parquet`.
- `geometryColumn` names the geometry column of the collections, `geometry` by default. The bounding box of an item skips its file under a filter on that column.

## What it reads

- A static catalog of JSON documents, local or remote, or a stac-geoparquet table of items.
- A catalog with a `latest` property naming one of its child catalogs: only that child is read, as in a catalog of releases.
- Item bounding boxes in 2D or 3D. A 3D box skips files by its X and Y ranges.
- Items without a bounding box, or with a box in an unknown shape. Their files are always kept.

## Limits

- The module reads catalogs. It does not write them.
- It has no client for the STAC API, the search service of dynamic catalogs. It reads static catalogs and item tables.
- A stac-geoparquet table serves as an index of items with external files. Its own rows are not read as features.
- The `datetime` of the items does not skip files.
- A collection without GeoParquet assets is listed with the others. Reading it fails with an error naming the collection.

## In GeoTools and GeoServer

The GeoTools store selected by the `stac-geoparquet` parameter opens a catalog, with one feature type per collection. The parameter takes the URI of a `catalog.json`, of a directory holding one (the URI ends in `/`), or of a stac-geoparquet `.parquet` file. The `storage.*` parameters configure the backends as for [the "Parquet" store](../geoserver/geotools-datastore.md#connection-parameters), without `storage.provider`: the backend follows the URIs of the documents and files. GeoServer Cloud ships this store disabled ([Experimental parts](index.md#in-geotools-and-geoserver)).
