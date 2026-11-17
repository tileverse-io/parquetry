# Architecture

This page shows how Parquetry is built, in seven C4 diagrams, from the systems around it down to the classes of a read. It is for developers who read or change the code. The diagrams name real modules and types: open a type in the source to see the details. Select a diagram to open it at full size.

The diagrams are generated from a Structurizr model kept in the repository, under `docs/structurizr/`. In the diagrams, brown boxes are parts of Parquetry, grey boxes are other systems, and lighter brown boxes with a dashed border are experimental modules.

## System context

[![System context](../assets/images/structurizr/structurizr-SystemContext.svg)](../assets/images/structurizr/structurizr-SystemContext.svg)

Parquetry is a set of Java libraries, used by Java applications and by GeoServer. GeoServer and GeoServer Cloud reach Parquetry through its GeoTools data store. Parquetry does not talk to object storage itself: it reads byte ranges, lists files and writes files through tileverse-storage. A read can also be handed as Apache Arrow to an analytics engine.

## Containers

[![Containers](../assets/images/structurizr/structurizr-Containers.svg)](../assets/images/structurizr/structurizr-Containers.svg)

The containers are the Maven modules of Parquetry, and the arrows are the dependencies declared in their POMs. `parquetry-core` reads and writes single files, over three foundation modules: `parquetry-io`, `parquetry-format` and `parquetry-compression`. `parquetry-catalog` reads many files as one dataset, and the integration modules build on it. `parquetry-io` holds the adapter to tileverse-storage. `parquetry-core`, the Iceberg, STAC and GeoTools modules and `par` also use tileverse-storage types directly, and the diagram leaves those arrows out to stay readable. [Modules](../reference/modules.md) lists the modules with their status.

## Components of parquetry-core

[![Components of parquetry-core](../assets/images/structurizr/structurizr-CoreComponents.svg)](../assets/images/structurizr/structurizr-CoreComponents.svg)

The diagram shows the read pipeline and the writer of one file. `ParquetFileReader` opens a file and decodes its footer once. `ReadPlanner` runs the filter pipeline of a query on the metadata of the file. `FilterPipeline` runs one class per tier: STATS, SPATIAL, COLUMN_INDEX and BLOOM_FILTER. The fifth tier, RECORD_LEVEL, runs later, during the decode, in `VectorizedPredicateEvaluator`. The fetch classes read the byte ranges of the surviving row groups through `parquetry-io`. The decode classes turn their pages into batches. `BatchPipeline` hands the batches, or records built by a `Materializer`, to the caller. `ParquetFileWriter` writes row groups, the page index, the bloom filters and the footer. [The read path](../concepts/read-path.md) explains the same classes step by step.

## Components of parquetry-catalog

[![Components of parquetry-catalog](../assets/images/structurizr/structurizr-CatalogComponents.svg)](../assets/images/structurizr/structurizr-CatalogComponents.svg)

A `DatasetCatalog` gives access to datasets by name. `FilesetCatalog` is the catalog over a listing of files: it builds one dataset merging the files, or one dataset per file. It reads the footers of the files on virtual threads, and parses Hive partition directories such as `year=2024/`. A `FilesetDataset` prunes its files before a query, then reads the surviving files several at a time. It reads a file through a `ParquetSource`, over the reader of `parquetry-core`. The Iceberg and STAC modules provide their own catalogs and datasets behind the same two interfaces. [Multi-file datasets](../guides/datasets.md) shows how to use them.

## Components of parquetry-geotools

[![Components of parquetry-geotools](../assets/images/structurizr/structurizr-GeoToolsComponents.svg)](../assets/images/structurizr/structurizr-GeoToolsComponents.svg)

GeoServer opens the store through `GeoParquetDataStoreFactory`. The factory lists the files and opens a `FilesetCatalog` over them. `CatalogDataStore` publishes one feature type per dataset, and `CatalogFeatureSource` serves the features, the count and the bounds of one of them. `QueryTranslator` and `FilterToPredicate` turn a GeoTools query into a predicate, the columns to decode, and the filter left to GeoTools. `DatasetFeatureReader` builds the features from the rows of the read. `ScreenMapReadProbe` passes the pixels already drawn on a map to the read. `GeoParquetExporter` writes features as a GeoParquet file. [The GeoTools data store](../geoserver/geotools-datastore.md) describes the store from the user side.

## A filtered read

[![A filtered read](../assets/images/structurizr/structurizr-FilteredRead.svg)](../assets/images/structurizr/structurizr-FilteredRead.svg)

The numbers give the order of the calls in one filtered read of a file. Steps 1 to 11 run inside the `read` call: the filter pipeline prunes the row groups and pages on the metadata, and the read returns a lazy stream. Steps 12 to 20 run when the caller pulls the first rows. The decode asks for the bytes of a row group, the fetch reads the byte ranges of its surviving pages from storage, and the masked scan evaluates the predicate while it decodes. [The read path](../concepts/read-path.md) follows the same steps in more detail.

## A GeoServer request

[![A GeoServer request](../assets/images/structurizr/structurizr-GeoServerRequest.svg)](../assets/images/structurizr/structurizr-GeoServerRequest.svg)

A WMS GetMap or a WFS GetFeature request asks the store for the features of a layer. The feature source translates the filter of the request into a predicate, and opens a reader with it. For a map, it also passes the drawn pixels to the read. The dataset of `parquetry-catalog` prunes its files and reads the surviving ones with `parquetry-core`. The reads fetch only the byte ranges left after filtering, through tileverse-storage. GeoServer then iterates over the features, one per matching row.
