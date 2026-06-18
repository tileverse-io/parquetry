# parquetry-catalog

The dataset and catalog layer above the single-file Parquet engine. It turns a connection (a directory of files, a
glob, an Iceberg table) into one or more named, schema-bearing, queryable **datasets**, each read as a single stream
even when it spans many files. A GeoTools DataStore or any other consumer codes against this one contract and swaps the
backend (pure-parquet, Iceberg, and later Delta or STAC) without changing the read code.

It depends on `parquetry-core` (the read pipeline, predicates, schema) and `parquetry-io` (byte access). It does not
depend on any specific backend; the Iceberg backend lives in its own `parquetry-iceberg` module and implements this
module's SPI.

## Two packages

- **`io.tileverse.parquetry.catalog`** - the connection. `DatasetCatalog` is the SPI; `CatalogCapabilities` describes
  what the connection can do; `FileSourceCatalog` is the pure-parquet implementation.
- **`io.tileverse.parquetry.dataset`** - the queryable view. `Dataset` is the per-dataset facade;
  `DatasetCapabilities` describes one dataset; `ParquetDataset` reads 1..N same-schema files as one stream above the
  core `ParquetReader`.

## The SPI

```
DatasetCatalog  ──datasets()──▶  names
       │
       └──dataset(name)──▶  Dataset ──read/count/explain──▶  records
                               │
                               └──plan(predicate)──▶  FilePlan ──files()──▶  PlannedFile
```

```java
public interface DatasetCatalog extends AutoCloseable {
    CatalogCapabilities capabilities();
    List<String> datasets();
    Dataset dataset(String name);
    void close();
}

public interface Dataset {
    String name();
    ParquetSchema schema();
    Optional<CatalogSnapshot> snapshot();          // present only for versioned backends (Iceberg)
    DatasetCapabilities capabilities();
    Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options);
    <T> Stream<T> read(Predicate predicate, Projection projection, Materializer<T> materializer, ReadOptions options);
    long count(Predicate predicate, ReadOptions options);
    ExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options);
}
```

The read/count/explain methods mirror the single-file engine; the catalog adds the name, capabilities, the optional
snapshot, and (later) partition awareness. Every `read(...)` returns a closeable `Stream` - use try-with-resources, or
the in-flight row-group buffers leak.

## Capabilities: ask, do not probe

A caller reads a capability record once and branches on it, rather than discovering behavior by trial and error. Boolean
defaults are safe-false, and `require*` guards turn an unsupported request into a precise exception at the boundary.

`CatalogCapabilities(enumeratesDatasets, timeTravel, schemaSource)`:
- `enumeratesDatasets` - whether `datasets()` lists more than one (a directory of files) or exactly one (a single file
  or one table).
- `timeTravel` - whether a past snapshot can be pinned.
- `schemaSource` - `MERGED_FILES` (read from footers), `TABLE_METADATA` (an Iceberg table), or `COLLECTION`.

`DatasetCapabilities(mergeOnRead, fieldIdResolved, fileStats, fileSpatialBounds, partitionModel, cheapCount, cheapBounds)`:
- `fileStats` - where per-file statistics for pruning come from: `NONE`, `MANIFEST` (Iceberg), or `FOOTER_AGGREGATE`.
- `fileSpatialBounds` - `NONE`, `NATIVE_GEO`, or `COVERING_COLUMN`.
- `partitionModel` - `NONE`, `HIVE_PATH`, or `PARTITION_SPEC`.
- `mergeOnRead` / `fieldIdResolved` / `cheapCount` / `cheapBounds` - whether deletes are applied, columns are matched by
  field id, and count/bounds are answerable from metadata alone.

## Implementations

- **`FileSourceCatalog`** (pure-parquet, in this module) - reads the files of a `FileSource` as one or more datasets.
  A single file or single-unit listing yields one dataset; a directory of heterogeneous files or hive-partitioned trees
  yields many. It opens every file eagerly and owns the byte sources; `close()` releases them and the `FileSource`.
- **`ParquetDatasetCatalog`** (legacy) - resolves a base location to a single named dataset. Still works; new code
  should prefer `FileSourceCatalog`, which implements the `DatasetCatalog` SPI and resolves one or many datasets.
- **External backends** implement `DatasetCatalog` directly. The Iceberg backend (`parquetry-iceberg`) resolves a table
  from its metadata, follows the pinned snapshot through the manifests, and prunes data files by their manifest bounds.

## Discovery: how a listing becomes datasets

`HivePartitionResolver` shapes a flat file listing into named `DatasetUnit`s. The listing (a glob or directory) is only
the discovery mechanism; how those files group into datasets is configured through `CatalogOptions.maxHiveDepth()`, never
inferred from the data:

- files with no `key=value` path segments become one dataset each (layer-per-file - a directory of `pois.parquet`,
  `buildings.parquet`, ...);
- hive-partitioned trees (`theme=buildings/type=building/...`) group by their partition keys up to `maxHiveDepth` levels;
- `maxHiveDepth` absent means all levels discriminate; `0` folds every file into one dataset; a negative value is
  rejected.

Files within one dataset must agree on schema by equality; merge across siblings is never implicit.

## Reading a dataset

```java
try (FileSource source = LocalFileSource.directory(dir, "*.parquet");
        FileSourceCatalog catalog = FileSourceCatalog.open(source, CatalogOptions.defaults())) {

    for (String name : catalog.datasets()) {
        Dataset dataset = catalog.dataset(name);
        Predicate where = new Predicate.Gt(ColumnPath.of("population"), new Value.LongVal(10_000));
        try (Stream<ParquetRecord> rows = dataset.read(where, Projection.ALL, ReadOptions.DEFAULTS)) {
            rows.forEach(this::handle);
        }
    }
}
```

## Relationship to the core engine

`ParquetDataset` sits directly above the single-file `io.tileverse.parquetry.data.ParquetReader`. A one-file dataset
opens through `ParquetDataset.open(ByteRangeSource)`; a multi-file dataset opens through
`ParquetDataset.open(FilesetReader)`, where `FilesetReader` is the seam an implementation satisfies to supply per-file
byte sources by index (the Iceberg backend builds one over a snapshot's surviving data files). Runtime wiring (the
shared `ParquetRuntime`, an optional decryption key) is bound once through `OpenOptions`; per-query policy stays in
`ReadOptions`.

## File-level pruning

`FilePlan` and `PlannedFile` expose the ordered, pruned set of files a read will visit, and `FileStatistics` holds
the per-column bounds a backend records. A backend feeds those bounds into the engine's public pruning facade
(`io.tileverse.parquetry.filter.prune.FilePruner` in `parquetry-core`) to skip files that cannot contribute a matching
row. Pruning never changes results - a kept file is still filtered at row-group and record level.

## License

Apache-2.0.
