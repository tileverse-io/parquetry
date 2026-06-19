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
  what the connection can do; `FilesetCatalog` is the pure-parquet implementation.
- **`io.tileverse.parquetry.dataset`** - the queryable view. `Dataset` is the per-dataset facade;
  `GeoParquetDataset extends Dataset` adds `geoMetadata()` for GeoParquet-backed datasets; `DatasetCapabilities`
  describes one dataset; `ParquetDataset` reads 1..N same-schema files as one stream above the core `ParquetReader`.

## The SPI

```
DatasetCatalog  ──datasets()──▶  names
       │
       └──dataset(name)──▶  Dataset ──read/count/bounds/explain──▶  records
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
    Optional<BoundingBox> bounds(Predicate predicate, ReadOptions options);  // default: empty
    ExplainPlan explain(Predicate predicate, Projection projection, ReadOptions options);
}

public interface GeoParquetDataset extends Dataset {
    Optional<GeoParquetMetadata> geoMetadata();    // aggregated "geo" metadata across the files
}
```

The read/count/explain methods mirror the single-file engine; the catalog adds the name, capabilities, the optional
snapshot, and (later) partition awareness. `bounds(predicate, options)` returns the aggregated spatial extent for the
unfiltered case (the predicate reduces to always-true) and empty for a filtered query for now, leaving the caller to
compute it; it also returns empty for a dataset without a spatial extent. A GeoParquet-backed dataset implements
`GeoParquetDataset`, exposing the aggregated `"geo"` metadata via `geoMetadata()`; backends whose geometry is not
GeoParquet (Iceberg native geometry) implement plain `Dataset`. Every `read(...)` returns a closeable `Stream` - use
try-with-resources, or the in-flight row-group buffers leak.

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

- **`FilesetCatalog`** (pure-parquet, in this module) - resolves exactly one merged dataset from a `FileSource`,
  whether that source is a single file, a glob or directory of same-schema files, or a Hive-partitioned tree. All
  matched files must agree on schema by equality. It opens every file eagerly and owns the byte sources; `close()`
  releases them and the `FileSource`.
- **External backends** implement `DatasetCatalog` directly. The Iceberg backend (`parquetry-iceberg`) resolves a table
  from its metadata, follows the pinned snapshot through the manifests, and prunes data files by their manifest bounds.

## Discovery: how a listing becomes a dataset

The listing (a glob or directory) is the discovery mechanism: all listed files form one merged dataset and must agree
on schema by equality. Hive `key=value/...` path segments are a physical-column **pruning** aid, never a dataset
discriminator: the whole tree is one dataset. `HivePartitionResolver.partitionValues(relativePath)` parses a file's
`key=value` segments (`theme=buildings/type=building/...`) into an ordered map, and each partition value becomes an
exact `min == max` file statistic on its column, fed to the engine's `FilePruner` to skip files that cannot match. A
partition key whose column is absent from the files (a path-only key) is rejected at open with an
`IllegalStateException`. A file with no `key=value` segments has no partitions.

## Reading a dataset

```java
try (FileSource source = LocalFileSource.directory(dir, "*.parquet");
        FilesetCatalog catalog = FilesetCatalog.open(source, CatalogOptions.defaults())) {

    String name = catalog.datasets().get(0);
    Dataset dataset = catalog.dataset(name);
    Predicate where = new Predicate.Gt(ColumnPath.of("population"), new Value.LongVal(10_000));
    try (Stream<ParquetRecord> rows = dataset.read(where, Projection.ALL, ReadOptions.DEFAULTS)) {
        rows.forEach(this::handle);
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
