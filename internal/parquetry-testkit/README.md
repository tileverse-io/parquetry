# parquetry-testkit

Shared test fixtures for parquetry and its integrations: the Parquet and GeoParquet test corpora,
plus a small utility that extracts them from the classpath onto disk. Build and dev only - never published.

## What it does

- Bundles two upstream test corpora into a jar, as classpath resources:
  - `apache/parquet-testing` - the whole corpus (`data/`, `bad_data/`, `variant/`, `shredded_variant/`).
  - `opengeospatial/geoparquet` - only `test_data/` (the per-geometry `.parquet` files and their `.csv` WKT sidecars).
- Provides `TestCorpus`, which copies a bundled resource directory or a single file into a target directory on disk.
  A consuming test reads real files from a JUnit `@TempDir` regardless of where its module lives in the build tree.

Both corpora arrive as **git submodules** under `src/main/resources/` (see the repo root `.gitmodules`). The Maven
build packs `parquet-testing/**` and `geoparquet/test_data/**` into the jar and leaves the rest (submodule `.git` pointers, docs) out.

## Getting the corpora

The submodules are not populated by a plain `git clone`. Either clone recursively:

```bash
git clone --recurse-submodules <repo-url>
```

or, in an existing checkout:

```bash
git submodule update --init --recursive
```

If the corpora are missing from the classpath, `TestCorpus` fails with a clear `IllegalArgumentException` naming the resource it could not find.

## Usage

Depend on the testkit at **test** scope:

```xml
<dependency>
  <groupId>io.tileverse.parquetry</groupId>
  <artifactId>parquetry-testkit</artifactId>
  <version>${project.version}</version>
  <scope>test</scope>
</dependency>
```

Extract into a `@TempDir` and read from there:

```java
import io.tileverse.parquetry.testkit.TestCorpus;

@TempDir
Path tempDir;

@Test
void readsAFixture() {
    // A whole subtree, structure preserved:
    Path data = TestCorpus.extractDirectory("parquet-testing/data", tempDir);
    Path file = data.resolve("alltypes_plain.parquet");

    // Or a single file:
    Path one = TestCorpus.extractFile("geoparquet/test_data/data-point-encoding_wkb.parquet", tempDir);
}
```

Resource paths are slash-separated and rooted at the jar's classpath (e.g. `parquet-testing/data`, `geoparquet/test_data`). Extraction works whether the resources are an exploded directory (reactor build) or packed inside the jar.

Never reach for the fixtures with `Paths.get("src/test/resources/...")` - that breaks the moment a module moves, which is the whole reason this module exists.

## Where it fits

```
   parquetry-testkit jar  (parquet-testing/**, geoparquet/test_data/**)
                 |
                 |  test-scope dependency
                 v
   consuming module's test  -- TestCorpus.extractDirectory(...) --> @TempDir
                                                                       |
                                                                       v
                                                              real files on disk
```

`parquetry-core` does not call `TestCorpus` directly; it wraps it in `CorpusFixtures` (package `io.tileverse.parquetry.testsupport`),
which extracts the `parquet-testing/data` corpus once per JVM into a shared temp directory and cleans it up at shutdown.

## Notes

- **Dependency-free on purpose.** `TestCorpus` depends on nothing in parquetry. A `core -> testkit -> core` edge would be a reactor cycle,
  since core consumes the testkit at test scope.
- **Never published.** The module sets `maven.deploy.skip` and is listed in the root `central-publishing-maven-plugin` `<excludeArtifacts>`.
  It inherits from the `parquetry-internal` aggregator parent.
- Each bundled corpus keeps its upstream `LICENSE` file alongside its data.

