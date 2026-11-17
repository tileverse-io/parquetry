# Build from source

You want to build Parquetry from its sources and run its tests, to try a change or to use a version not released yet. This page clones the repository, builds the modules, runs the tests and builds this documentation site.

## Requirements

- JDK 25. The build compiles with Java 25 preview features.
- Git.
- Docker, for part of the integration tests. These tests start object storage emulators in containers, and are skipped when Docker is not available.

Build with `./mvnw`, the Maven wrapper at the root of the repository. It downloads the Maven version used by the project, and reads the JVM options of the build from `.mvn/jvm.config`.

## Clone with the test files

The tests read files from other repositories: the Apache Parquet test corpus (`apache/parquet-testing`) and the GeoParquet examples and test data (`opengeospatial/geoparquet`, `geoparquet/geoparquet-testing`). These repositories are Git submodules under `internal/parquetry-testkit`. Clone with them, or the conformance tests cannot run:

```bash
git clone --recurse-submodules https://github.com/tileverse-io/parquetry.git
```

In a clone made without them, fetch them:

```bash
git submodule update --init --recursive
```

## Build and test

```bash
# Compile and run the unit tests
./mvnw test

# Unit and integration tests
./mvnw verify

# Package the modules without running the tests
./mvnw package -DskipTests
```

Unit tests are the classes named `*Test`, and run in the `test` phase. Integration tests are the classes named `*IT`, and run in the `verify` phase. They read the test corpora, compare the results with other Parquet readers, and read files through the storage emulators.

## Build one module

Select a module by its artifact id, and add `-am` to also build the modules it depends on:

```bash
./mvnw -pl :parquetry-core -am test
```

Without `-am`, Maven takes the other modules from your local repository, where they may be older than your sources, or missing. [Modules](../reference/modules.md) lists the artifact ids.

Pass JVM options to the test JVMs through `extraArgLine`, for example a larger heap:

```bash
./mvnw -pl :parquetry-core -am verify -DextraArgLine="-Xmx2g"
```

## Format the code

The build formats the Java sources and the POM files with Spotless. To format without building:

```bash
./mvnw spotless:apply
```

[Contribute](contribute.md#formatting) describes the formatting rules.

## Measure test coverage

```bash
./mvnw -Pcoverage verify
```

The build writes a coverage report of all modules to `internal/parquetry-coverage-report/target/site/jacoco-aggregate/index.html`.

## Build this site

The sources of this site are under `docs/`. The build needs Python 3.11 or newer, and Docker for the architecture diagrams:

```bash
# Create the Python environment, generate the diagrams, build the site into docs/site/
./docs/build.sh

# Serve the site at http://127.0.0.1:8000, reloading on changes
./docs/serve.sh
```

The build is strict: a link to a missing page or anchor fails it. The [README of the docs directory](https://github.com/tileverse-io/parquetry/blob/main/docs/README.md) describes the layout of the sources and how to add a page.
